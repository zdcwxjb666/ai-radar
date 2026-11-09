#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Radar — 第 1 周（v0.2）

v0.2 改动：
    1. 打分拆成「相关度 + 可行动性」两维，加权分由代码算，不让模型做算术
    2. 原始正文进库 —— 改判断标准时可以直接重打分，不用重抓（--reanalyze）
    3. 日报分三档：必看(>=7) / 值得(6) / 速览，避免"全低分"时一片空白
    4. 每个源计时、token 与可选成本统计

用法：
    python radar.py --check        # 测 API
    python radar.py --fetch-only   # 只抓取，不花钱
    python radar.py                # 跑一遍
    python radar.py --reanalyze    # 用新 prompt 重打分（不重新抓取）
    python radar.py --stats        # 累计数据
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import smtplib
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "radar.db"
LOG_PATH = DATA_DIR / "radar.log"

UA = "Mozilla/5.0 (compatible; AIRadar/0.2)"
ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}

# 权重：相关度比可行动性重要。改这里就能调口味，不用动 prompt。
W_RELEVANCE = 0.6
W_ACTION = 0.4


# --------------------------------------------------------------------------
# 配置
# --------------------------------------------------------------------------
def load_env() -> None:
    p = ROOT / ".env"
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def cfg(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def log(msg: str) -> None:
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


# --------------------------------------------------------------------------
# 数据库
# --------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    hash          TEXT UNIQUE NOT NULL,
    source        TEXT,
    url           TEXT,
    title         TEXT,
    content       TEXT,
    published     TEXT,
    fetched_at    TEXT,
    status        TEXT DEFAULT 'new',
    relevance     INTEGER,
    actionability INTEGER,
    score         INTEGER,
    summary       TEXT,
    reason        TEXT,
    action        TEXT,
    tags          TEXT
);
CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at  TEXT,
    finished_at TEXT,
    fetched     INTEGER DEFAULT 0,
    new_items   INTEGER DEFAULT 0,
    analyzed    INTEGER DEFAULT 0,
    failed      INTEGER DEFAULT 0,
    tokens      INTEGER DEFAULT 0,
    seconds     REAL    DEFAULT 0,
    ok          INTEGER DEFAULT 1,
    note        TEXT
);
CREATE TABLE IF NOT EXISTS feedback (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id    INTEGER NOT NULL,
    verdict    TEXT NOT NULL,          -- like / skip
    created_at TEXT,
    UNIQUE(item_id)
);
"""

# 表结构演进：新版本加的列，老库自动补上，不用删库
# 表结构演进：新版本加的列，老库自动补上，不用删库。
# 注意：两张表都要管。v0.2 只给 items 加了列，忘了 runs，结果跑到最后一步才崩。
TABLE_COLUMNS = {
    "items": [("content", "TEXT"), ("relevance", "INTEGER"), ("actionability", "INTEGER"),
              ("score", "INTEGER"), ("summary", "TEXT"), ("reason", "TEXT"),
              ("action", "TEXT"), ("tags", "TEXT")],
    "runs": [("seconds", "REAL")],
}


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    for table, cols in TABLE_COLUMNS.items():
        existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        for name, typ in cols:
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
    conn.commit()
    return conn


def make_hash(url: str, title: str) -> str:
    key = (url or title or "").strip().lower()
    key = re.sub(r"[?#].*$", "", key)
    key = re.sub(r"^https?://(www\.)?", "", key)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def combine(relevance: int, actionability: int) -> int:
    """模型负责判断，算术交给代码——模型算加权平均经常算错。"""
    return int(round(W_RELEVANCE * relevance + W_ACTION * actionability))


# --------------------------------------------------------------------------
# 抓取
# --------------------------------------------------------------------------
def http_get(url: str, timeout: int = 20, retries: int = 2) -> bytes:
    last: Exception | None = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            last = e
            if i < retries:
                time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"抓取失败 {url}: {last}")


def strip_html(text: str) -> str:
    text = re.sub(r"<script.*?</script>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;?", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_feed(xml_bytes: bytes) -> list[dict]:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        raise RuntimeError(f"XML 解析失败: {e}") from e

    out: list[dict] = []
    if root.tag.endswith("feed"):  # Atom
        for e in root.findall("a:entry", ATOM_NS):
            link_el = e.find("a:link", ATOM_NS)
            link = link_el.get("href", "") if link_el is not None else ""
            out.append({
                "title": (e.findtext("a:title", "", ATOM_NS) or "").strip(),
                "url": link,
                "published": (e.findtext("a:updated", "", ATOM_NS) or "").strip(),
                "content": strip_html(e.findtext("a:content", "", ATOM_NS)
                                      or e.findtext("a:summary", "", ATOM_NS) or ""),
            })
    else:  # RSS 2.0
        for e in root.findall("./channel/item"):
            out.append({
                "title": (e.findtext("title") or "").strip(),
                "url": (e.findtext("link") or "").strip(),
                "published": (e.findtext("pubDate") or "").strip(),
                "content": strip_html(e.findtext("description") or ""),
            })
    return [x for x in out if x["title"] or x["url"]]


# --------------------------------------------------------------------------
# LLM
# --------------------------------------------------------------------------
def chat(messages: list[dict], json_mode: bool = True, max_tokens: int = 600) -> tuple[str, int]:
    base = cfg("LLM_BASE_URL", "https://api.deepseek.com/v1").rstrip("/")
    key = cfg("LLM_API_KEY")
    model = cfg("LLM_MODEL", "deepseek-chat")
    if not key:
        raise RuntimeError("没有配置 LLM_API_KEY，请把 .env.example 复制为 .env 并填入")
    if not key.isascii() or key.startswith("sk-你的"):
        raise RuntimeError("LLM_API_KEY 里还是占位符，请填真实 key")

    body: dict = {"model": model, "messages": messages, "temperature": 0.2,
                  "max_tokens": max_tokens}
    if json_mode:
        body["response_format"] = {"type": "json_object"}

    req = urllib.request.Request(
        f"{base}/chat/completions",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            data = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"API 返回 {e.code}: {e.read().decode('utf-8', 'replace')[:300]}") from e

    return data["choices"][0]["message"]["content"], int(data.get("usage", {}).get("total_tokens", 0))


def extract_json(text: str) -> dict:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t, flags=re.S)
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        i, j = t.find("{"), t.rfind("}")
        if i >= 0 and j > i:
            return json.loads(t[i:j + 1])
        raise


PROMPT_TMPL = ROOT / "prompts" / "analyze.md"


def _clip(v, lo: int = 0, hi: int = 10) -> int:
    try:
        return max(lo, min(hi, int(round(float(v)))))
    except (TypeError, ValueError):
        return 0


def analyze(item: dict, hint: str = "") -> tuple[dict, int]:
    profile = cfg("PROFILE", "在校大学生，目标进入 AI 应用工程岗")
    tmpl = PROMPT_TMPL.read_text(encoding="utf-8") if PROMPT_TMPL.exists() else (
        '只输出 JSON：{"relevance":0-10,"actionability":0-10,"summary":"","reason":"","tags":[],"action":"read|skim|skip"}'
    )
    prompt = (tmpl.replace("{profile}", profile)
                  .replace("{feedback}", hint)
                  .replace("{title}", item.get("title", ""))
                  .replace("{source}", item.get("source", ""))
                  .replace("{content}", strip_html(item.get("content", ""))[:1200]))

    raw, tokens = chat([
        {"role": "system", "content": "你是严格的信息筛选助手，只输出合法 JSON，不输出任何解释。"},
        {"role": "user", "content": prompt},
    ])
    d = extract_json(raw)

    relevance = _clip(d.get("relevance"))
    action = _clip(d.get("actionability"))
    tags = d.get("tags") or []
    return {
        "relevance": relevance,
        "actionability": action,
        "score": combine(relevance, action),
        "summary": str(d.get("summary", ""))[:120],
        "reason": str(d.get("reason", ""))[:80],
        "action": str(d.get("action", "skim")),
        "tags": ",".join(str(t) for t in tags[:4]) if isinstance(tags, list) else str(tags)[:60],
    }, tokens


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------
def feeds() -> list[str]:
    return [u.strip() for u in re.split(r"[,\n]", cfg("FEEDS")) if u.strip().startswith("http")]


def save_analysis(conn: sqlite3.Connection, h: str, d: dict) -> None:
    conn.execute(
        "UPDATE items SET status='analyzed',relevance=?,actionability=?,score=?,"
        "summary=?,reason=?,action=?,tags=? WHERE hash=?",
        (d["relevance"], d["actionability"], d["score"], d["summary"],
         d["reason"], d["action"], d["tags"], h),
    )


def run(dry_run: bool = False, fetch_only: bool = False, limit: int = 20) -> int:
    t0 = time.time()
    conn = db() if not (dry_run or fetch_only) else None
    started = datetime.now().isoformat(timespec="seconds")
    fetched = new_items = analyzed = failed = tokens = 0
    results: list[dict] = []
    hint = feedback_hint(conn) if conn else ""
    if hint:
        log("已载入历史反馈，本次打分会对齐你的口味")

    src_list = feeds()
    if not src_list:
        log("没有配置 FEEDS，请检查 .env")
        return 2

    for src in src_list:
        t_src = time.time()
        try:
            raw = http_get(src)
            items = parse_feed(raw)[:limit]
            log(f"{src} -> {len(items)} 条 ({time.time() - t_src:.1f}s)")
        except Exception as e:  # noqa: BLE001
            log(f"[源失败] {e}")
            failed += 1
            continue

        for it in items:
            fetched += 1
            it["source"] = src
            h = make_hash(it["url"], it["title"])

            if conn:
                if conn.execute("SELECT 1 FROM items WHERE hash=?", (h,)).fetchone():
                    continue
                conn.execute(
                    "INSERT INTO items(hash,source,url,title,content,published,fetched_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (h, src, it["url"], it["title"], it["content"][:8000],
                     it.get("published", ""), datetime.now().isoformat(timespec="seconds")),
                )
                conn.commit()
            new_items += 1

            if fetch_only:
                results.append({**it, "score": 0})
                continue
            try:
                d, tk = analyze(it, hint)
                tokens += tk
                analyzed += 1
                results.append({**it, **d})
                if conn:
                    save_analysis(conn, h, d)
                    conn.commit()
            except Exception as e:  # noqa: BLE001
                failed += 1
                log(f"[分析失败] {(it['title'] or '')[:40]} :: {e}")
                if conn:
                    conn.execute("UPDATE items SET status='failed' WHERE hash=?", (h,))

    secs = time.time() - t0
    # 日报从「今天库里所有已分析条目」重建，而不是只用本次新增的。
    # 好处：一天跑两次不会互相覆盖；改了版式可以 --render 重出，不花一个 token。
    items = items_today(conn) if (conn and not fetch_only) else results
    report = render(items, fetch_only, tokens, len(results))
    print("\n" + report)

    if not dry_run and not items:
        log("本次没有新内容，不覆盖已有日报")
    elif not dry_run:
        (DATA_DIR / f"digest-{datetime.now():%Y-%m-%d}.md").write_text(report, encoding="utf-8")
        log(f"日报已写入 data/digest-{datetime.now():%Y-%m-%d}.md")
        if not fetch_only:
            notify(report)

    if conn:
        ok = 0 if (failed and failed >= max(1, fetched // 2)) else 1
        try:
            conn.execute(
                "INSERT INTO runs(started_at,finished_at,fetched,new_items,analyzed,"
                "failed,tokens,seconds,ok) VALUES(?,?,?,?,?,?,?,?,?)",
                (started, datetime.now().isoformat(timespec="seconds"),
                 fetched, new_items, analyzed, failed, tokens, round(secs, 1), ok),
            )
            conn.commit()
        except sqlite3.Error as e:
            # 记账失败不该毁掉一次已经成功的运行
            log(f"[记账失败，不影响本次结果] {e}")

    cost = ""
    if price := cfg("PRICE_PER_MTOK"):
        try:
            cost = f" / ≈¥{tokens / 1_000_000 * float(price):.4f}"
        except ValueError:
            pass
    log(f"完成：抓取 {fetched} / 新增 {new_items} / 分析 {analyzed} / 失败 {failed} / "
        f"tokens {tokens:,}{cost} / 耗时 {secs:.1f}s")
    return 0 if not (failed and failed >= max(1, fetched // 2)) else 1


MUST_N, WORTH_N, SKIM_N = 5, 10, 10  # 名额制：每天最多展开这么多条


def sort_key(r: dict):
    """排序必须全项目唯一，否则日报里的编号和 --like 解析到的条目会错位。"""
    return (-(r.get("score") or 0), -(r.get("actionability") or 0), -(r.get("id") or 0))


def items_today(conn: sqlite3.Connection) -> list[dict]:
    today = datetime.now().strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT * FROM items WHERE substr(fetched_at,1,10)=? AND status='analyzed'",
        (today,)).fetchall()
    return [dict(r) for r in rows]


def ranked_today(conn: sqlite3.Connection) -> list[dict]:
    rows = sorted(items_today(conn), key=sort_key)
    for i, r in enumerate(rows, 1):
        r["_n"] = i
    return rows


# --------------------------------------------------------------------------
# 反馈：这是整个系统的"学习"部分
# --------------------------------------------------------------------------
def add_feedback(refs: list[str], verdict: str) -> int:
    conn = db()
    rows = ranked_today(conn)
    if not rows:
        log("今天还没有可标记的内容")
        return 1
    hit = 0
    for ref in refs:
        row = None
        if ref.isdigit():
            i = int(ref) - 1
            row = rows[i] if 0 <= i < len(rows) else None
        else:
            row = next((r for r in rows if r["hash"].startswith(ref.lower())), None)
        if not row:
            log(f"找不到编号 {ref}（今天共 {len(rows)} 条）")
            continue
        conn.execute("DELETE FROM feedback WHERE item_id=?", (row["id"],))
        conn.execute("INSERT INTO feedback(item_id,verdict,created_at) VALUES(?,?,?)",
                     (row["id"], verdict, datetime.now().isoformat(timespec="seconds")))
        hit += 1
        log(f"[{ref}] {verdict} · {row['title'][:50]}")
    conn.commit()
    if hit:
        log(f"已记录 {hit} 条反馈，共 {conn.execute('SELECT COUNT(*) c FROM feedback').fetchone()['c']} 条")
    return 0 if hit else 1


def _tag_counter(conn: sqlite3.Connection, verdict: str) -> list[tuple[str, int]]:
    rows = conn.execute(
        "SELECT i.tags FROM feedback f JOIN items i ON i.id=f.item_id "
        "WHERE f.verdict=? AND i.tags IS NOT NULL AND i.tags != ''", (verdict,)).fetchall()
    cnt: dict[str, int] = {}
    for r in rows:
        for t in str(r["tags"]).split(","):
            t = t.strip()
            if t:
                cnt[t] = cnt.get(t, 0) + 1
    return sorted(cnt.items(), key=lambda x: -x[1])


def feedback_hint(conn: sqlite3.Connection, n: int = 6) -> str:
    """把最近的喜欢/跳过变成 few-shot，注入打分 prompt。少于 6 条时不注入，避免噪声。"""
    total = conn.execute("SELECT COUNT(*) c FROM feedback").fetchone()["c"]
    if total < 6:
        return ""
    likes = conn.execute(
        "SELECT i.title FROM feedback f JOIN items i ON i.id=f.item_id "
        "WHERE f.verdict='like' ORDER BY f.id DESC LIMIT ?", (n,)).fetchall()
    skips = conn.execute(
        "SELECT i.title FROM feedback f JOIN items i ON i.id=f.item_id "
        "WHERE f.verdict='skip' ORDER BY f.id DESC LIMIT ?", (n,)).fetchall()
    parts = ["## 读者真实反馈（比他的自述更可信，请优先对齐）"]
    if likes:
        parts.append("他亲手标记为「想看」的：\n"
                     + "\n".join(f"- {r['title'][:60]}" for r in likes))
    if skips:
        parts.append("他亲手标记为「跳过」的：\n"
                     + "\n".join(f"- {r['title'][:60]}" for r in skips))
    parts.append("请从这些例子中归纳他真正的偏好（内容类型、深度、形式），"
                 "对同类内容加权或降权，而不是只认标题里的关键词。")
    return "\n\n".join(parts)


def show_feedback() -> int:
    conn = db()
    c = lambda v: conn.execute("SELECT COUNT(*) n FROM feedback WHERE verdict=?", (v,)).fetchone()["n"]
    likes, skips = c("like"), c("skip")
    if not likes and not skips:
        print("还没有反馈。看日报时记下编号，然后：\n"
              "  python radar.py --like 3 7\n  python radar.py --skip 1 2")
        return 0
    good, bad = _tag_counter(conn, "like"), _tag_counter(conn, "skip")
    # 在「喜欢」和「跳过」里都高频出现的标签没有区分度，剔除掉再展示
    common = {t for t, _ in good[:8]} & {t for t, _ in bad[:8]}
    good = [(t, n) for t, n in good if t not in common]
    bad = [(t, n) for t, n in bad if t not in common]
    print(f"\n喜欢 {likes} 条 / 跳过 {skips} 条\n")
    if good:
        print("  喜欢的标签：", "  ".join(f"{t}×{n}" for t, n in good[:8]))
    if bad:
        print("  跳过的标签：", "  ".join(f"{t}×{n}" for t, n in bad[:8]))
    if likes:
        print("\n  最近喜欢：")
        for r in conn.execute(
                "SELECT i.title FROM feedback f JOIN items i ON i.id=f.item_id "
                "WHERE f.verdict='like' ORDER BY f.id DESC LIMIT 5").fetchall():
            print(f"    · {r['title'][:60]}")
    sug = []
    if good:
        sug.append("多看：" + "、".join(t for t, _ in good[:5]))
    if bad:
        sug.append("少看：" + "、".join(t for t, _ in bad[:5]))
    if sug:
        print("\n  可以把这句追加到 .env 的 PROFILE 末尾：\n    " + "；".join(sug))
    if likes + skips >= 6:
        print(f"\n  已有 {likes + skips} 条反馈，下次打分会自动带上 few-shot 示例。")
    else:
        print(f"\n  再积累 {6 - likes - skips} 条，就会自动注入 prompt。")
    return 0


def render(items: list[dict], fetch_only: bool, tokens: int = 0, seen: int = 0) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    if fetch_only:
        lines = [f"# Radar 抓取预览 · {today}", "", f"共 {len(items)} 条（未调用 LLM）", ""]
        lines += [f"- {r['title']}\n  {r['url']}" for r in items]
        return "\n".join(lines)

    ranked = sorted(items, key=sort_key)
    for i, r in enumerate(ranked, 1):
        r["_n"] = i
    hi = [r for r in ranked if (r.get("score") or 0) >= 7]

    # 名额制：先排序，再按名额截断。
    # 阈值法（>=7 全展开）在源变多时会漂移 —— 25 条"必看"等于没有必看。
    # 日报要的是"今天最值得看的 5 条"，不是"今天所有好内容"。
    must, worth, rest = [], [], []
    for r in ranked:
        s = r.get("score") or 0
        if s >= 7 and len(must) < MUST_N:
            must.append(r)
        elif 6 <= s < 7 and len(worth) < WORTH_N:
            worth.append(r)
        elif len(rest) < SKIM_N:
            rest.append(r)

    head = f"今天共 {len(items)} 条"
    if seen and seen != len(items):
        head = f"本次新增 {seen} 条，今日累计 {len(items)} 条"
    lines = [f"# Radar · {today}", "",
             f"{head}，其中 **{len(hi)} 条 ≥7 分**，展开得分最高的 {len(must)} 条。", ""]

    def block(title: str, rows: list[dict]) -> None:
        if not rows:
            return
        lines.append(f"## {title}\n")
        for r in rows:
            lines.extend([
                f"### [{r.get('_n', '?')}] {r['score']}/10 · {r['title']}",
                "",
                f"> {r.get('summary', '')}",
                "",
                f"- 为什么：{r.get('reason', '')}",
                f"- 维度：相关 {r.get('relevance')} · 可用 {r.get('actionability')}",
                f"- 标签：{r.get('tags', '')}",
                f"- 链接：{r['url']}",
                "",
            ])
        lines.append("")

    block("必看", must)
    block("值得看", worth)

    if not must and not worth:
        lines.append("> 今天没有值得展开的内容。先怀疑信息源，再怀疑判断标准。")
        lines.append("")

    if rest:
        lines.append("## 速览\n")
        lines.extend([f"- [{r.get('_n', '?')}] {r.get('score', '?')}/10 {r['title']}" for r in rest])
        lines.append("")

    lines.append("---")
    lines.append(f"本日 {len(items)} 条" + (f" / 本次 {tokens:,} tokens" if tokens else ""))
    lines.append("标记想看：`python radar.py --like 1 3`　跳过：`--skip 2`")
    return "\n".join(lines)


def rebuild_digest() -> int:
    """按当前版式重出今天的日报：不抓取、不调用 LLM、不花钱。"""
    conn = db()
    items = items_today(conn)
    if not items:
        log("今天还没有已分析的内容")
        return 1
    report = render(items, fetch_only=False)
    path = DATA_DIR / f"digest-{datetime.now():%Y-%m-%d}.md"
    path.write_text(report, encoding="utf-8")
    print(report)
    log(f"已重建 {path}")
    return 0


def send_webhook(report: str) -> None:
    """支持企业微信 / 飞书 / 钉钉 / 自定义 JSON 四种机器人。"""
    url = cfg("WEBHOOK_URL")
    kind = cfg("WEBHOOK_TYPE", "wecom").lower()
    text = report
    if kind in ("wecom", "dingtalk") and len(text) > 3800:
        text = text[:3800] + "\n\n（内容过长，已截断）"

    if kind == "wecom":
        body = {"msgtype": "markdown", "markdown": {"content": text}}
    elif kind == "feishu":
        body = {"msg_type": "text", "content": {"text": report[:18000]}}
    elif kind == "dingtalk":
        body = {"msgtype": "markdown",
                "markdown": {"title": f"Radar {datetime.now():%m-%d}", "text": text}}
    else:
        body = {"text": report[:18000]}

    req = urllib.request.Request(
        url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=20) as r:
        resp = r.read().decode("utf-8", "replace")[:200]
    log(f"已推送（{kind}）：{resp}")


def send_email(report: str) -> None:
    host, user, pw = cfg("SMTP_HOST"), cfg("SMTP_USER"), cfg("SMTP_PASS")
    to = cfg("MAIL_TO", user)
    msg = EmailMessage()
    msg["Subject"] = f"Radar {datetime.now():%m-%d}"
    msg["From"], msg["To"] = user, to
    msg.set_content(report)
    with smtplib.SMTP_SSL(host, 465, context=ssl.create_default_context()) as s:
        s.login(user, pw)
        s.send_message(msg)
    log(f"邮件已发送 -> {to}")


def notify(report: str) -> None:
    """渠道可以有多个，任何一个失败都不能影响其它渠道，更不能影响主流程。"""
    sent = False
    if cfg("WEBHOOK_URL"):
        try:
            send_webhook(report)
            sent = True
        except Exception as e:  # noqa: BLE001
            log(f"[推送失败] {e}")
    if cfg("SMTP_HOST") and cfg("SMTP_USER") and cfg("SMTP_PASS"):
        try:
            send_email(report)
            sent = True
        except Exception as e:  # noqa: BLE001
            log(f"[邮件失败] {e}")
    if not sent:
        log("未配置任何推送渠道，只写本地文件")


# --------------------------------------------------------------------------
# 重打分（改了 prompt 或权重后用，不重新抓取）
# --------------------------------------------------------------------------
def reanalyze(limit: int = 200) -> int:
    conn = db()
    rows = conn.execute(
        "SELECT * FROM items WHERE content IS NOT NULL AND content != '' "
        "ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    if not rows:
        log("库里没有带正文的条目，先正常跑一次")
        return 1
    log(f"重打分 {len(rows)} 条…")
    hint = feedback_hint(conn)
    tokens = failed = 0
    for row in rows:
        try:
            d, tk = analyze(dict(row), hint)
            tokens += tk
            save_analysis(conn, row["hash"], d)
            conn.commit()
        except Exception as e:  # noqa: BLE001
            failed += 1
            log(f"[失败] {row['title'][:40]} :: {e}")
    log(f"重打分完成：tokens {tokens:,}，失败 {failed}")
    return 0


# --------------------------------------------------------------------------
# 辅助
# --------------------------------------------------------------------------
def check_api() -> int:
    log("测试 API 连通性…")
    try:
        content, tokens = chat([{"role": "user", "content": "只回复两个字：收到"}],
                               json_mode=False, max_tokens=20)
        log(f"成功 ✅ 回复 {content.strip()!r}，tokens={tokens}")
        return 0
    except Exception as e:  # noqa: BLE001
        log(f"失败 ❌ {e}")
        return 1


def print_stats() -> int:
    conn = db()
    rows = conn.execute("SELECT * FROM runs ORDER BY id").fetchall()
    if not rows:
        print("还没有运行记录。")
        return 0
    c = conn.execute
    total = c("SELECT COUNT(*) n FROM items").fetchone()["n"]
    analyzed = c("SELECT COUNT(*) n FROM items WHERE status='analyzed'").fetchone()["n"]
    dist = c("SELECT score, COUNT(*) n FROM items WHERE score IS NOT NULL "
             "GROUP BY score ORDER BY score DESC").fetchall()
    print(f"""
运行天数      : {len({r['started_at'][:10] for r in rows})}
运行次数      : {len(rows)}
累计处理      : {total} 条（已分析 {analyzed}）
累计 tokens   : {sum(r['tokens'] or 0 for r in rows):,}
平均耗时      : {sum(r['seconds'] or 0 for r in rows) / len(rows):.1f}s
首次 / 最近   : {rows[0]['started_at']} / {rows[-1]['started_at']}
""")
    print("分数分布   :", "  ".join(f"{r['score']}分×{r['n']}" for r in dist) or "（无）")
    hi = sum(r["n"] for r in dist if (r["score"] or 0) >= 7)
    print(f"高分占比   : {hi}/{analyzed} = {hi / analyzed * 100:.0f}%" if analyzed else "")
    return 0


def main() -> int:
    load_env()
    ap = argparse.ArgumentParser(description="AI Radar")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--fetch-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--reanalyze", action="store_true")
    ap.add_argument("--render", action="store_true", help="按当前版式重出今天的日报（不花钱）")
    ap.add_argument("--stats", action="store_true")
    ap.add_argument("--like", nargs="+", metavar="N", help="标记想看：--like 1 3 7")
    ap.add_argument("--skip", nargs="+", metavar="N", help="标记跳过：--skip 2 5")
    ap.add_argument("--feedback", action="store_true", help="查看反馈统计与偏好建议")
    ap.add_argument("--limit", type=int, default=10, help="每个源最多处理条数")
    args = ap.parse_args()

    if args.check:
        return check_api()
    if args.stats:
        return print_stats()
    if args.like:
        return add_feedback(args.like, "like")
    if args.skip:
        return add_feedback(args.skip, "skip")
    if args.feedback:
        return show_feedback()
    if args.reanalyze:
        return reanalyze()
    if args.render:
        return rebuild_digest()
    return run(dry_run=args.dry_run, fetch_only=args.fetch_only, limit=args.limit)


if __name__ == "__main__":
    sys.exit(main())
