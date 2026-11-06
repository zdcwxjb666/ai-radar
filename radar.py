#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Radar — 第 1 周骨架（零依赖，只用 Python 标准库）

功能：
    RSS/Atom 抓取 -> hash 去重入库 -> LLM 结构化打分 -> 生成 Markdown 日报 -> (可选) 发邮件

用法：
    python radar.py --check        # D1：测试 API 连通性
    python radar.py --fetch-only   # D2：只抓取+去重，不花钱
    python radar.py --dry-run      # D3：完整跑一遍，但不写库、不发信
    python radar.py                # 正常跑
    python radar.py --stats        # 看累计数据（写简历用）

设计原则：
    1. 状态落在 SQLite，每条内容只处理一次（幂等）
    2. 任何一环失败都不整体崩溃，但会记录、会返回非零退出码（好让定时任务告警）
    3. 结构化输出，下游可编程消费
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

# Windows 控制台默认 GBK，先切成 UTF-8，否则中文/emoji 会炸
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "radar.db"
LOG_PATH = DATA_DIR / "radar.log"

UA = "Mozilla/5.0 (compatible; AIRadar/0.1; +https://github.com/)"


# --------------------------------------------------------------------------
# 配置
# --------------------------------------------------------------------------
def load_env() -> None:
    """极简 .env 解析，避免引入 python-dotenv。已存在的环境变量优先。"""
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
    published     TEXT,
    fetched_at    TEXT,
    status        TEXT DEFAULT 'new',     -- new / analyzed / failed
    relevance     INTEGER,
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
    ok          INTEGER DEFAULT 1,
    note        TEXT
);
"""


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def make_hash(url: str, title: str) -> str:
    """URL 优先；没 URL 用标题。归一化后取 sha1，保证同一条内容只进一次。"""
    key = (url or title or "").strip().lower()
    key = re.sub(r"[?#].*$", "", key)          # 去掉 utm 等查询参数
    key = re.sub(r"^https?://(www\.)?", "", key)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# 抓取
# --------------------------------------------------------------------------
ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}


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


def parse_feed(xml_bytes: bytes) -> list[dict]:
    """同时支持 RSS 2.0 与 Atom。解析不了的条目直接跳过，不炸整批。"""
    out: list[dict] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        raise RuntimeError(f"XML 解析失败: {e}") from e

    if root.tag.endswith("feed"):  # Atom
        for e in root.findall("a:entry", ATOM_NS):
            link_el = e.find("a:link", ATOM_NS)
            link = link_el.get("href", "") if link_el is not None else ""
            out.append({
                "title": (e.findtext("a:title", "", ATOM_NS) or "").strip(),
                "url": link,
                "published": (e.findtext("a:updated", "", ATOM_NS) or "").strip(),
                "content": (e.findtext("a:summary", "", ATOM_NS)
                            or e.findtext("a:content", "", ATOM_NS) or "").strip(),
            })
    else:  # RSS 2.0
        for e in root.findall("./channel/item"):
            out.append({
                "title": (e.findtext("title") or "").strip(),
                "url": (e.findtext("link") or "").strip(),
                "published": (e.findtext("pubDate") or "").strip(),
                "content": re.sub(r"<[^>]+>", " ", e.findtext("description") or "").strip(),
            })
    return [x for x in out if x["title"] or x["url"]]


def strip_html(text: str) -> str:
    text = re.sub(r"<script.*?</script>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# --------------------------------------------------------------------------
# LLM 调用（OpenAI 兼容协议，DeepSeek / 智谱 / Moonshot / 通义 都适用）
# --------------------------------------------------------------------------
def chat(messages: list[dict], json_mode: bool = True, max_tokens: int = 600) -> tuple[str, int]:
    base = cfg("LLM_BASE_URL", "https://api.deepseek.com/v1").rstrip("/")
    key = cfg("LLM_API_KEY")
    model = cfg("LLM_MODEL", "deepseek-chat")
    if not key:
        raise RuntimeError("没有配置 LLM_API_KEY，请复制 .env.example 为 .env 并填入")
    if not key.isascii() or key.startswith("sk-你的"):
        raise RuntimeError("LLM_API_KEY 里还是占位符或含非 ASCII 字符，请填真实 key（否则请求头编码会失败）")

    body: dict = {
        "model": model,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
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
        detail = e.read().decode("utf-8", "replace")[:300]
        raise RuntimeError(f"API 返回 {e.code}: {detail}") from e

    content = data["choices"][0]["message"]["content"]
    tokens = int(data.get("usage", {}).get("total_tokens", 0))
    return content, tokens


def extract_json(text: str) -> dict:
    """模型偶尔会在 JSON 外面包 ``` 或加解释，这里做一次容错提取。"""
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


PROMPT_TMPL = (ROOT / "prompts" / "analyze.md")


def analyze(item: dict) -> tuple[dict, int]:
    profile = cfg("PROFILE", "在校大学生，目标进入 AI 应用工程岗")
    tmpl = PROMPT_TMPL.read_text(encoding="utf-8") if PROMPT_TMPL.exists() else (
        '为下面内容打分。只输出 JSON：{"relevance":0-10,"summary":"","reason":"","tags":[],"action":"read|skim|skip"}'
    )
    prompt = (tmpl.replace("{profile}", profile)
                  .replace("{title}", item["title"])
                  .replace("{source}", item.get("source", ""))
                  .replace("{content}", strip_html(item.get("content", ""))[:1200]))

    raw, tokens = chat([
        {"role": "system", "content": "你是一个严格的信息筛选助手，只输出合法 JSON。"},
        {"role": "user", "content": prompt},
    ])
    d = extract_json(raw)

    # 兜底与裁剪：模型给什么都不信，自己校验一遍
    rel = d.get("relevance", 0)
    rel = int(rel) if str(rel).lstrip("-").isdigit() else 0
    d["relevance"] = max(0, min(10, rel))
    d["summary"] = str(d.get("summary", ""))[:120]
    d["reason"] = str(d.get("reason", ""))[:80]
    d["action"] = str(d.get("action", "skim"))
    tags = d.get("tags") or []
    d["tags"] = ",".join(str(t) for t in tags[:4]) if isinstance(tags, list) else str(tags)[:60]
    return d, tokens


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------
def feeds() -> list[str]:
    raw = cfg("FEEDS")
    return [u.strip() for u in re.split(r"[,\n]", raw) if u.strip().startswith("http")]


def run(dry_run: bool = False, fetch_only: bool = False, limit: int = 20) -> int:
    # --fetch-only 不落库：否则这些条目会被当成"已见过"，正式跑时被跳过
    conn = db() if not (dry_run or fetch_only) else None
    started = datetime.now().isoformat(timespec="seconds")
    fetched = new_items = analyzed = failed = tokens = 0
    results: list[dict] = []

    src_list = feeds()
    if not src_list:
        log("没有配置 FEEDS，请检查 .env")
        return 2

    for src in src_list:
        try:
            items = parse_feed(http_get(src))[:limit]
            log(f"{src} -> {len(items)} 条")
        except Exception as e:  # noqa: BLE001
            log(f"[失败] {e}")
            failed += 1
            continue

        for it in items:
            fetched += 1
            it["source"] = src
            h = make_hash(it["url"], it["title"])

            if conn:
                seen = conn.execute("SELECT 1 FROM items WHERE hash=?", (h,)).fetchone()
                if seen:
                    continue
                conn.execute(
                    "INSERT INTO items(hash,source,url,title,published,fetched_at) VALUES(?,?,?,?,?,?)",
                    (h, src, it["url"], it["title"], it.get("published", ""),
                     datetime.now().isoformat(timespec="seconds")),
                )
                conn.commit()
            new_items += 1

            if fetch_only:
                results.append({**it, "relevance": None})
                continue

            try:
                d, tk = analyze(it)
                tokens += tk
                analyzed += 1
                results.append({**it, **d})
                if conn:
                    conn.execute(
                        "UPDATE items SET status='analyzed',relevance=?,summary=?,reason=?,action=?,tags=? WHERE hash=?",
                        (d["relevance"], d["summary"], d["reason"], d["action"], d["tags"], h),
                    )
                    conn.commit()
            except Exception as e:  # noqa: BLE001
                failed += 1
                log(f"[分析失败] {it['title'][:40]} :: {e}")
                if conn:
                    conn.execute("UPDATE items SET status='failed' WHERE hash=?", (h,))

    report = render(results, fetch_only)
    out = DATA_DIR / f"digest-{datetime.now():%Y-%m-%d}.md"
    if not dry_run:
        out.write_text(report, encoding="utf-8")
        log(f"日报已写入 {out}")

    if not fetch_only and not dry_run:
        send_email_if_configured(report)

    if conn:
        conn.execute(
            "INSERT INTO runs(started_at,finished_at,fetched,new_items,analyzed,failed,tokens,ok,note) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (started, datetime.now().isoformat(timespec="seconds"),
             fetched, new_items, analyzed, failed, tokens,
             0 if failed and failed >= max(1, fetched // 2) else 1, ""),
        )
        conn.commit()

    log(f"完成：抓取 {fetched} / 新增 {new_items} / 分析 {analyzed} / 失败 {failed} / tokens {tokens}")
    print("\n" + report)

    # 一半以上环节失败 -> 非零退出码，让定时任务能发现
    return 1 if (failed and failed >= max(1, fetched // 2)) else 0


def render(results: list[dict], fetch_only: bool) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    if fetch_only:
        lines = [f"# Radar 抓取预览 · {today}", "", f"共 {len(results)} 条（未调用 LLM）", ""]
        lines += [f"- {r['title']}\n  {r['url']}" for r in results]
        return "\n".join(lines)

    top = sorted([r for r in results if (r.get("relevance") or 0) >= 7],
                 key=lambda x: -x["relevance"])
    rest = sorted([r for r in results if (r.get("relevance") or 0) < 7],
                  key=lambda x: -(x.get("relevance") or 0))

    lines = [f"# Radar · {today}", "",
             f"今天看了 {len(results)} 条，**{len(top)} 条值得你看**。", ""]
    if top:
        lines.append("## 值得看\n")
        for r in top:
            lines += [
                f"### {r['relevance']}/10 · {r['title']}",
                "",
                f"> {r.get('summary', '')}",
                "",
                f"- **为什么**：{r.get('reason', '')}",
                f"- **标签**：{r.get('tags', '')}",
                f"- **链接**：{r['url']}",
                "",
            ]
    if rest:
        lines.append("## 略过\n")
        lines += [f"- {r.get('relevance', '?')}/10 {r['title']}" for r in rest]
    return "\n".join(lines)


def send_email_if_configured(report: str) -> None:
    host, user, pw = cfg("SMTP_HOST"), cfg("SMTP_USER"), cfg("SMTP_PASS")
    to = cfg("MAIL_TO", user)
    if not (host and user and pw and to):
        log("未配置 SMTP，跳过发信（可先看 data/digest-*.md）")
        return
    msg = EmailMessage()
    msg["Subject"] = f"Radar {datetime.now():%m-%d}"
    msg["From"], msg["To"] = user, to
    msg.set_content(report)
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(host, 465, context=ctx) as s:
        s.login(user, pw)
        s.send_message(msg)
    log(f"邮件已发送 -> {to}")


# --------------------------------------------------------------------------
# 辅助命令
# --------------------------------------------------------------------------
def check_api() -> int:
    log("测试 API 连通性…")
    try:
        content, tokens = chat([{"role": "user", "content": "只回复两个字：收到"}],
                               json_mode=False, max_tokens=20)
        log(f"成功 ✅ 模型回复：{content.strip()!r}，tokens={tokens}")
        return 0
    except Exception as e:  # noqa: BLE001
        log(f"失败 ❌ {e}")
        return 1


def print_stats() -> int:
    conn = db()
    rows = conn.execute("SELECT * FROM runs ORDER BY id").fetchall()
    if not rows:
        print("还没有运行记录。先跑一次 python radar.py")
        return 0
    total_items = conn.execute("SELECT COUNT(*) c FROM items").fetchone()["c"]
    analyzed = conn.execute("SELECT COUNT(*) c FROM items WHERE status='analyzed'").fetchone()["c"]
    days = len({r["started_at"][:10] for r in rows})
    tokens = sum(r["tokens"] or 0 for r in rows)
    print(f"""
运行天数      : {days}
运行次数      : {len(rows)}
累计处理条目  : {total_items}
已分析        : {analyzed}
累计 tokens   : {tokens:,}
首次运行      : {rows[0]['started_at']}
最近运行      : {rows[-1]['started_at']}
""")
    return 0


def main() -> int:
    load_env()
    ap = argparse.ArgumentParser(description="AI Radar")
    ap.add_argument("--check", action="store_true", help="测试 API 连通性")
    ap.add_argument("--fetch-only", action="store_true", help="只抓取和去重，不调 LLM")
    ap.add_argument("--dry-run", action="store_true", help="完整跑但不写库不发信")
    ap.add_argument("--stats", action="store_true", help="查看累计统计")
    ap.add_argument("--limit", type=int, default=20, help="每个源最多处理多少条")
    args = ap.parse_args()

    if args.check:
        return check_api()
    if args.stats:
        return print_stats()
    return run(dry_run=args.dry_run, fetch_only=args.fetch_only, limit=args.limit)


if __name__ == "__main__":
    sys.exit(main())
