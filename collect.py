#!/usr/bin/env python3
"""Буферный сборщик модуля мониторинга инцидентов с БПЛА на объектах ТЭК.

Запускается GitHub Actions по расписанию и складывает сырые сообщения открытых
источников в data/YYYY-MM-DD.jsonl, когда основной стенд выключен. Формат записи
и хэш (SHA-256 от url+текст) совпадают с Collector Service, поэтому при импорте
в модуль (services/collector/app/buffer_import.py) повторы отсекаются штатной
дедупликацией (ФТ-3), а сообщения проходят обычный путь Classifier → Incident Service.

Режимы:
  python collect.py                                  # регулярный опрос (новое с прошлого запуска)
  python collect.py --since 2026-09-26 --until 2026-10-22   # догрузка пропуска
"""
import argparse, hashlib, json, re, sys, time, urllib.parse, urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import feedparser
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent
DATA, STATE = ROOT / "data", ROOT / "state.json"
UA_TG = "Mozilla/5.0 (compatible; incident-monitoring-collector/0.1)"
UA_RSS = "incident-monitoring-collector/0.1 (+https://github.com/)"
MAX_PAGES_REGULAR, MAX_PAGES_BACKFILL, SEEN_CAP = 15, 400, 40000


def get(url, ua):
    req = urllib.request.Request(url, headers={"User-Agent": ua})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read()
        except Exception as exc:  # noqa: BLE001 — сбой одного источника не роняет запуск (ФТ-30)
            if attempt == 3:
                raise
            time.sleep(3 * (attempt + 1))


def content_hash(url, raw_text):  # == services/collector/app/dedup.py
    h = hashlib.sha256(); h.update(url.encode("utf-8")); h.update(raw_text.encode("utf-8")); return h.hexdigest()


def strip_html(text):  # == services/collector/app/textclean.py
    if not text:
        return ""
    return re.sub(r"\s+", " ", BeautifulSoup(text, "html.parser").get_text(separator=" ")).strip()


# ---------- Telegram (t.me/s/<канал>) с пагинацией ?before=<id> ----------
def tg_page(channel, before=None):
    url = f"https://t.me/s/{channel}" + (f"?before={before}" if before else "")
    soup = BeautifulSoup(get(url, UA_TG).decode("utf-8", "replace"), "html.parser")
    out = []
    for m in soup.select(".tgme_widget_message"):
        post = m.get("data-post", "")
        try:
            mid = int(post.rsplit("/", 1)[-1])
        except ValueError:
            continue
        text_el = m.select_one(".tgme_widget_message_text")
        t = m.select_one("time.time")
        pub = None
        if t is not None and t.has_attr("datetime"):
            try:
                pub = datetime.fromisoformat(t["datetime"])
            except ValueError:
                pass
        raw = text_el.get_text(separator="\n", strip=True) if text_el is not None else ""
        link = m.select_one("a.tgme_widget_message_date")
        url = link["href"] if link is not None and link.has_attr("href") else f"https://t.me/s/{channel}"
        out.append((mid, url, raw, pub))
    return out


def collect_telegram(src, state, since=None, until=None):
    channel = src["url"].rstrip("/").rsplit("/", 1)[-1].lstrip("@")
    last_id = state["telegram"].get(channel, 0)
    items, before, pages, newest = [], None, 0, last_id
    limit = MAX_PAGES_BACKFILL if since else MAX_PAGES_REGULAR
    while pages < limit:
        page = tg_page(channel, before); pages += 1
        if not page:
            break
        newest = max(newest, max(p[0] for p in page))
        for mid, url, raw, pub in page:
            if not raw:
                continue
            if since:
                if pub and since <= pub < until:
                    items.append((url, raw, pub))
            elif mid > last_id:
                items.append((url, raw, pub))
        oldest_id = min(p[0] for p in page)
        oldest_pub = min((p[3] for p in page if p[3]), default=None)
        if since and oldest_pub and oldest_pub < since:
            break
        if not since and (oldest_id <= last_id or last_id == 0):
            break  # дошли до уже собранного (или первый запуск — только последняя страница)
        before = oldest_id
        time.sleep(1.0)
    if not since:
        state["telegram"][channel] = newest
    return items, pages


# ---------- RSS / Google News ----------
def collect_rss(src, since=None, until=None):
    urls = [src["url"]]
    if since and "news.google.com/rss/search" in src["url"]:
        # Google News отдаёт до ~100 записей на запрос — режем пропуск на окна по 3 суток
        p = urllib.parse.urlsplit(src["url"]); qs = dict(urllib.parse.parse_qsl(p.query))
        urls, d = [], since.date()
        while d < until.date():
            e = min(d + timedelta(days=3), until.date())
            q = dict(qs, q=f"{qs['q']} after:{d - timedelta(days=1)} before:{e}")
            urls.append(urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, urllib.parse.urlencode(q), "")))
            d = e
    items = []
    for u in urls:
        parsed = feedparser.parse(get(u, UA_RSS))
        for entry in parsed.entries:  # == services/collector/app/adapters/rss.py
            url = entry.get("link", "") or ""
            raw = f"{strip_html(entry.get('title', ''))}\n{strip_html(entry.get('summary', entry.get('description', '')))}".strip()
            if not raw or not url:
                continue
            pt = entry.get("published_parsed") or entry.get("updated_parsed")
            pub = datetime(*pt[:6], tzinfo=timezone.utc) if pt else None
            if since and pub and not (since <= pub < until):
                continue
            items.append((url, raw, pub))
        time.sleep(1.0)
    return items, len(urls)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since"); ap.add_argument("--until")
    a = ap.parse_args()
    since = datetime.fromisoformat(a.since).replace(tzinfo=timezone.utc) if a.since else None
    until = (datetime.fromisoformat(a.until).replace(tzinfo=timezone.utc) if a.until else datetime.now(timezone.utc)) if since else None
    sources = [s for s in json.loads((ROOT / "sources.json").read_text(encoding="utf-8")) if s.get("active", True)]
    state = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {"telegram": {}, "seen": []}
    seen_list = list(state["seen"]); seen = set(seen_list)
    now = datetime.now(timezone.utc)
    DATA.mkdir(exist_ok=True)
    out_file = DATA / (f"backfill_{a.since}_{a.until}.jsonl" if since else f"{now.date().isoformat()}.jsonl")
    new, report = [], []
    for src in sources:
        try:
            if src["type"] == "telegram":
                items, calls = collect_telegram(src, state, since, until)
            else:
                items, calls = collect_rss(src, since, until)
        except Exception as exc:  # noqa: BLE001
            report.append(f"{src['name']}: ОШИБКА {exc}")
            continue
        added = 0
        for url, raw, pub in items:
            h = content_hash(url, raw)
            if h in seen:
                continue
            seen.add(h); seen_list.append(h); added += 1
            new.append({"source_name": src["name"], "source_type": src["type"], "source_url": src["url"],
                        "url": url, "raw_text": raw, "published_at": pub.isoformat() if pub else None,
                        "fetched_at": now.isoformat(), "content_hash": h})
        report.append(f"{src['name']}: запросов {calls}, новых {added}")
    with out_file.open("a", encoding="utf-8") as f:
        for r in new:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    state["seen"] = seen_list[-SEEN_CAP:]
    state["last_run"] = now.isoformat()
    STATE.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    print("\n".join(report)); print(f"Итого новых сообщений: {len(new)} → {out_file.name}")
    if any("ОШИБКА" in r for r in report) and not new:
        sys.exit(1)


if __name__ == "__main__":
    main()
