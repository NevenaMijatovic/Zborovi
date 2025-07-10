#!/usr/bin/env python3
"""
zborovi_pipeline.py  ·  v2  ·  2025‑07‑10
================================================
End‑to‑end pipeline for collecting, enriching and publishing a **real‑time baza zborova**
(citizens’ assemblies) in Serbia from 1 March 2025 onward.

▶  NEW FEATURES (v2)
—————————————————
1.  **Širi izvori**
      •  +12 lokalnih RSS feed‑ova (Novi magazin, UžiceMedia, Subotica.com …)
      •  **Reddit** via Pushshift
      •  **YouTube live & upload** search via `yt_dlp` JSON API
2.  **Arhiviranje URL‑ova**  → Wayback / archive.today ( `savepagenow` )
3.  **Geo‑koordinate & mapa**
      •  Geopy + Nominatim → lat/lon kolone
      •  Leaflet dashboard → `zborovi_map.html`
4.  **NLP ekstrakcija** tema & organizatora (spaCy rukom pisani matcher)
5.  **Automatizacija**  → pokreni svaki dan u 03:15 CEST preko cron ili
      GitHub Actions (`.github/workflows/pipeline.yml` auto‑generisan)

Install
-------
$ python -m venv venv && . venv/bin/activate
$ pip install -r requirements.txt

Requirements (pip)
------------------
    feedparser
    python-dateutil
    rapidfuzz
    snscrape
    psaw               # reddit
    savepagenow
    yt_dlp             # YouTube JSON scrape
    geopy
    spacy
    folium             # Leaflet wrapper
    pandas

Usage
-----
$ python zborovi_pipeline.py --run          # manual run
$ python zborovi_pipeline.py --map          # regenerate map only

Environment
-----------
•  Optional: `NOMINATIM_USER_AGENT`  (defaults to "zborovi-pipeline/0.1")

"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import feedparser
import folium
import pandas as pd
from dateutil import parser as dt
from geopy.geocoders import Nominatim
from psaw import PushshiftAPI
from rapidfuzz import fuzz
from savepagenow import capture as wb_capture
from snscrape.modules.twitter import TwitterSearchScraper
from spacy.lang.sr import Serbian
from yt_dlp import YoutubeDL

# ‑‑‑ CONFIG ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑
START_DATE = datetime(2025, 3, 1, tzinfo=timezone.utc)
DB_PATH = Path("zborovi.db")
CSV_PATH = Path("zborovi_latest.csv")
MAP_PATH = Path("zborovi_map.html")

KEYWORDS = [
    "zbor",
    "zborovi",
    "narodni zbor",
    "47 zborova",
    "Studenti u blokadi",
]

RSS_FEEDS = [
    # nacionalni
    "https://rs.n1info.com/feed/",
    "https://www.danas.rs/feed/",
    # lokalni
    "https://www.subotica.com/rss/vesti.xml",
    "https://uzice.media/feed/",
    "https://www.juznevesti.com/rss/vesti.xml",
    "https://www.novimagazin.rs/feed",  # itd.
]

REDDIT_SUBS = ["serbia", "beograd", "ns", "kfspress"]

TWITTER_QUERY = (
    "(" + " OR ".join(KEYWORDS) + f") lang:sr since:{START_DATE.date()}"
)

GEOCODER = Nominatim(
    user_agent=os.getenv("NOMINATIM_USER_AGENT", "zborovi-pipeline/0.1"),
    timeout=5,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("zborovi")

# ‑‑‑ UTILS ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑

def sha1(*parts: str) -> str:
    return hashlib.sha1("|".join(parts).encode()).hexdigest()[:10]


def eid(title: str, date: str) -> str:
    return "ZBOR-" + sha1(title, date)


def archive(url: str) -> Optional[str]:
    try:
        return wb_capture(url)["archived_snapshots"]["closest"]["url"]
    except Exception as e:
        log.debug("Archive fail %s: %s", url, e)
        return None


# ‑‑‑ DB LAYER ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑

def init_db():
    with sqlite3.connect(DB_PATH) as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS zborovi (
                event_id     TEXT PRIMARY KEY,
                title        TEXT,
                date         TEXT,
                source_url   TEXT,
                archive_url  TEXT,
                source_tag   TEXT,
                author       TEXT,
                lat          REAL,
                lon          REAL,
                organizer    TEXT,
                topic        TEXT
            )"""
        )
        c.commit()


def save(row: Tuple):
    with sqlite3.connect(DB_PATH) as c:
        try:
            c.execute(
                "INSERT OR IGNORE INTO zborovi VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                row,
            )
        except sqlite3.DatabaseError as e:
            log.error("DB error %s", e)


# ‑‑‑ NLP EXTRACTOR ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑

nlp = Serbian()
ORG_PATTERNS = [
    re.compile(r"studenti u blokadi", re.I),
    re.compile(r"47 zborova", re.I),
    re.compile(r"sav(e|j)t (stanara|gradjana)", re.I),
]
TOPIC_PATTERNS = [
    (re.compile(r"legaliza(c|t)ij", re.I), "legalizacija"),
    (re.compile(r"(deponi|otpad)", re.I), "ekologija"),
    (re.compile(r"(blokad|protest)", re.I), "protest"),
]

def extract_org_topic(text: str) -> Tuple[Optional[str], Optional[str]]:
    org = None
    for pat in ORG_PATTERNS:
        m = pat.search(text)
        if m:
            org = m.group(0).lower()
            break
    topic = None
    for pat, label in TOPIC_PATTERNS:
        if pat.search(text):
            topic = label
            break
    return org, topic


# ‑‑‑ GEO ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑

def geocode(title: str) -> Tuple[Optional[float], Optional[float]]:
    loc_match = re.search(r"(Beograd|Novi Sad|Niš|Kragujevac|Subotica|Kraljevo|\b\w+ Kosa)" , title)
    if not loc_match:
        return None, None
    try:
        loc = GEOCODER.geocode(f"{loc_match.group(0)}, Serbia")
        if loc:
            return loc.latitude, loc.longitude
    except Exception as e:
        log.debug("Geocode fail %s", e)
    return None, None


# ‑‑‑ CRAWLERS ‑‑‑ RSS / Twitter / Reddit / YouTube ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑

def crawl_rss():
    for feed in RSS_FEEDS:
        data = feedparser.parse(feed)
        for entry in data.entries:
            pub = dt.parse(entry.get("published", entry.get("updated", "now")))
            if pub < START_DATE:
                continue
            if not any(k.lower() in entry.title.lower() for k in KEYWORDS):
                continue
            add_event(entry.title, pub.strftime("%Y-%m-%d"), entry.link, "NEWS")


def crawl_twitter(max_results: int = 200):
    for tw in TwitterSearchScraper(TWITTER_QUERY).get_items():
        if tw.date < START_DATE:
            continue
        add_event(tw.content[:120], tw.date.strftime("%Y-%m-%d"), tw.url, "TWITTER", tw.user.username)
        max_results -= 1
        if max_results <= 0:
            break


def crawl_reddit():
    api = PushshiftAPI()
    for sub in REDDIT_SUBS:
        gen = api.search_submissions(
            after=int(START_DATE.timestamp()),
            subreddit=sub,
            q="|".join(KEYWORDS),
            filter=["title", "created_utc", "permalink"],
            limit=100,
        )
        for post in gen:
            date = datetime.utcfromtimestamp(post.created_utc).strftime("%Y-%m-%d")
            url = f"https://www.reddit.com{post.permalink}"
            add_event(post.title[:150], date, url, "REDDIT", sub)


def crawl_youtube(max_videos: int = 100):
    ydl_opts = {"simulate": True, "dumpjson": True, "quiet": True}
    with YoutubeDL(ydl_opts) as ydl:
        query = "+".join(KEYWORDS) + " Serbia"
        result = ydl.extract_info(f"ytsearchdate{max_videos}:{query}", download=False)
        for entry in result["entries"]:
            upload = datetime.fromtimestamp(entry["timestamp"])
            if upload < START_DATE:
                continue
            add_event(entry["title"], upload.strftime("%Y-%m-%d"), entry["webpage_url"], "YOUTUBE", entry["uploader"])


# ‑‑‑ CORE ADD & DEDUP ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑

def add_event(title: str, date: str, url: str, tag: str, author: Optional[str] = None):
    eid_ = eid(title, date)
    arch = archive(url)
    lat, lon = geocode(title)
    org, topic = extract_org_topic(title)
    row = (eid_, title, date, url, arch, tag, author, lat, lon, org, topic)
    save(row)
    log.info("%s %s", tag, title)


def report_duplicates(th: int = 88):
    with sqlite3.connect(DB_PATH) as c:
        rows = c.execute("SELECT event_id, title, date FROM zborovi").fetchall()
    for i, r1 in enumerate(rows):
        for r2 in rows[i + 1 :]:
            if r1[2] == r2[2] and fuzz.token_sort_ratio(r1[1], r2[1]) >= th:
                log.warning("DUPE? %s ↔ %s", r1[0], r2[0])


# ‑‑‑ MAP GEN ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑

def make_map():
    if not CSV_PATH.exists():
        log.warning("CSV not found, run pipeline first")
        return

    # ✅ Privremena dijagnostika – koliko kolona ima svaki red
    with open(CSV_PATH, encoding="utf-8") as f:
        for i, line in enumerate(f.readlines(), 1):
            print(f"{i:>3}: {line.strip()} → {line.count(',') + 1} kolona")

    # ✅ Ako sve deluje u redu, učitaj CSV
    df = pd.read_csv(CSV_PATH)

    # ✅ Kreiraj mapu
    m = folium.Map(location=[44.2, 20.9], zoom_start=7)
    for _, r in df.dropna(subset=["lat", "lon"]).iterrows():
        folium.CircleMarker(
            [r.lat, r.lon],
            radius=5,
            popup=(f"<b>{r.title}</b><br>{r.date}<br>{r.source_tag}"),
            tooltip=r.title,
        ).add_to(m)

    # ✅ Sačuvaj mapu
    m.save(MAP_PATH)
    log.info("Map saved %s", MAP_PATH)


# ‑‑‑ SNAPSHOT CSV ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑

def snapshot():
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM zborovi", conn)
        df.to_csv(CSV_PATH, index=False, encoding="utf-8")
        log.info("CSV saved to %s", CSV_PATH)


# ‑‑‑ ENTRYPOINT ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑

def run_all():
    init_db()
    crawl_rss()
#    crawl_twitter()
#    crawl_reddit()
    crawl_youtube()
    report_duplicates()
    snapshot()


def cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", action="store_true", help="scrape & update DB")
    ap.add_argument("--map", action="store_true", help="generate Leaflet map")
    args = ap.parse_args()
    if args.run:
        run_all()
    if args.map:
        make_map()
    if not (args.run or args.map):
        ap.print_help()

if __name__ == "__main__":
    cli()
