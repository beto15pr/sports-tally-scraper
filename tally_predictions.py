#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prediction Tally Scraper
------------------------
Given a query like "Texans vs 49ers prediction", fetch the first N Google results (via Serper.dev or SerpAPI),
filter to pages published in the last --days (default 5), try to *extract who wins*, and tally votes.
Outputs a CSV of sources and a quick console/Markdown summary.

Usage:
  export SERPER_API_KEY=...
  python tally_predictions.py \
    --provider serper \
    --query "Texans vs 49ers prediction" \
    --team-a "Houston Texans,Texans,Houston" \
    --team-b "San Francisco 49ers,49ers,San Francisco,SF" \
    --results 50 --days 5 \
    --allow "espn.com,actionnetwork.com,covers.com,pickswise.com,rotowire.com,usatoday.com,sportingnews.com,cbssports.com,oddsshark.com" \
    --out predictions_texans_49ers.csv --md predictions_texans_49ers.md

Notes:
- Only articles with a detectable publish date within the last N days are counted.
- If the script cannot detect a winner with high confidence, it marks the row as 'ambiguous' and excludes from the tally by default.
- Respect site Terms, robots.txt, and rate limits.
"""
import os, re, json, argparse, sys, time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup
from readability import Document
from dateutil import parser as dtparse
from dotenv import load_dotenv

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"

def parse_args():
    ap = argparse.ArgumentParser(description="Tally predicted winner from search results in the last N days.")
    ap.add_argument("--provider", choices=["serper","serpapi"], default="serper", help="Search provider backend")
    ap.add_argument("--api-key", default=None, help="API key (falls back to SERPER_API_KEY or SERPAPI_KEY env vars)")
    ap.add_argument("--query", required=True, help="Search query, e.g. 'Texans vs 49ers prediction'")
    ap.add_argument("--team-a", required=True, help="Comma-separated synonyms for Team A (e.g., 'Houston Texans,Texans,Houston')")
    ap.add_argument("--team-b", required=True, help="Comma-separated synonyms for Team B (e.g., 'San Francisco 49ers,49ers,San Francisco,SF')")
    ap.add_argument("--results", type=int, default=50, help="How many search results to fetch (max allowed by provider)")
    ap.add_argument("--days", type=int, default=5, help="Only include sources published within the last N days (strict)")
    ap.add_argument("--allow", default="", help="Comma-separated domain allowlist (optional)")
    ap.add_argument("--deny", default="reddit.com,facebook.com,youtube.com,twitter.com,x.com,instagram.com", help="Comma-separated domain denylist")
    ap.add_argument("--rate", type=float, default=1.0, help="Seconds to sleep between page fetches")
    ap.add_argument("--out", required=True, help="CSV output path")
    ap.add_argument("--md", default="", help="Optional Markdown output path")
    return ap.parse_args()

def env_key(provider, cli_key):
    if cli_key:
        return cli_key
    if provider == "serper":
        return os.getenv("SERPER_API_KEY") or os.getenv("SERPER_KEY")
    if provider == "serpapi":
        return os.getenv("SERPAPI_KEY")
    return None

def map_recency_to_tbs(days):
    # We can't express '5 days' exactly; use qdr:w (last 7 days) to bias results,
    # then strictly filter by publish date later.
    if days <= 1:
        return "qdr:d"
    if days <= 7:
        return "qdr:w"
    if days <= 31:
        return "qdr:m"
    return None

def search_serper(api_key, query, num=10, tbs=None):
    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": api_key, "Content-Type":"application/json"}
    payload = {"q": query, "num": num}
    if tbs:
        payload["tbs"] = tbs
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    items = data.get("organic", []) or []
    results = []
    for it in items:
        results.append({
            "title": it.get("title"),
            "link": it.get("link"),
            "snippet": it.get("snippet"),
        })
    return results

def search_serpapi(api_key, query, num=10, tbs=None):
    url = "https://serpapi.com/search.json"
    params = {"engine":"google","q":query,"num":num,"api_key":api_key}
    if tbs:
        params["tbs"] = tbs
    r = requests.get(url, params=params, timeout=30, headers={"User-Agent": UA})
    r.raise_for_status()
    data = r.json()
    items = data.get("organic_results", []) or []
    results = []
    for it in items:
        results.append({
            "title": it.get("title"),
            "link": it.get("link"),
            "snippet": it.get("snippet"),
        })
    return results

def fetch_page(url):
    r = requests.get(url, timeout=30, headers={"User-Agent": UA})
    r.raise_for_status()
    html = r.text
    doc = Document(html)
    title = (doc.title() or "").strip()
    soup = BeautifulSoup(doc.summary(), "html.parser")
    text = soup.get_text("\n")
    pub = extract_date_from_html(html)
    return title, text, pub

def extract_date_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    # <time datetime="...">
    t = soup.find("time", attrs={"datetime": True})
    if t and t.get("datetime"):
        dt = normalize_date(t.get("datetime"))
        if dt: return dt
    # JSON-LD
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
        except Exception:
            continue
        for obj in (data if isinstance(data, list) else [data]):
            if isinstance(obj, dict):
                for k in ("datePublished","dateModified","uploadDate"):
                    val = obj.get(k)
                    if isinstance(val, str):
                        dt = normalize_date(val)
                        if dt: return dt
    # Meta
    for name in ("article:published_time","og:published_time","pubdate","publishdate","parsely-pub-date"):
        m = soup.find("meta", attrs={"property":name}) or soup.find("meta", attrs={"name":name})
        if m and m.get("content"):
            dt = normalize_date(m.get("content"))
            if dt: return dt
    return None

def normalize_date(s):
    try:
        dt = dtparse.parse(s)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

# --- UPDATED: stricter patterns and verification ---
def make_team_patterns(team_syns):
    """
    Build regexes + a lowercase synonym set.
    Returns (context_patterns, scoreline_pattern, synonym_set)
    """
    syn_set = {s.strip().lower() for s in team_syns if s.strip()}
    syns = [re.escape(x) for x in syn_set]
    syns = sorted(set(syns), key=len, reverse=True)
    name_group = "(" + "|".join(syns) + ")" if syns else "(.*)"

    contexts = [
        # Prefer explicit "Final score:" statements
        rf"(?:final\s*score[:\s]*{name_group}\s*\d{{1,2}}[,–-]\s*[A-Za-z\s\.']+\s*\d{{1,2}})",
        rf"(?:moneyline\s*pick\s*[:\-]?\s*{name_group})",
        rf"(?:pick\s*[:\-]?\s*{name_group})",
        rf"(?:prediction\s*[:\-]?\s*{name_group})",
        rf"(?:who\s*wins[?:]?\s*{name_group})",
        rf"(?:to\s*win\s*(?:outright|straight\s*up)?\s*[:\-]?\s*{name_group})",
        rf"(?:winner\s*[:\-]?\s*{name_group})",
        rf"(?:straight\s*up\s*[:\-]?\s*{name_group})",
    ]
    scoreline = rf"{name_group}\s*(\d{{1,2}})\s*[,–-]\s*([A-Za-z\s\.']+?)\s*(\d{{1,2}})"
    return [re.compile(c, re.IGNORECASE) for c in contexts], re.compile(scoreline, re.IGNORECASE), syn_set

def choose_winner(text, team_a_syns, team_b_syns):
    """
    Extract the predicted winner with false-positive guards.
    Returns: ("A"/"B"/"ambiguous", reason, matched_phrase)
    """
    # Prepare patterns + synonym sets
    ctx_a, score_a, set_a = make_team_patterns(team_a_syns)
    ctx_b, score_b, set_b = make_team_patterns(team_b_syns)

    def looks_like_spread(s: str) -> bool:
        # Avoid interpreting ATS/spread lines as straight-up winner calls
        return bool(re.search(r"\b(?:spread|cover|ATS)\b", s, flags=re.IGNORECASE)) or \
               bool(re.search(r"\b[+-]\d+(\.\d+)?\b", s))  # e.g., -2.5, +3

    # 0) Strong "final score" override
    mfs_a = re.search(
        rf"final\s*score[:\s]*({ '|'.join(re.escape(x) for x in set_a) })\s*(\d{{1,2}})\s*[,–-]\s*([A-Za-z\s\.']+?)\s*(\d{{1,2}})",
        text, re.IGNORECASE) if set_a else None
    if mfs_a:
        s1, s2 = int(mfs_a.group(2)), int(mfs_a.group(4))
        if s1 != s2:
            return ("A" if s1 > s2 else "B"), "final_score", mfs_a.group(0)

    mfs_b = re.search(
        rf"final\s*score[:\s]*({ '|'.join(re.escape(x) for x in set_b) })\s*(\d{{1,2}})\s*[,–-]\s*([A-Za-z\s\.']+?)\s*(\d{{1,2}})",
        text, re.IGNORECASE) if set_b else None
    if mfs_b:
        s1, s2 = int(mfs_b.group(2)), int(mfs_b.group(4))
        if s1 != s2:
            return ("B" if s1 > s2 else "A"), "final_score", mfs_b.group(0)

    # 1) Explicit contexts (skip spread-like phrases; verify token belongs to team set)
    for pat in ctx_a:
        ma = pat.search(text)
        if ma and not looks_like_spread(ma.group(0)):
            token = ma.group(1).strip().lower() if ma.groups() else ""
            if not set_a or token in set_a or token == "":
                return "A", "explicit", ma.group(0)

    for pat in ctx_b:
        mb = pat.search(text)
        if mb and not looks_like_spread(mb.group(0)):
            token = mb.group(1).strip().lower() if mb.groups() else ""
            if not set_b or token in set_b or token == "":
                return "B", "explicit", mb.group(0)

    # 2) Scoreline logic with team-token verification
    ma = score_a.search(text)
    if ma:
        try:
            token = ma.group(1).strip().lower()
            if not set_a or token in set_a:
                s1 = int(ma.group(2)); s2 = int(ma.group(4))
                if s1 != s2:
                    return ("A" if s1 > s2 else "B"), "scoreline", ma.group(0)
        except Exception:
            pass

    mb = score_b.search(text)
    if mb:
        try:
            token = mb.group(1).strip().lower()
            if not set_b or token in set_b:
                s1 = int(mb.group(2)); s2 = int(mb.group(4))
                if s1 != s2:
                    return ("B" if s1 > s2 else "A"), "scoreline", mb.group(0)
        except Exception:
            pass

    # 3) Weak fallback fields (avoid ATS)
    weak_ctx = [
        (re.compile(r"\bmoneyline[:\s]+([A-Za-z\.\s']+)", re.IGNORECASE), "moneyline_field"),
        (re.compile(r"\bpick[:\s]+([A-Za-z\.\s']+)", re.IGNORECASE), "pick_field"),
        (re.compile(r"\bprediction[:\s]+([A-Za-z\.\s']+)", re.IGNORECASE), "prediction_field"),
    ]
    for pat, tag in weak_ctx:
        m = pat.search(text)
        if not m or looks_like_spread(m.group(0)):
            continue
        blob = m.group(1).lower()
        if any(s.lower() in blob for s in team_a_syns):
            return "A", tag, m.group(0)
        if any(s.lower() in blob for s in team_b_syns):
            return "B", tag, m.group(0)

    return "ambiguous", "none", ""

def within_days(utc_dt, days):
    if not utc_dt:
        return False
    return utc_dt >= datetime.now(timezone.utc) - timedelta(days=days)

def main():
    args = parse_args()
    load_dotenv()
    key = env_key(args.provider, args.api_key)
    if not key:
        print("Missing API key. Provide --api-key or set SERPER_API_KEY / SERPAPI_KEY.", file=sys.stderr)
        sys.exit(2)

    allow = set([d.strip().lower() for d in args.allow.split(",") if d.strip()])
    deny  = set([d.strip().lower() for d in args.deny.split(",") if d.strip()])

    team_a_syns = [s.strip() for s in args.team_a.split(",") if s.strip()]
    team_b_syns = [s.strip() for s in args.team_b.split(",") if s.strip()]

    tbs = map_recency_to_tbs(args.days)

    # Search
    if args.provider == "serper":
        hits = search_serper(key, args.query, num=args.results, tbs=tbs)
    else:
        hits = search_serpapi(key, args.query, num=args.results, tbs=tbs)

    rows = []
    for h in hits:
        url = h.get("link")
        if not url:
            continue
        dom = urlparse(url).netloc.lower()
        if allow and not any(ad in dom for ad in allow):
            continue
        if any(dd in dom for dd in deny):
            continue
        try:
            title, text, pub = fetch_page(url)
        except Exception:
            # Skip troublesome pages gracefully
            continue
        pub_dt = pub if isinstance(pub, datetime) else None
        if not within_days(pub_dt, args.days):
            # Strict: must be within last N days AND date must be detected.
            continue

        # Extract winner
        who, method, phrase = choose_winner(" ".join([title or "", h.get("snippet","") or "", text or ""]), team_a_syns, team_b_syns)

        rows.append({
            "published_utc": pub_dt.isoformat() if pub_dt else "",
            "domain": dom,
            "url": url,
            "result_title": h.get("title",""),
            "page_title": title or "",
            "snippet": h.get("snippet","") or "",
            "winner": who,           # "A", "B", or "ambiguous"
            "winner_method": method, # "explicit", "scoreline", "moneyline_field", etc.
            "match_phrase": phrase,
        })
        time.sleep(args.rate)

    df = pd.DataFrame(rows)
    if df.empty:
        print("No eligible articles found in the last {} days (or filters too strict).".format(args.days))
        # Still write empty CSV for consistency
        df.to_csv(args.out, index=False)
        if args.md:
            with open(args.md, "w", encoding="utf-8") as f:
                f.write("# Prediction Tally (No eligible sources)\n")
        sys.exit(0)

    # Save all rows
    df.to_csv(args.out, index=False)

    # Build tally excluding ambiguous
    a_votes = int((df["winner"] == "A").sum())
    b_votes = int((df["winner"] == "B").sum())
    ambig   = int((df["winner"] == "ambiguous").sum())

    team_a_label = team_a_syns[0]
    team_b_label = team_b_syns[0]

    print("\n=== Prediction Tally (last {} days) ===".format(args.days))
    print(f"{team_a_label}: {a_votes}")
    print(f"{team_b_label}: {b_votes}")
    print(f"Ambiguous/Unclear (excluded): {ambig}")
    print(f"Sources saved to: {args.out}")
    if args.md:
        with open(args.md, "w", encoding="utf-8") as f:
            f.write(f"# Prediction Tally – last {args.days} days\n\n")
            f.write(f"- **{team_a_label}**: {a_votes}\n")
            f.write(f"- **{team_b_label}**: {b_votes}\n")
            f.write(f"- Ambiguous/Unclear (excluded): {ambig}\n\n")
            f.write("## Sources\n")
            for _, r in df.iterrows():
                f.write(f"- {r['published_utc']} — **{r['page_title'] or r['result_title']}** ({r['domain']}) — winner: {r['winner']} via {r['winner_method']}\n  \n  <{r['url']}>\n\n")
        print(f"Markdown summary: {args.md}")

if __name__ == "__main__":
    main()
