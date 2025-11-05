import sys, re, gzip
from datetime import datetime, timedelta, timezone
import requests
from lxml import etree
from io import BytesIO

URLS = [
    "https://epghub.xyz/epg/EPG-BEIN-CY-DELUXEMUSIC.xml",
    "https://epghub.xyz/epg/EPG-DIRECTVSPORTS-DISTROTV-DRAFTKINGS.xml",
    "https://epghub.xyz/epg/EPG-DUMMY-CHANNELS-FANDUEL-LOCOMOTIONTV.xml",
    "https://epghub.xyz/epg/EPG-PAC-PEACOCK-PLEX.xml",
    "https://epghub.xyz/epg/EPG-POWERNATION-RAKUTEN-SPORTKLUB.xml",
    "https://epghub.xyz/epg/EPG-SSPORTPLUS-TBNPLUS-THESPORTPLUS.xml",
    "https://epghub.xyz/epg/EPG-US-US-LOCALS-US-SPORTS.xml",
    "https://epghub.xyz/epg/EPG-VOA.xml",
]

# Keep a rolling window to shrink size (adjust if you want)
KEEP_PAST_DAYS = 3     # keep 3 days behind "now"
KEEP_FUTURE_DAYS = 14  # and 14 days ahead

def parse_xmltv_time(ts: str):
    """
    Parse XMLTV times like 'YYYYMMDDHHMMSS ±HHMM' or 'YYYYMMDDHHMMSSZ' or without TZ.
    Returns timezone-aware UTC datetime, or None if unparsable.
    """
    if not ts:
        return None
    m = re.match(r"(\d{14})(?:\s*([+\-]\d{4}|Z))?", ts)
    if not m:
        return None
    base = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
    tz = m.group(2)
    if tz and tz != "Z":
        sign = 1 if tz[0] == "+" else -1
        hours = int(tz[1:3]); mins = int(tz[3:5])
        offset = timezone(sign * timedelta(hours=hours, minutes=mins))
        dt = base.replace(tzinfo=offset).astimezone(timezone.utc)
    else:
        dt = base.replace(tzinfo=timezone.utc)
    return dt

def intersects_window(start_dt, stop_dt, win_start, win_end):
    # keep programme if it overlaps the window at all
    if not start_dt and not stop_dt:
        return True
    if not start_dt:
        return stop_dt >= win_start
    if not stop_dt:
        return start_dt <= win_end
    return (start_dt <= win_end) and (stop_dt >= win_start)

def fetch_xml(url):
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    content = r.content
    # transparent gunzip if needed
    if content[:2] == b"\x1f\x8b":
        import gzip as gz
        content = gz.decompress(content)
    return etree.parse(BytesIO(content))

def main():
    now = datetime.now(timezone.utc)
    win_start = now - timedelta(days=KEEP_PAST_DAYS)
    win_end   = now + timedelta(days=KEEP_FUTURE_DAYS)

    tv_root = etree.Element("tv")
    channel_ids_seen = set()
    programme_keys_seen = set()

    for url in URLS:
        try:
            doc = fetch_xml(url)
        except Exception as e:
            print(f"WARNING: failed to fetch {url}: {e}", file=sys.stderr)
            continue

        root = doc.getroot()

        # Copy channels (dedupe by id)
        for ch in root.findall("channel"):
            cid = ch.get("id") or ""
            if cid not in channel_ids_seen:
                channel_ids_seen.add(cid)
                tv_root.append(ch)

        # Copy programmes if they overlap our time window
        for pr in root.findall("programme"):
            ch = (pr.get("channel") or "")
            start_s = pr.get("start") or ""
            stop_s  = pr.get("stop") or ""

            start_dt = parse_xmltv_time(start_s)
            stop_dt  = parse_xmltv_time(stop_s)

            if not intersects_window(start_dt, stop_dt, win_start, win_end):
                continue

            key = (ch, start_s, stop_s, (pr.findtext("title") or "").strip())
            if key in programme_keys_seen:
                continue
            programme_keys_seen.add(key)
            tv_root.append(pr)

    # Sort for stability
    channels = [e for e in tv_root.findall("channel")]
    programmes = [e for e in tv_root.findall("programme")]
    for e in channels + programmes:
        tv_root.remove(e)
    channels.sort(key=lambda c: (c.get("id") or ""))
    programmes.sort(key=lambda p: (p.get("start") or ""))
    for e in channels + programmes:
        tv_root.append(e)

    # Write directly to gzip to stay under GitHub's 100MB limit
    tree = etree.ElementTree(tv_root)
    with gzip.open("merged_epg.xml.gz", "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True, pretty_print=False)

if __name__ == "__main__":
    main()
