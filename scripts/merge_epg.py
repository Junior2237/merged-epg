import sys
import re
from datetime import datetime, timedelta, timezone
import requests
from lxml import etree
from io import BytesIO

"""
Merged EPG - Stable Version
Time window:
  - 2 days past
  - 7 days future
Output: merged_epg.xml
"""

URLS = [
    "https://epghub.xyz/epg/EPG-BEIN.xml",
    "https://epghub.xyz/epg/EPG-BR.xml",
    "https://epghub.xyz/epg/EPG-CA.xml",
    "https://epghub.xyz/epg/EPG-DELUXEMUSIC.xml",
    "https://epghub.xyz/epg/EPG-DIRECTVSPORTS.xml",
    "https://epghub.xyz/epg/EPG-DISTROTV.xml",
    "https://epghub.xyz/epg/EPG-DRAFTKINGS.xml",
    "https://epghub.xyz/epg/EPG-DUMMY-CHANNELS.xml",
    "https://epghub.xyz/epg/EPG-ES.xml",
    "https://epghub.xyz/epg/EPG-FANDUEL.xml",
    "https://epghub.xyz/epg/EPG-LOCOMOTIONTV.xml",
    "https://epghub.xyz/epg/EPG-PEACOCK.xml",
    "https://epghub.xyz/epg/EPG-PLEX.xml",
    "https://epghub.xyz/epg/EPG-POWERNATION.xml",
    "https://epghub.xyz/epg/EPG-RAKUTEN.xml",
    "https://epghub.xyz/epg/EPG-SPORTKLUB.xml",
    "https://epghub.xyz/epg/EPG-SSPORTPLUS.xml",
    "https://epghub.xyz/epg/EPG-TBNPLUS.xml",
    "https://epghub.xyz/epg/EPG-THESPORTPLUS.xml",
    "https://epghub.xyz/epg/EPG-UK.xml",
    "https://epghub.xyz/epg/EPG-US.xml",
    "https://epghub.xyz/epg/EPG-US-LOCALS.xml",
    "https://epghub.xyz/epg/EPG-US-SPORTS.xml",
    "https://epghub.xyz/epg/EPG-VOA.xml",
]

KEEP_PAST_DAYS = 2
KEEP_FUTURE_DAYS = 7


def parse_xmltv_time(ts: str):
    if not ts:
        return None
    m = re.match(r"(\d{14})(?:\s*([+\-]\d{4}|Z))?", ts)
    if not m:
        return None

    base = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
    tz = m.group(2)

    if tz and tz != "Z":
        sign = 1 if tz[0] == "+" else -1
        hours = int(tz[1:3])
        mins = int(tz[3:5])
        offset = timezone(sign * timedelta(hours=hours, minutes=mins))
        dt = base.replace(tzinfo=offset).astimezone(timezone.utc)
    else:
        dt = base.replace(tzinfo=timezone.utc)

    return dt


def intersects_window(start_dt, stop_dt, win_start, win_end):
    if not start_dt and not stop_dt:
        return True
    if not start_dt:
        return stop_dt >= win_start
    if not stop_dt:
        return start_dt <= win_end
    return (start_dt <= win_end) and (stop_dt >= win_start)


def fetch_xml(url):
    print(f"Fetching {url} ...")
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    return etree.parse(BytesIO(r.content))


def main():
    now = datetime.now(timezone.utc)
    win_start = now - timedelta(days=KEEP_PAST_DAYS)
    win_end = now + timedelta(days=KEEP_FUTURE_DAYS)

    tv_root = etree.Element("tv")
    channel_ids_seen = set()
    programme_keys_seen = set()
    programme_count = 0

    for url in URLS:
        try:
            doc = fetch_xml(url)
        except Exception as e:
            print(f"⚠️ Failed to fetch {url}: {e}", file=sys.stderr)
            continue

        root = doc.getroot()

        for ch in root.findall("channel"):
            cid = ch.get("id") or ""
            if cid not in channel_ids_seen:
                channel_ids_seen.add(cid)
                tv_root.append(ch)

        for pr in root.findall("programme"):
            ch_id = pr.get("channel") or ""
            start_s = pr.get("start") or ""
            stop_s = pr.get("stop") or ""

            start_dt = parse_xmltv_time(start_s)
            stop_dt = parse_xmltv_time(stop_s)

            if not intersects_window(start_dt, stop_dt, win_start, win_end):
                continue

            title_text = (pr.findtext("title") or "").strip()
            key = (ch_id, start_s, stop_s, title_text)

            if key in programme_keys_seen:
                continue

            programme_keys_seen.add(key)
            tv_root.append(pr)
            programme_count += 1

    if programme_count == 0:
        print("❌ ERROR: No programmes found. Stopping to protect previous version.")
        sys.exit(1)

    tree = etree.ElementTree(tv_root)
    output_name = "merged_epg.xml"
    tree.write(output_name, encoding="utf-8", xml_declaration=True)

    print(f"✅ Success. Programmes kept: {programme_count}")


if __name__ == "__main__":
    main()
