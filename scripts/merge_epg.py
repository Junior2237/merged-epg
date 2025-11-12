import sys, re, gzip
from datetime import datetime, timedelta, timezone
import requests
from lxml import etree
from io import BytesIO

"""
Merge multiple XMLTV/EPG URLs into one gzipped EPG, keeping only a small
time window so the file stays fast and under CDN limits.

- Past window: KEEP_PAST_DAYS (days before now)
- Future window: KEEP_FUTURE_DAYS (days after now)

Currently configured for:
  - 1 day past
  - 1 day future
"""

# ✅ All your EPG sources combined (including PEACOCK)
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
    "https://epghub.xyz/epg/EPG-IT.xml",
    "https://epghub.xyz/epg/EPG-LOCOMOTIONTV.xml",
    "https://epghub.xyz/epg/EPG-PAC.xml",
    "https://epghub.xyz/epg/EPG-PEACOCK.xml.gz",
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

# 🔧 Time window to keep (to shrink file size)
KEEP_PAST_DAYS = 1      # how many days BEFORE now to keep
KEEP_FUTURE_DAYS = 1    # how many days AFTER now to keep


def parse_xmltv_time(ts: str):
    """Parse XMLTV timestamps (e.g. '20251105120000 +0000') → datetime in UTC."""
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
    """
    Return True if a programme [start_dt, stop_dt] overlaps the window
    [win_start, win_end].
    """
    # If no timestamps, keep to be safe
    if not start_dt and not stop_dt:
        return True
    # If only stop time, make sure it ends after window start
    if not start_dt:
        return stop_dt >= win_start
    # If only start time, make sure it starts before window end
    if not stop_dt:
        return start_dt <= win_end
    # Normal case: check interval overlap
    return (start_dt <= win_end) and (stop_dt >= win_start)


def fetch_xml(url):
    """Download and parse XML or .gz EPG files into an lxml ElementTree."""
    print(f"Fetching {url} ...")
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    content = r.content

    # Handle gzip-compressed responses and .xml.gz files
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)

    return etree.parse(BytesIO(content))


def main():
    now = datetime.now(timezone.utc)
    win_start = now - timedelta(days=KEEP_PAST_DAYS)
    win_end = now + timedelta(days=KEEP_FUTURE_DAYS)

    tv_root = etree.Element("tv")
    channel_ids_seen = set()
    programme_keys_seen = set()

    for url in URLS:
        try:
            doc = fetch_xml(url)
        except Exception as e:
            print(f"⚠️ Failed to fetch {url}: {e}", file=sys.stderr)
            continue

        root = doc.getroot()

        # Merge channels (de-duplicate by id)
        for ch in root.findall("channel"):
            cid = ch.get("id") or ""
            if cid not in channel_ids_seen:
                channel_ids_seen.add(cid)
                tv_root.append(ch)

        # Merge programmes with time-window + de-duplication
        for pr in root.findall("programme"):
            ch = pr.get("channel") or ""
            start_s = pr.get("start") or ""
            stop_s = pr.get("stop") or ""

            start_dt = parse_xmltv_time(start_s)
            stop_dt = parse_xmltv_time(stop_s)

            # Drop programmes entirely outside our [win_start, win_end] window
            if not intersects_window(start_dt, stop_dt, win_start, win_end):
                continue

            title_text = (pr.findtext("title") or "").strip()
            key = (ch, start_s, stop_s, title_text)

            # De-duplicate identical programmes
            if key in programme_keys_seen:
                continue
            programme_keys_seen.add(key)
            tv_root.append(pr)

    # Sort channels and programmes for tidy output
    channels = [e for e in tv_root.findall("channel")]
    programmes = [e for e in tv_root.findall("programme")]

    for e in channels + programmes:
        tv_root.remove(e)

    channels.sort(key=lambda c: (c.get("id") or ""))
    programmes.sort(key=lambda p: (p.get("start") or ""))

    for e in channels + programmes:
        tv_root.append(e)

    # Save gzipped output with the name used in your repo/jsDelivr
    tree = etree.ElementTree(tv_root)
    output_name = "epg.xml.gz"  # this is what jsDelivr is serving
    with gzip.open(output_name, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True, pretty_print=False)

    print(f"✅ Merged EPG saved as {output_name}")


if __name__ == "__main__":
    main()
