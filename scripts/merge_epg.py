import sys, re, gzip, time, shutil, os
from datetime import datetime, timedelta, timezone
import requests
from lxml import etree
from io import BytesIO

"""
Merged EPG - Full Sources
Time window:
  - 1 day past
  - 1 day future

IMPORTANT SAFETY:
- If ALL sources fail (epghub blocks GitHub Actions sometimes),
  we reuse the previous dist/epg.xml.gz instead of producing <tv/>.
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
    "https://epghub.xyz/epg/EPG-PEACOCK.xml",   # ✅ keep XML
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

KEEP_PAST_DAYS = 1
KEEP_FUTURE_DAYS = 1

# Helps avoid epghub blocking GitHub Actions
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; merged-epg-bot/1.0; +https://github.com/Junior2237/merged-epg)",
    "Accept": "*/*",
}


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


def fetch_xml(url: str, retries: int = 3):
    """Download and parse XML or .gz EPG files with retries."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            print(f"Fetching {url} (attempt {attempt}/{retries}) ...")
            r = requests.get(url, timeout=180, headers=HEADERS)
            r.raise_for_status()
            content = r.content

            # If content is actually gz bytes, decompress by magic header
            if content[:2] == b"\x1f\x8b":
                content = gzip.decompress(content)

            return etree.parse(BytesIO(content))
        except Exception as e:
            last_err = e
            # small backoff
            time.sleep(2 * attempt)

    raise last_err


def reuse_previous_if_available(output_name: str):
    """If merge failed completely, copy previous dist/epg.xml.gz to output."""
    prev_path = os.path.join("dist", "epg.xml.gz")
    if os.path.exists(prev_path):
        print("⚠️ All sources failed. Reusing previous dist/epg.xml.gz to avoid breaking output.")
        shutil.copyfile(prev_path, output_name)
        return True

    print("❌ All sources failed AND no previous dist/epg.xml.gz exists. Cannot recover.")
    return False


def main():
    now = datetime.now(timezone.utc)
    win_start = now - timedelta(days=KEEP_PAST_DAYS)
    win_end = now + timedelta(days=KEEP_FUTURE_DAYS)

    tv_root = etree.Element("tv")
    channel_ids_seen = set()
    programme_keys_seen = set()

    sources_ok = 0

    for url in URLS:
        try:
            doc = fetch_xml(url)
            sources_ok += 1
        except Exception as e:
            print(f"⚠️ Failed to fetch {url}: {e}", file=sys.stderr)
            continue

        root = doc.getroot()

        for ch in root.findall("channel"):
            cid = ch.get("id") or ""
            if cid and cid not in channel_ids_seen:
                channel_ids_seen.add(cid)
                tv_root.append(ch)

        for pr in root.findall("programme"):
            ch = pr.get("channel") or ""
            start_s = pr.get("start") or ""
            stop_s = pr.get("stop") or ""

            start_dt = parse_xmltv_time(start_s)
            stop_dt = parse_xmltv_time(stop_s)

            if not intersects_window(start_dt, stop_dt, win_start, win_end):
                continue

            title_text = (pr.findtext("title") or "").strip()
            key = (ch, start_s, stop_s, title_text)

            if key in programme_keys_seen:
                continue
            programme_keys_seen.add(key)
            tv_root.append(pr)

    output_name = "merged_epg.xml.gz"

    # If everything failed, reuse previous file so workflow doesn't break
    if sources_ok == 0 or (len(channel_ids_seen) == 0 and len(programme_keys_seen) == 0):
        recovered = reuse_previous_if_available(output_name)
        if recovered:
            return
        # If we cannot recover, write minimal file (will fail check, but at least logs show why)
        tree = etree.ElementTree(tv_root)
        with gzip.open(output_name, "wb") as f:
            tree.write(f, encoding="utf-8", xml_declaration=True, pretty_print=False)
        return

    # Sort elements for tidy output
    channels = [e for e in tv_root.findall("channel")]
    programmes = [e for e in tv_root.findall("programme")]

    for e in channels + programmes:
        tv_root.remove(e)

    channels.sort(key=lambda c: (c.get("id") or ""))
    programmes.sort(key=lambda p: (p.get("start") or ""))

    for e in channels + programmes:
        tv_root.append(e)

    tree = etree.ElementTree(tv_root)
    with gzip.open(output_name, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True, pretty_print=False)

    print(f"✅ Merged EPG saved successfully: {output_name}")
    print(f"✅ Sources OK: {sources_ok}/{len(URLS)} | Channels: {len(channel_ids_seen)} | Programmes: {len(programme_keys_seen)}")


if __name__ == "__main__":
    main()
