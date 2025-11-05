scripts/merge_epg.py
import sys
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

def fetch_xml(url):
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    content = r.content
    # Handle .gz transparently if needed
    if content[:2] == b"\x1f\x8b":
        import gzip
        content = gzip.decompress(content)
    return etree.parse(BytesIO(content))

def main():
    # Base XMLTV root
    tv_root = etree.Element("tv")
    channel_ids_seen = set()
    # For programmes, dedupe by (channel,start,stop,title)
    programme_keys_seen = set()

    for url in URLS:
        try:
            doc = fetch_xml(url)
        except Exception as e:
            print(f"WARNING: failed to fetch {url}: {e}", file=sys.stderr)
            continue

        root = doc.getroot()
        # copy channels
        for ch in root.findall("channel"):
            cid = ch.get("id") or ""
            if cid not in channel_ids_seen:
                channel_ids_seen.add(cid)
                tv_root.append(ch)

        # copy programmes (dedupe basic key)
        for pr in root.findall("programme"):
            key = (
                pr.get("channel") or "",
                pr.get("start") or "",
                pr.get("stop") or "",
                (pr.findtext("title") or "").strip(),
            )
            if key not in programme_keys_seen:
                programme_keys_seen.add(key)
                tv_root.append(pr)

    # Optional: sort channels by id, programmes by start
    channels = [e for e in tv_root.findall("channel")]
    programmes = [e for e in tv_root.findall("programme")]
    for e in channels + programmes:
        tv_root.remove(e)

    channels.sort(key=lambda c: (c.get("id") or ""))
    programmes.sort(key=lambda p: (p.get("start") or ""))

    for e in channels + programmes:
        tv_root.append(e)

    tree = etree.ElementTree(tv_root)
    tree.write(
        "merged_epg.xml",
        encoding="utf-8",
        xml_declaration=True,
        pretty_print=False
    )

if __name__ == "__main__":
    main()
