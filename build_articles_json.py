import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[0]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

MIN_CONTENT_LEN = 200


def load_jsonl(path: Path):
    items = []
    if not path.exists():
        return items
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                items.append(obj)
            except json.JSONDecodeError:
                continue
    return items


def normalize_article(raw, default_source=None):
    url = raw.get("url")
    title = raw.get("title", "").strip()
    content = raw.get("content", "").strip()
    source = raw.get("source", default_source)
    published_date = raw.get("published_date")
    league = raw.get("league")

    return {
        "url": url,
        "title": title,
        "content": content,
        "source": source,
        "published_date": published_date,
        "league": league,
    }


def collect_articles():
    articles = []

    bbc_dir = RAW_DIR / "bbc"
    espn_dir = RAW_DIR / "espn"
    sky_dir = RAW_DIR / "skysports"

    for path in sorted(bbc_dir.glob("*.jsonl")):
        articles.extend(load_jsonl(path))

    for path in sorted(espn_dir.glob("*.jsonl")):
        articles.extend(load_jsonl(path))

    for path in sorted(sky_dir.glob("*.jsonl")):
        articles.extend(load_jsonl(path))

    normalized = []
    for raw in articles:
        art = normalize_article(raw)
        if not art["url"] or not art["content"]:
            continue
        if len(art["content"]) < MIN_CONTENT_LEN:
            continue
        normalized.append(art)

    dedup = {}
    for art in normalized:
        url = art["url"]
        if url not in dedup:
            dedup[url] = art

    return list(dedup.values())


def save_articles(articles):
    out_path = PROCESSED_DIR / "articles.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    print(f"[Build] Saved {len(articles)} articles to {out_path}")


def main():
    articles = collect_articles()
    save_articles(articles)


if __name__ == "__main__":
    main()
