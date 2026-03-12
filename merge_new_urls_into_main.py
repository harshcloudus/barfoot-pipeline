import csv
from pathlib import Path
from typing import Set

from compute_all_remaining_urls import CONFIGS, UrlSetConfig  # reuse same pairs


def load_url_set(path: Path) -> Set[str]:
    urls: Set[str] = set()
    if not path.exists():
        return urls
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("URL") or "").strip()
            if url:
                urls.add(url)
    return urls


def write_url_set(path: Path, urls: Set[str]) -> None:
    sorted_urls = sorted(urls)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["URL"])
        for url in sorted_urls:
            writer.writerow([url])


def main() -> None:
    print("Merging *_new URL CSVs back into their main CSVs...")

    for cfg in CONFIGS:
        assert isinstance(cfg, UrlSetConfig)

        existing = load_url_set(cfg.existing_file)
        new = load_url_set(cfg.new_file)

        if not new:
            print(f"- {cfg.name}: SKIP (no *_new URLs at {cfg.new_file})")
            continue

        merged = existing | new
        added_count = len(merged) - len(existing)

        write_url_set(cfg.existing_file, merged)
        # Optionally clear the *_new file now that everything is merged.
        write_url_set(cfg.new_file, set())

        print(
            f"- {cfg.name}: existing(before)={len(existing)}, new={len(new)}, "
            f"added={added_count}, final(main)={len(merged)}; cleared {cfg.new_file.name}"
        )


if __name__ == "__main__":
    main()

