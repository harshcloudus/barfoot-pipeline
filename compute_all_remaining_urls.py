import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Set


@dataclass
class UrlSetConfig:
    name: str
    existing_file: Path
    new_file: Path


BASE_DIR = Path("urls")

# Configure every pair here: main (existing) CSV and its corresponding *_new CSV.
# Only the seven main URL files you listed are used.
CONFIGS = [
    # Rural
    UrlSetConfig(
        name="rural",
        existing_file=BASE_DIR / "barfoot_rural_urls.csv",
        new_file=BASE_DIR / "barfoot_rural_urls_new.csv",
    ),
    # Residential
    UrlSetConfig(
        name="residential",
        existing_file=BASE_DIR / "barfoot_residential_urls.csv",
        new_file=BASE_DIR / "barfoot_residential_urls_new.csv",
    ),
    # Commercial
    UrlSetConfig(
        name="commercial",
        existing_file=BASE_DIR / "barfoot_commercial_urls.csv",
        new_file=BASE_DIR / "barfoot_commercial_urls_new.csv",
    ),
    # Rentals
    UrlSetConfig(
        name="rental_all",
        existing_file=BASE_DIR / "barfoot_rental_urls.csv",
        new_file=BASE_DIR / "barfoot_rental_urls_new.csv",
    ),
    UrlSetConfig(
        name="rental_furnished",
        existing_file=BASE_DIR / "barfoot_rental_urls_furnished.csv",
        new_file=BASE_DIR / "barfoot_rental_urls_furnished_new.csv",
    ),
    UrlSetConfig(
        name="rental_pets_allowed",
        existing_file=BASE_DIR / "barfoot_rental_urls_pet_allowed.csv",
        new_file=BASE_DIR / "barfoot_rental_urls_pet_allowed_new.csv",
    ),
    UrlSetConfig(
        name="rental_available_now",
        existing_file=BASE_DIR / "barfoot_rental_urls_available_now.csv",
        new_file=BASE_DIR / "barfoot_rental_urls_available_now_new.csv",
    ),
]


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
    print("Computing remaining URLs for all configured datasets...")
    for cfg in CONFIGS:
        existing = load_url_set(cfg.existing_file)
        new = load_url_set(cfg.new_file)

        if not new:
            print(f"- {cfg.name}: SKIP (no *_new file or it is empty at {cfg.new_file})")
            continue

        remaining = new - existing

        # Overwrite the *_new file with only remaining URLs.
        write_url_set(cfg.new_file, remaining)

        print(
            f"- {cfg.name}: existing={len(existing)}, scraped(new)={len(new)}, "
            f"remaining={len(remaining)} -> updated {cfg.new_file.name}"
        )


if __name__ == "__main__":
    main()

