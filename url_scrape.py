import argparse
import asyncio
import csv
import math
from pathlib import Path

from playwright.async_api import async_playwright

BASE_URL = "https://www.barfoot.co.nz"
DEFAULT_START_URL = f"{BASE_URL}/properties/rental/search-only=available-now"
DEFAULT_OUTPUT_FILE = "barfoot_rental_urls_available_now_new.csv"
PAGE_SIZE = 48

# PRESET_SEARCHES now includes a flag:
# (start_url, output_filename, new_only)
# For residential/commercial/rural we only collect cards with a "NEW LISTING" tag.
PRESET_SEARCHES = {
    "available-now": (
        f"{BASE_URL}/properties/rental/search-only=available-now",
        "barfoot_rental_urls_available_now_new.csv",
        False,
    ),
    "pets-allowed": (
        f"{BASE_URL}/properties/rental/search-only=pets-allowed",
        "barfoot_rental_urls_pet_allowed_new.csv",
        False,
    ),
    "furnished": (
        f"{BASE_URL}/properties/rental/furnished=yes",
        "barfoot_rental_urls_furnished_new.csv",
        False,
    ),
    "rental": (
        f"{BASE_URL}/properties/rental",
        "barfoot_rental_urls_new.csv",
        False,
    ),
    # These three can have very large result sets; we only take
    # listings that show the "NEW LISTING" badge on the card.
    "commercial": (
        f"{BASE_URL}/properties/commercial",
        "barfoot_commercial_urls_new.csv",
        True,
    ),
    "rural": (
        f"{BASE_URL}/properties/rural",
        "barfoot_rural_urls_new.csv",
        True,
    ),
    "residential": (
        f"{BASE_URL}/properties/residential",
        "barfoot_residential_urls_new.csv",
        True,
    ),
}


async def scrape_urls(start_url: str, output_file: Path, new_only: bool = False):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        all_urls = set()

        print(f"Loading page 1: {start_url}")
        await page.goto(start_url, timeout=60_000)
        await page.wait_for_timeout(5_000)

        total_text = await page.locator('[data-total-listings-attr]').get_attribute(
            "data-total-listings-attr"
        )
        total = int(total_text) if total_text else 274
        total_pages = math.ceil(total / PAGE_SIZE)
        print(f"Total listings: {total}, Pages: {total_pages}")

        for pg in range(1, total_pages + 1):
            if pg > 1:
                url = f"{start_url}/page={pg}"
                print(f"Loading page {pg}: {url}")
                await page.goto(url, timeout=60_000)
                await page.wait_for_timeout(4_000)

            page_urls = set()

            if new_only:
                # Find cards that contain a "NEW LISTING" badge and extract the
                # first property link from each card.
                badges = page.locator("text=NEW LISTING")
                badge_count = await badges.count()
                for i in range(badge_count):
                    badge = badges.nth(i)
                    try:
                        card = badge.locator(
                            "xpath=ancestor::*[.//a[contains(@href,'/property/')]][1]"
                        )
                        if await card.count() == 0:
                            continue
                        link = card.locator("a[href*='/property/']").first
                        href = await link.get_attribute("href")
                        if href:
                            full = href if href.startswith("http") else BASE_URL + href
                            page_urls.add(full)
                    except Exception:
                        continue
            else:
                links = await page.locator('a[href*="/property/"]').all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href:
                        full = href if href.startswith("http") else BASE_URL + href
                        page_urls.add(full)

            print(f"  Page {pg}: {len(page_urls)} unique URLs")
            all_urls.update(page_urls)

        await browser.close()

    sorted_urls = sorted(all_urls)
    print(f"\nTotal unique listing URLs collected: {len(sorted_urls)}")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["URL"])
        for url in sorted_urls:
            writer.writerow([url])

    print(f"Saved to {output_file}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Barfoot listing URLs from a search results page."
    )
    parser.add_argument(
        "--start-url",
        type=str,
        default=DEFAULT_START_URL,
        help="Start URL for the search results (page 1).",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default=DEFAULT_OUTPUT_FILE,
        help="Output CSV file path.",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=sorted(list(PRESET_SEARCHES.keys()) + ["all"]),
        help=(
            "Preset search to run. Choices: "
            + ", ".join(sorted(PRESET_SEARCHES.keys()))
            + ", or 'all' to run every preset."
        ),
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    if args.mode:
        if args.mode == "all":
            items = PRESET_SEARCHES.items()
        else:
            items = [(args.mode, PRESET_SEARCHES[args.mode])]

        for key, (start_url, filename, new_only) in items:
            print(f"\n=== Running preset '{key}' (new_only={new_only}) ===")
            await scrape_urls(start_url, Path("urls") / filename, new_only=new_only)
    else:
        await scrape_urls(args.start_url, Path(args.output_file))


if __name__ == "__main__":
    asyncio.run(main())

