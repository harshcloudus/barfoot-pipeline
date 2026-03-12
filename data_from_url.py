import asyncio
import argparse
import csv
import re
from datetime import date
from pathlib import Path

from playwright.async_api import async_playwright

INPUT_FILE = "test.csv"
OUTPUT_FILE = "barfoot_rural_data.csv"
CONCURRENCY = 1  # how many listings to scrape in parallel

# Predefined jobs: input URL CSV -> output data CSV
ALL_JOBS = [
    ("urls/barfoot_rural_urls_new.csv", "data/barfoot_rural_data.csv"),
    ("urls/barfoot_residential_urls_new.csv", "data/barfoot_residential_data.csv"),
    ("urls/barfoot_rental_urls_pet_allowed_new.csv", "data/barfoot_rental_pet_allowed_data.csv"),
    ("urls/barfoot_rental_urls_new.csv", "data/barfoot_rental_data.csv"),
    ("urls/barfoot_rental_urls_furnished_new.csv", "data/barfoot_rental_furnished_data.csv"),
    ("urls/barfoot_rental_urls_available_now_new.csv", "data/barfoot_rental_available_now_data.csv"),
    ("urls/barfoot_commercial_urls_new.csv", "data/barfoot_commercial_data.csv"),
]


def output_path_with_date(path_str: str) -> str:
    """Return path with current date before extension, e.g. data/foo.csv -> data/foo_2025-03-11.csv."""
    p = Path(path_str)
    return str(p.parent / f"{p.stem}_{date.today().isoformat()}{p.suffix}")


# CSV columns: URL, location, sale type, description,
# parsed property details block, all agent name:number.
OUTPUT_FIELDS = [
    "URL",
    "Location",
    "Sale_Type",
    "Description",
    "Property_Details",
    "Agents",
]


async def get_text(page, selector: str, default: str = "") -> str:
    """Safely get inner text from first matching element."""
    try:
        loc = page.locator(selector).first
        if await loc.count() > 0:
            return (await loc.inner_text()).strip()
    except Exception:
        pass
    return default


def parse_property_details(raw_text: str) -> dict:
    """Parse 'Property details' block into individual key-value fields."""
    details = {}
    if not raw_text:
        return details
    for line in raw_text.split("\n"):
        line = line.strip()
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip()
        val = val.strip()
        key_lower = key.lower()
        if "property type" in key_lower:
            details["Property_Type"] = val
        elif key_lower == "rent":
            details["Rent"] = val
        elif key_lower == "bedrooms":
            details["Bedrooms"] = val
        elif key_lower == "bathrooms":
            details["Bathrooms"] = val
        elif "garag" in key_lower:
            details["Garaging"] = val
        elif "carport" in key_lower:
            details["Carport"] = val
        elif "off street" in key_lower or "off-street" in key_lower:
            details["Off_Street_Parking"] = val
        elif "furnished" in key_lower:
            details["Furnished"] = val
        elif "pets" in key_lower:
            details["Pets"] = val
        elif "property id" in key_lower:
            details["Property_ID"] = val
    return details


async def scrape_listing(browser, url: str, index: int, total: int):
    """Scrape a single listing URL using its own Playwright page."""
    print(f"[{index}/{total}] Scraping: {url}", flush=True)
    page = await browser.new_page()
    try:
        try:
            await page.goto(url, timeout=60000)
            # Wait for the main listing summary section to ensure core content is loaded,
            # then give the page a little extra time to stabilise.
            try:
                await page.wait_for_selector("section.listingdetailssummary", timeout=15000)
            except Exception:
                # Not all templates may have this exact selector; continue anyway.
                pass
            await page.wait_for_timeout(2000)
        except Exception as e:
            print(f"  ⚠ Failed to load: {e}")
            return {**dict.fromkeys(OUTPUT_FIELDS, ""), "URL": url}

        # --- Location (property title/address) ---
        location = await get_text(page, "h1")

        # --- Sale type / banner (simple heading-based selector as in your new code) ---
        sale_type = await get_text(page, "h3 span[data-attr-test=\"listing-sub-heading\"]")
        if not sale_type:
            sale_type = await get_text(page, "h3")

        # --- Description (simple description-wrapper block) ---
        description = await get_text(page, "div.description-wrapper")

        # --- Property details block (parsed into key/value pairs) ---
        raw_details = await get_text(page, "section.ListingPropertyDetails")
        details = parse_property_details(raw_details)
        property_details_str = "\n".join(f"{k}: {v}" for k, v in details.items())

        # --- Agents: all "name : phone" pairs in a single column ---
        agents = []
        try:
            tel_loc = page.locator('a[href^="tel:"]')
            tel_count = await tel_loc.count()
            seen_phones = set()

            for idx in range(tel_count):
                link = tel_loc.nth(idx)
                href = await link.get_attribute("href") or ""
                raw_phone = href.replace("tel:", "").strip()
                phone_digits = re.sub(r"[^0-9+]", "", raw_phone)
                if not phone_digits or phone_digits in seen_phones:
                    continue
                seen_phones.add(phone_digits)

                name = ""

                def _is_sale_text(t: str) -> bool:
                    l = t.lower().strip()
                    return bool(
                        "for sale" in l
                        or "by negotiation" in l
                        or "negotiation" in l
                        or "auction" in l
                        or "tender" in l
                        or "for lease" in l
                        or "deadline" in l
                        or l.startswith("call ")
                        or "$" in t
                        or "gst" in l
                        or re.match(r"^[\d\s\-+()]{6,}$", t)
                    )

                try:
                    # Prefer the dedicated contact card wrapping the phone number,
                    # e.g. div.ContactListingPerson / div.Person / div.PersonDetails.
                    parent = link.locator(
                        "xpath=ancestor::*[contains(@class,'ContactListingPerson') or contains(@class,'listing-contact-person') or contains(@class,'Person')][1]"
                    )
                    if await parent.count() == 0:
                        # Fallback: nearest section/div that contains this tel link
                        parent = link.locator(
                            "xpath=ancestor::section[.//a[starts-with(@href,'tel:')]][1]"
                        )
                    if await parent.count() == 0:
                        parent = link.locator(
                            "xpath=ancestor::div[.//a[starts-with(@href,'tel:')]][1]"
                        )

                    if await parent.count() > 0:
                        card = parent.first
                        # First, try an explicit people-link inside the card (e.g. /our-people/...)
                        people = card.locator("a[href*='/our-people/'], a[href*='people']").first
                        if await people.count() > 0:
                            candidate = (await people.inner_text()).strip()
                            if candidate and not _is_sale_text(candidate):
                                name = candidate

                        # Fallback: scan the card text for the line that looks most like a name
                        if not name:
                            parent_text = (await card.inner_text()).strip()
                            for line in parent_text.split("\n"):
                                line = line.strip()
                                if not line or _is_sale_text(line):
                                    continue
                                # Heuristic: real names usually have few words with letters
                                if re.search(r"[A-Za-z]", line) and len(line.split()) <= 4:
                                    name = line
                                    break
                except Exception:
                    pass

                if name:
                    agents.append(f"{name} : {raw_phone[:30]}")
        except Exception:
            pass

        agents_str = " | ".join(agents)

        return {
            "URL": url,
            "Location": location,
            "Sale_Type": sale_type,
            "Description": description,
            "Property_Details": property_details_str,
            "Agents": agents_str,
        }
    finally:
        await page.close()


async def scrape(input_file: str = INPUT_FILE, output_file: str = OUTPUT_FILE):
    print(f"\nLoading URLs from {input_file} ...", flush=True)
    # Read URLs from CSV (skip header row for URL column)
    urls = []
    with open(input_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("URL", "").strip()
            if url and url.startswith("http"):
                urls.append(url)

    if not urls:
        print(f"No valid URLs found in input file: {input_file}", flush=True)
        return

    print(f"Found {len(urls)} URLs in {input_file}", flush=True)

    results = []

    print(f"Starting browser for {input_file} -> {output_file}", flush=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        sem = asyncio.Semaphore(CONCURRENCY)

        async def worker(u, idx, total):
            async with sem:
                return await scrape_listing(browser, u, idx, total)

        tasks = [
            worker(url, i, len(urls))
            for i, url in enumerate(urls, 1)
        ]

        listings = await asyncio.gather(*tasks)

        # Filter out any Nones from failed scrapes that didn't return a dict
        results = [item for item in listings if item]

        await browser.close()

    # Save to CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved scraped data to {output_file} ({len(results)} rows)", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Scrape listing data from URLs in a CSV file.")
    parser.add_argument(
        "--input-file",
        "-i",
        default=INPUT_FILE,
        help=f"Path to input CSV containing a 'URL' column (default: {INPUT_FILE})",
    )
    parser.add_argument(
        "--output-file",
        "-o",
        default=OUTPUT_FILE,
        help=f"Path to output CSV to write scraped data (default: {OUTPUT_FILE})",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Scrape all predefined URL CSVs in sequence.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.all:
        for in_file, out_file in ALL_JOBS:
            out_file = output_path_with_date(out_file)
            print(f"\n=== Running job: {in_file} -> {out_file} ===", flush=True)
            out_path = Path(out_file)
            if out_path.parent and not out_path.parent.exists():
                out_path.parent.mkdir(parents=True, exist_ok=True)
            asyncio.run(scrape(input_file=in_file, output_file=out_file))
            print(f"=== Finished job: {in_file} -> {out_file} ===", flush=True)
    else:
        # Single job mode (output filename includes current date)
        out_file = output_path_with_date(args.output_file)
        out_path = Path(out_file)
        if out_path.parent and not out_path.parent.exists():
            out_path.parent.mkdir(parents=True, exist_ok=True)
        asyncio.run(scrape(input_file=args.input_file, output_file=out_file))
