import asyncio
from playwright.async_api import async_playwright

START_URL = "https://www.barfoot.co.nz/properties/rural"


async def discover():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(START_URL, timeout=60_000)
        await page.wait_for_timeout(8_000)

        # Look for "274" or total count in the page
        html = await page.content()
        idx = html.find("274")
        while idx >= 0:
            print(f"Found '274' at pos {idx}: ...{html[max(0,idx-200):idx+200]}...")
            print()
            idx = html.find("274", idx + 1)

        # Check how many listing cards there are
        print("--- Listing card structure ---")
        cards = page.locator('[class*="listing"], [class*="Listing"], [class*="property"], [class*="Property"], [class*="card"], [class*="Card"]')
        count = await cards.count()
        print(f"Found {count} card-like elements")
        for i in range(min(count, 5)):
            el = cards.nth(i)
            cls = await el.get_attribute("class") or ""
            tag = await el.evaluate("el => el.tagName")
            print(f"  [{i}] <{tag}> class='{cls[:120]}'")

        # Check for any "show all" or filter links
        print("\n--- Links with 'all' or 'more' in text ---")
        all_links = await page.locator("a").all()
        for link in all_links:
            try:
                text = (await link.inner_text()).strip().lower()
                href = (await link.get_attribute("href")) or ""
                if any(kw in text for kw in ["all", "more", "view", "show", "next", "page"]):
                    print(f"  text='{text[:80]}' href='{href[:120]}'")
            except Exception:
                pass

        # Check for sub-categories / tabs
        print("\n--- Filter/category elements ---")
        filters = page.locator('[class*="filter"], [class*="Filter"], [class*="tab"], [class*="Tab"], [class*="category"], [class*="Category"]')
        fcount = await filters.count()
        print(f"Found {fcount} filter-like elements")
        for i in range(min(fcount, 15)):
            el = filters.nth(i)
            try:
                text = (await el.inner_text()).strip()
                cls = await el.get_attribute("class") or ""
                if text:
                    print(f"  [{i}] class='{cls[:100]}' text='{text[:100]}'")
            except Exception:
                pass

        # Try scrolling to see if more load
        print("\n--- Scrolling test ---")
        before = await page.locator('a[href*="/property/"]').count()
        for _ in range(5):
            await page.evaluate("window.scrollBy(0, 2000)")
            await page.wait_for_timeout(2000)
        after = await page.locator('a[href*="/property/"]').count()
        print(f"Links before scroll: {before}, after scroll: {after}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(discover())
