import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

from upload_to_rag import run_for_file


PYTHON = sys.executable or "python"


def run_step(label: str, args: list[str]) -> None:
    print(f"\n=== STEP: {label} ===")
    print("Running:", " ".join(args))
    result = subprocess.run(args, check=False)
    if result.returncode != 0:
        raise SystemExit(f"Step '{label}' failed with exit code {result.returncode}")


def main() -> None:
    today = date.today().isoformat()
    # Unique ID for this pipeline run within the day, e.g. 2026-03-12_15-47-03
    run_suffix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    date_only = today  # kept for compatibility with data_from_url naming
    data_dir = Path("data")

    # 1) Scrape URLs for all presets (rentals = all listings, others = NEW LISTING only).
    run_step("scrape_urls_all_presets", [PYTHON, "url_scrape.py", "--mode", "all"])

    # 2) Reduce *_new URL CSVs down to only URLs not already in the main CSVs.
    run_step("compute_remaining_urls", [PYTHON, "compute_all_remaining_urls.py"])

    # 3) Scrape listing data for all *_new URL CSVs into dated CSV files.
    run_step("scrape_listing_data", [PYTHON, "data_from_url.py", "--all"])

    # 4) Convert today's data CSVs to TXT for RAG.
    base_names = [
        ("barfoot_rural_data", "barfoot_rural"),
        ("barfoot_residential_data", "barfoot_residential"),
        ("barfoot_rental_pet_allowed_data", "barfoot_rental_pet_allowed"),
        ("barfoot_rental_data", "barfoot_rental_all"),
        ("barfoot_rental_furnished_data", "barfoot_rental_furnished"),
        ("barfoot_rental_available_now_data", "barfoot_rental_available_now"),
        ("barfoot_commercial_data", "barfoot_commercial"),
    ]

    txt_files: list[tuple[Path, str]] = []

    for base, kb_prefix in base_names:
        # data_from_url.py names CSVs using only the date.
        csv_path = data_dir / f"{base}_{date_only}.csv"
        # TXT and KB names include time as well so multiple runs in one day don't clash.
        txt_path = data_dir / f"{base}_{run_suffix}.txt"
        if not csv_path.exists():
            print(f"Skipping TXT conversion for {csv_path} (file not found)")
            continue
        run_step(
            f"txt_convert_{base}",
            [PYTHON, "txt_converter.py", str(csv_path), str(txt_path)],
        )
        txt_files.append((txt_path, f"{kb_prefix}_{run_suffix}"))

    if not txt_files:
        print("No TXT files were generated; stopping before RAG upload.")
        return

    # 5) Upload each TXT to ElevenLabs RAG and attach to the agent.
    for txt_path, kb_name in txt_files:
        if not txt_path.exists():
            print(f"Skipping RAG upload for {txt_path} (file not found)")
            continue
        print(f"\n=== STEP: upload_to_rag ({txt_path.name}) as KB '{kb_name}' ===")
        run_for_file(str(txt_path), kb_name)

    # 6) Merge *_new URL CSVs back into the main URL CSVs and clear *_new,
    #    so that next day's run only treats actually new listings as new.
    run_step("merge_new_urls_into_main", [PYTHON, "merge_new_urls_into_main.py"])


if __name__ == "__main__":
    main()

