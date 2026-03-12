import time
from pathlib import Path

from upload_to_rag import run_for_file


JOBS = [
    ("data/barfoot_rural_data_2026-03-12.txt", "barfoot_rural_2026-03-12"),
    ("data/barfoot_residential_data_2026-03-12.txt", "barfoot_residential_2026-03-12"),
    ("data/barfoot_rental_data_2026-03-12.txt", "barfoot_rental_all_2026-03-12"),
    ("data/barfoot_rental_furnished_data_2026-03-12.txt", "barfoot_rental_furnished_2026-03-12"),
    ("data/barfoot_rental_pet_allowed_data_2026-03-12.txt", "barfoot_rental_pet_allowed_2026-03-12"),
    ("data/barfoot_rental_available_now_data_2026-03-12.txt", "barfoot_rental_available_now_2026-03-12"),
    ("data/barfoot_commercial_data_2026-03-12.txt", "barfoot_commercial_2026-03-12"),
]


def main() -> None:
    for path_str, kb_name in JOBS:
        path = Path(path_str)
        if not path.exists():
            print(f"Skipping {path_str} (file not found)")
            continue
        print(f"\n=== Uploading {path_str} as KB '{kb_name}' ===")
        run_for_file(str(path), kb_name)
        # Small pause between jobs so ElevenLabs has breathing room
        print("Waiting 5 seconds before next upload...\n")
        time.sleep(5)


if __name__ == "__main__":
    main()

