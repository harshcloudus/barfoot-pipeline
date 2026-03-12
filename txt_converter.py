import argparse
import csv
import re
from pathlib import Path


def normalize_description(text: str) -> str:
    if not text:
        return ""
    # Normalize newlines
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Trim trailing spaces per line
    lines = [line.rstrip() for line in text.split("\n")]
    text = "\n".join(lines)
    # Collapse 3+ blank lines into 2
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def convert_csv_to_rag_txt(
    input_path: Path,
    output_path: Path,
) -> None:
    with input_path.open(newline="", encoding="utf-8") as f_in, output_path.open(
        "w", encoding="utf-8"
    ) as f_out:
        reader = csv.DictReader(f_in)
        for idx, row in enumerate(reader, start=1):
            url = (row.get("URL") or "").strip()

            # Prefer the numeric ID at the end of the URL as the listing ID
            # for all datasets. Fall back to row index if missing.
            listing_id = None
            if url:
                m = re.search(r"(\d+)(?:/?$)", url)
                if m:
                    listing_id = m.group(1)
            if listing_id is None:
                listing_id = str(idx)
            location = (row.get("Location") or "").strip()
            sale_type = (row.get("Sale_Type") or "").strip()
            description = normalize_description(row.get("Description") or "")
            # New parsed property details block (may be empty or missing for older CSVs)
            property_details = normalize_description(row.get("Property_Details") or "")
            agents = (row.get("Agents") or "").strip()

            f_out.write(f"Listing ID: {listing_id}\n")
            f_out.write(f"URL: {url}\n")
            f_out.write(f"Location: {location}\n")
            f_out.write(f"Sale type: {sale_type}\n")
            f_out.write("Description:\n")
            if description:
                f_out.write(description + "\n")
            else:
                f_out.write("(No description)\n")
            # Only write property details section if we have anything non-empty
            if property_details:
                f_out.write("Property details:\n")
                f_out.write(property_details + "\n")
            f_out.write(f"Agents: {agents}\n")
            f_out.write("\n---\n\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert a Barfoot-style rural listings CSV "
            "into a structured TXT file for RAG."
        )
    )
    parser.add_argument(
        "input_csv",
        type=str,
        help="Path to the input CSV file",
    )
    parser.add_argument(
        "output_txt",
        type=str,
        help="Path to the output TXT file",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    input_csv = Path(args.input_csv)
    output_txt = Path(args.output_txt)
    convert_csv_to_rag_txt(input_csv, output_txt)
