import argparse
import time
from pathlib import Path

import requests

# ==========================
# CONFIG
# ==========================

API_KEY = ""
AGENT_ID = ""
DEFAULT_FILE_PATH = "data/barfoot_rural_new_data.txt"
EMBEDDING_MODEL = "e5_mistral_7b_instruct"


def run_for_file(file_path: str, kb_name: str | None = None) -> None:
    if kb_name is None:
        kb_name = Path(file_path).stem

    # ==========================
    # STEP 1 — Upload TXT file
    # ==========================

    upload_url = "https://api.elevenlabs.io/v1/convai/knowledge-base"

    headers = {
        "xi-api-key": API_KEY
    }

    print(f"Uploading knowledge file: {file_path}")

    with open(file_path, "rb") as f:
        files = {"file": (Path(file_path).name, f, "text/plain")}
        response = requests.post(upload_url, headers=headers, files=files)

    if response.status_code != 200:
        print("Upload failed:", response.text)
        return

    doc_id = response.json()["id"]

    print("Upload successful")
    print("Document ID:", doc_id)

    # ==========================
    # STEP 2 — Trigger RAG indexing for this document
    # ==========================

    rag_url = "https://api.elevenlabs.io/v1/convai/knowledge-base/rag-index"
    rag_headers = {
        "xi-api-key": API_KEY,
        "Content-Type": "application/json",
    }
    rag_body = {
        "items": [
            {
                "document_id": doc_id,
                "create_if_missing": True,
                "model": EMBEDDING_MODEL,
            }
        ]
    }

    print("Triggering RAG index creation...")
    rag_resp = requests.post(rag_url, headers=rag_headers, json=rag_body)
    if rag_resp.status_code != 200:
        print("RAG index request failed:", rag_resp.text)
    else:
        print("RAG index request accepted:", rag_resp.json())

    # Give ElevenLabs some time to build the index before we try to
    # attach this document to the agent (helps with larger files).
    print("Waiting 15 seconds for RAG index to start building...")
    time.sleep(15)

    # ==========================
    # STEP 3 — Attach to agent (append, don't replace)
    # ==========================

    agent_url = f"https://api.elevenlabs.io/v1/convai/agents/{AGENT_ID}"

    common_headers = {
        "xi-api-key": API_KEY,
    }

    print("Fetching current agent configuration...")
    get_resp = requests.get(agent_url, headers=common_headers)
    if get_resp.status_code != 200:
        print("Failed to fetch agent:", get_resp.text)
        return

    agent_json = get_resp.json()
    kb_list = (
        agent_json.get("conversation_config", {})
        .get("agent", {})
        .get("prompt", {})
        .get("knowledge_base", [])
    )

    new_entry = {
        "type": "file",
        "id": doc_id,
        "name": kb_name,
    }

    # Only append if this doc_id is not already present
    if not any(item.get("type") == "file" and item.get("id") == doc_id for item in kb_list):
        kb_list.append(new_entry)
    else:
        print("Document already present in agent knowledge base; not adding duplicate.")

    patch_headers = {
        "xi-api-key": API_KEY,
        "Content-Type": "application/json",
    }

    data = {
        "conversation_config": {
            "agent": {
                "prompt": {
                    "knowledge_base": kb_list,
                }
            }
        }
    }

    # Try to update the agent knowledge base, but wait for the RAG index if needed.
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        print(f"Updating agent knowledge base (attempt {attempt}/{max_attempts})...")
        response = requests.patch(agent_url, headers=patch_headers, json=data)

        if response.status_code == 200:
            print("Agent knowledge base updated successfully (existing entries preserved)")
            break

        text = response.text
        if "rag_index_not_ready" in text:
            if attempt == max_attempts:
                print("Agent update failed: RAG index still not ready after retries.")
                return
            print("RAG index not ready yet; waiting 10 seconds before retrying...")
            time.sleep(10)
            continue

        print("Agent update failed:", text)
        return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload a TXT file to ElevenLabs RAG and attach it to an agent.")
    parser.add_argument(
        "--file",
        type=str,
        default=DEFAULT_FILE_PATH,
        help=f"Path to TXT file to upload (default: {DEFAULT_FILE_PATH})",
    )
    parser.add_argument(
        "--kb-name",
        type=str,
        default=None,
        help="Optional knowledge base name; defaults to file stem.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_for_file(args.file, args.kb_name)