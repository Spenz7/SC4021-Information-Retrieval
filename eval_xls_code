import json
import pandas as pd
from glob import glob
import os

# --- Configuration ---
CORE_KEYWORDS = [
    "AI",
    "AI hiring",
    "AI recruiting",
    "resume screening",
    "AI interview",
    "automation",
    "hiring",
    "recruitment",
    "recruiting"
]

OUTPUT_FOLDER = "jsonl_crawl_full"
OUTPUT_FILE = "eval.xlsx"

# --- Helper to sanitize sheet names ---
def sanitize_sheet_name(name):
    invalid_chars = ['\\', '/', '*', '[', ']', ':', '?']
    for c in invalid_chars:
        name = name.replace(c, "_")
    return name[:31]  # Excel limit

# --- Read JSONL files ---
records_by_subreddit = {}

for file in glob(os.path.join(OUTPUT_FOLDER, "*.jsonl")):
    subreddit = os.path.basename(file).split("_")[0]  # assumes filename starts with subreddit
    with open(file, "r", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            text_lower = r["text"].lower()
            if any(kw.lower() in text_lower for kw in CORE_KEYWORDS):
                if subreddit not in records_by_subreddit:
                    records_by_subreddit[subreddit] = []
                records_by_subreddit[subreddit].append({
                    "comment": r["text"],
                    "words": len(r["text"].split()),
                    "label": ""  # placeholder for manual labeling
                })

# --- Write to Excel ---
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    for subreddit, recs in records_by_subreddit.items():
        if not recs:
            continue
        df = pd.DataFrame(recs)
        sheet_name = sanitize_sheet_name(subreddit)
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"Saved filtered comments to {OUTPUT_FILE} with {len(records_by_subreddit)} sheets.")
