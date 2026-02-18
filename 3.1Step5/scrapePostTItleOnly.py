import requests
import csv
import time
from datetime import datetime
import openai
import os
import json
from dotenv import load_dotenv

# --- Configuration ---
subreddits = [
    "recruiting",
    "recruitment",
    "humanresources",
    "recruitinghell",
    "technology",
    "futurology",
    "recruitmentagencies"

]

keywords = [
    "AI recruiting",
    "AI hiring",
    "AI resume screening",
    "AI interview",
    "ATS AI",
    "recruitment automation",
    "automated candidate screening",
    "interview bot",
    "AI talent acquisition",
    "candidate ranking AI",
]

MIN_COMMENTS = 25
POSTS_PER_QUERY = 100
BATCH_SIZE = 10
REQUEST_DELAY = 2.0
TARGET_COMMENTS = 30000   # total comments
TARGET_POSTS = 300        # total AI-approved posts


MIN_COMMENTS = 25
POSTS_PER_QUERY = 100
BATCH_SIZE = 10
REQUEST_DELAY = 2.0
TARGET_COMMENTS = 30000   # total comments
TARGET_POSTS = 300        # total AI-approved posts
OUTPUT_CSV = "stage1_posts_for_manual_review.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Stage1PostCollector/1.0; contact=edu)"
}

# --- Load OpenAI API key ---
load_dotenv("openai_api_key.env")
openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    raise ValueError("OpenAI API key not found in 'openai_api_key.env'")

# --- Persistence files ---
CHECKED_FILE = "checked_post_ids.json"
INCLUDED_FILE = "included_post_ids.json"

checked_post_ids = set()
included_post_ids = set()
total_comments_collected = 0

# Load previous progress if exists
if os.path.exists(CHECKED_FILE):
    with open(CHECKED_FILE, "r") as f:
        checked_post_ids = set(json.load(f))

if os.path.exists(INCLUDED_FILE):
    with open(INCLUDED_FILE, "r") as f:
        included_post_ids = set(json.load(f))

# --- Helpers ---
def fetch_posts(subreddit, keyword, limit=100):
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    params = {
        "q": keyword,
        "restrict_sr": 1,
        "sort": "comments",
        "limit": limit,
    }
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if r.status_code != 200:
            print(f"Failed ({subreddit}, '{keyword}') -> {r.status_code}")
            return []
        return r.json().get("data", {}).get("children", [])
    except Exception as e:
        print(f"Error ({subreddit}, '{keyword}'): {e}")
        return []

def check_batch_relevance(posts_batch):
    """Check relevance of multiple posts in one OpenAI call"""
    prompt = (
        "For each Reddit post below, answer only 'yes' if it is directly related to "
        "AI used in hiring, recruitment, resume screening, or interview automation, "
        "otherwise answer 'no'. Be very strict to avoid false positives. "
        "Reply with one answer per post in order, separated by commas.\n\n"
    )
    for i, post in enumerate(posts_batch, start=1):
        prompt += f"{i}. Title: {post['title']}\n   Selftext: {post['selftext']}\n\n"

    try:
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=len(posts_batch) * 3
        )
        answers = response.choices[0].message.content.strip().lower()
        answers_list = [ans.strip() for ans in answers.replace("\n", ",").split(",") if ans.strip()]
        return answers_list
    except Exception as e:
        print(f"OpenAI API error: {e}")
        return ["no"] * len(posts_batch)

# --- Main ---
rows = []

# Load existing CSV if exists
if os.path.exists(OUTPUT_CSV):
    with open(OUTPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
            # Sum up total comments from included posts
            total_comments_collected += int(r["num_comments"])

for kw in keywords:
    if len(included_post_ids) >= TARGET_POSTS and total_comments_collected >= TARGET_COMMENTS:
        break
    print(f"\n=== Keyword: '{kw}' ===")
    for sub in subreddits:
        if len(included_post_ids) >= TARGET_POSTS and total_comments_collected >= TARGET_COMMENTS:
            break
        print(f"Searching r/{sub}...")
        posts = fetch_posts(sub, kw, POSTS_PER_QUERY)
        time.sleep(REQUEST_DELAY)

        # filter out already checked or low-comment posts
        new_posts = [
            post["data"] for post in posts
            if post["data"]["id"] not in checked_post_ids and post["data"].get("num_comments",0) >= MIN_COMMENTS
        ]

        if not new_posts:
            continue

        # Process in batches
        for i in range(0, len(new_posts), BATCH_SIZE):
            batch = new_posts[i:i+BATCH_SIZE]
            batch_posts = [{"title": p.get("title",""), "selftext": p.get("selftext",""), **p} for p in batch]
            relevance_answers = check_batch_relevance(batch_posts)
            time.sleep(REQUEST_DELAY)

            for post_data, relevant in zip(batch_posts, relevance_answers):
                post_id = post_data["id"]
                checked_post_ids.add(post_id)  # mark as checked

                if relevant != "yes" or post_id in included_post_ids:
                    continue

                included_post_ids.add(post_id)
                num_comments = post_data.get("num_comments",0)
                total_comments_collected += num_comments

                row = {
                    "keyword": kw,
                    "subreddit": sub,
                    "post_id": post_id,
                    "title": post_data.get("title", ""),
                    "selftext": post_data.get("selftext", ""),
                    "num_comments": num_comments,
                    "url": f"https://www.reddit.com{post_data['permalink']}",
                    "created_utc": datetime.utcfromtimestamp(
                        post_data["created_utc"]
                    ).isoformat() + "Z",
                }
                rows.append(row)
                print(f"Included post {post_id} ({num_comments} comments). Total comments: {total_comments_collected}, Total posts: {len(included_post_ids)}")

                if len(included_post_ids) >= TARGET_POSTS and total_comments_collected >= TARGET_COMMENTS:
                    print("Reached target posts and comments!")
                    break

# --- Save progress ---
with open(CHECKED_FILE, "w") as f:
    json.dump(list(checked_post_ids), f)

with open(INCLUDED_FILE, "w") as f:
    json.dump(list(included_post_ids), f)

# --- Write CSV ---
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "keyword",
            "subreddit",
            "post_id",
            "title",
            "selftext",
            "num_comments",
            "url",
            "created_utc",
        ],
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"\nSaved {len(included_post_ids)} posts, {total_comments_collected} comments to {OUTPUT_CSV}")
