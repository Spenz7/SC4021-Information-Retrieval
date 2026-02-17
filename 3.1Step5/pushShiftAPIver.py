import requests
import json
import os
from datetime import datetime

#REV doesn't work

# Subreddits to crawl
subreddits = [
    "recruiting",
    "recruitment",
    "humanresources",
    "recruitinghell",
    "jobs",
    "careeradvice",
    "cscareerquestions"
]

# Keywords to search for
keywords = [
    "AI", "AI hiring", "AI recruiting", "AI interview",
    "resume screening", "automation", "hiring", "recruitment", "recruiting"
]

# Minimum number of comments per post (for relevance)
MIN_COMMENTS = 50

# Output folder
OUTPUT_FOLDER = "jsonl_crawl_step5"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Target counts
TARGET_COMMENTS = 10000
TARGET_WORDS = 100000

def fetch_comments(subreddit, keyword, size=100):
    """Fetch comments from Pushshift API for a subreddit + keyword"""
    url = "https://api.pushshift.io/reddit/comment/search/"
    params = {
        "q": keyword,
        "subreddit": subreddit,
        "size": size,
        "sort": "desc",
        "sort_type": "created_utc"
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"Failed to fetch comments for r/{subreddit} keyword '{keyword}'")
            return []
        data = resp.json()
        return data.get("data", [])
    except Exception as e:
        print(f"Error fetching comments for r/{subreddit} keyword '{keyword}': {e}")
        return []

def clean_comment(comment):
    """Return True if comment is valid (not deleted/removed/mod)"""
    body = comment.get("body", "")
    author = comment.get("author", "")
    if not body or body.lower() in ["[deleted]", "[removed]"]:
        return False
    if author.lower() == "automoderator":
        return False
    return True

def save_jsonl(comments, output_file):
    """Save list of comments to JSONL"""
    with open(output_file, "w", encoding="utf-8") as f:
        for c in comments:
            record = {
                "id": c.get("id"),
                "text": c.get("body"),
                "timestamp": datetime.utcfromtimestamp(c["created_utc"]).isoformat() + "Z",
                "source": "reddit",
                "metadata": {
                    "subreddit": c.get("subreddit"),
                    "keyword": c.get("keyword"),
                    "author": c.get("author")
                }
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

def crawl_step5(subreddits, keywords):
    total_comments = 0
    total_words = 0
    all_comments = []

    for sub in subreddits:
        for kw in keywords:
            fetched = 0
            batch_size = 100
            while fetched < 500:  # fetch up to 500 per keyword per subreddit
                comments = fetch_comments(sub, kw, size=batch_size)
                if not comments:
                    break
                for c in comments:
                    if not clean_comment(c):
                        continue
                    c["subreddit"] = sub
                    c["keyword"] = kw
                    all_comments.append(c)
                    total_comments += 1
                    total_words += len(c.get("body", "").split())

                fetched += len(comments)

                # Stop if targets reached
                if total_comments >= TARGET_COMMENTS and total_words >= TARGET_WORDS:
                    break
            if total_comments >= TARGET_COMMENTS and total_words >= TARGET_WORDS:
                break
        if total_comments >= TARGET_COMMENTS and total_words >= TARGET_WORDS:
            break

    print(f"Total comments collected: {total_comments}")
    print(f"Approx. total words: {total_words}")

    output_file = os.path.join(OUTPUT_FOLDER, "reddit_crawl_final.jsonl")
    save_jsonl(all_comments, output_file)
    print(f"Saved all comments to {output_file}")

if __name__ == "__main__":
    crawl_step5(subreddits, keywords)
