import requests
import json
import os
import time
from datetime import datetime

# --- Configuration ---
# Core subreddits – focused, high-quality data
subreddits = [
    "recruiting",
    "recruitment",
    "humanresources",
    "recruitinghell",   # adds critical/negative opinions
    #"jobs"              # broader/general, exclude if u can hit target wo it
]

# Core keywords – most relevant to your topic
keywords = [
    "AI",
    "AI hiring",
    "AI recruiting",
    "AI screening",
    "AI interview",     # expands coverage
]


MIN_COMMENTS = 25
OUTPUT_FOLDER = "jsonl_crawl_full"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Delays
REQUEST_DELAY_SEARCH = 2.0
REQUEST_DELAY_COMMENTS = 3.0
BACKOFF_DELAY = 90  # seconds

# Targets
TARGET_COMMENTS = 20000
TARGET_WORDS = 150000

# --- Helpers ---
def fetch_posts(subreddit, keyword, limit=100):
    """Fetch top posts for a subreddit+keyword"""
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    headers = {"User-Agent": "Mozilla/5.0 (RedditCrawler/0.1 by YourUsername)"}
    params = {"q": keyword, "sort": "comments", "limit": limit, "restrict_sr": 1}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            print(f"Failed to fetch posts ({subreddit}, '{keyword}'): {r.status_code}")
            return []
        return r.json().get("data", {}).get("children", [])
    except Exception as e:
        print(f"Error fetching posts ({subreddit}, '{keyword}'): {e}")
        return []

#old fetch_comments which worked 90% of the time but sometimes when encountered 429 error the 
#backoff delay wasn't enough so had a new fn w expontential backoff
# def fetch_comments(post_url):
#     """Fetch post comments JSON with 429 backoff"""
#     headers = {"User-Agent": "Mozilla/5.0 (compatible; SC4021Crawler/1.0; contact=edu)"}
#     try:
#         r = requests.get(f"{post_url}.json", headers=headers, timeout=15)

#         if r.status_code == 429:
#             print("429 hit. Backing off...")
#             time.sleep(BACKOFF_DELAY)
#             r = requests.get(f"{post_url}.json", headers=headers, timeout=15)

#         if r.status_code != 200:
#             print(f"Failed to fetch comments: {r.status_code}")
#             return None

#         return r.json()
#     except Exception as e:
#         print(f"Error fetching comments: {e}")
#         return None
    
def fetch_comments(post_url, max_retries=3):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SC4021Crawler/1.0; contact=edu)"
    }

    delay = 5
    for attempt in range(max_retries):
        try:
            r = requests.get(f"{post_url}.json", headers=headers, timeout=15)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                print(f"429 hit. Backing off for {delay}s (attempt {attempt+1}/{max_retries})...")
                time.sleep(delay)
                delay *= 2  # exponential backoff
            else:
                print(f"Failed to fetch comments: {r.status_code}")
                return None
        except Exception as e:
            print(f"Error fetching comments: {e}")
            time.sleep(delay)
            delay *= 2
    print("Max retries exceeded. Skipping this post.")
    return None



def json_to_jsonl(input_json, output_file, post_url):
    """Convert Reddit JSON to JSONL and return number of comments and words processed"""
    if not input_json or len(input_json) < 2:
        return 0, 0

    post_data = input_json[0]["data"]["children"][0]["data"]
    subreddit = post_data["subreddit"]
    post_title = post_data["title"]

    comments_count = 0
    words_count = 0

    with open(output_file, "w", encoding="utf-8") as out_f:
        def process_comment(comment):
            nonlocal comments_count, words_count
            if comment["kind"] != "t1":
                return
            c = comment["data"]
            if c.get("body") in ["[deleted]", "[removed]"]:
                return
            text = c.get("body", "")
            record = {
                "id": c["id"],
                "text": text,
                "timestamp": datetime.utcfromtimestamp(c["created_utc"]).isoformat() + "Z",
                "source": "reddit",
                "metadata": {
                    "subreddit": subreddit,
                    "post_title": post_title,
                    "url": post_url
                }
            }
            out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
            
            # Count words
            comments_count += 1
            words_count += len(text.split())

            # Process replies recursively
            if c.get("replies") and isinstance(c["replies"], dict):
                for reply in c["replies"]["data"]["children"]:
                    process_comment(reply)

        for comment in input_json[1]["data"]["children"]:
            process_comment(comment)

    return comments_count, words_count


# --- Main loop ---
total_comments = 0
total_words = 0
done = False

for sub in subreddits:
    for kw in keywords:
        if done:
            break

        print(f"Fetching posts for r/{sub} with keyword '{kw}'...")
        posts = fetch_posts(sub, kw, limit=100)
        time.sleep(REQUEST_DELAY_SEARCH)

        for post in posts:
            post_data = post["data"]
            if post_data.get("num_comments", 0) < MIN_COMMENTS:
                continue

            post_url = f"https://www.reddit.com{post_data['permalink']}"
            print(f"  Fetching comments for post: {post_url} ({post_data.get('num_comments')} comments)")
            data = fetch_comments(post_url)
            time.sleep(REQUEST_DELAY_COMMENTS)
            if not data:
                continue

            filename = f"{sub}_{kw.replace(' ', '_')}_{post_data['id']}.jsonl"
            output_path = os.path.join(OUTPUT_FOLDER, filename)
            comments_count, words_count = json_to_jsonl(data, output_path, post_url)

            total_comments += comments_count
            total_words += words_count

            print(f"  Saved JSONL: {output_path} | +{comments_count} comments, +{words_count} words")

            if total_comments >= TARGET_COMMENTS and total_words >= TARGET_WORDS:
                print("Reached target corpus size!")
                done = True
                break

print(f"\nFinal stats: {total_comments} comments, {total_words} words")

