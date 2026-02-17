import json, os
from datetime import datetime

#REV u manually download json file frm reddit posts then this prog will convert them to jsonl

def reddit_json_to_jsonl(input_json_file, output_jsonl_file, post_url):
    """
    Convert Reddit JSON (from /comments/.json) to JSONL where each comment is 1 record.
    Handles nested replies recursively.
    """
    with open(input_json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Post info for metadata
    post_data = data[0]["data"]["children"][0]["data"]
    subreddit = post_data["subreddit"]
    post_title = post_data["title"]

    with open(output_jsonl_file, "w", encoding="utf-8") as out_f:

        def process_comment(comment):
            if comment["kind"] != "t1":  # only process comments
                return
            c = comment["data"]
            record = {
                "id": c["id"],
                "text": c.get("body", ""),
                "timestamp": datetime.utcfromtimestamp(c["created_utc"]).isoformat() + "Z",
                "source": "reddit",
                "metadata": {
                    "subreddit": subreddit,
                    "post_title": post_title,
                    "url": post_url
                }
            }
            out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
            
            # Process replies recursively
            if c.get("replies") and isinstance(c["replies"], dict):
                for reply in c["replies"]["data"]["children"]:
                    process_comment(reply)

        # Top-level comments
        comments_data = data[1]["data"]["children"]
        for comment in comments_data:
            process_comment(comment)

    print(f"Finished! JSONL saved to {output_jsonl_file}")

# List of input/output files + URLs
#REV not a must to incl url here, but useful so that we know which post each comment came from
files = [
    {"input": "recruiting1.json", "url": "https://www.reddit.com/r/recruiting/comments/1ph6qhq/ai_recruiting_is_going_nowhere/"},
    {"input": "recruiting2.json", "url": "https://www.reddit.com/r/recruiting/comments/1jfeum2/recruiters_and_ai/"},
    {"input": "recruiting3.json", "url": "https://www.reddit.com/r/recruiting/comments/1ol29y8/anyone_actually_cut_hiring_costs_using_ai/"},
    {"input": "recruiting4.json", "url": "https://www.reddit.com/r/recruiting/comments/1nuxuwv/anyone_actually_had_a_positive_experience_with_ai/"},
    {"input": "recruitment1.json", "url": "https://www.reddit.com/r/Recruitment/comments/1jmizg8/ai_in_hiring_tools_yes_or_no/"}
]

for f in files:
    input_file = f["input"]
    #assume output file same name as input file
    output_file = os.path.splitext(input_file)[0] + ".jsonl"  # replaces .json with .jsonl
    reddit_json_to_jsonl(input_file, output_file, f["url"])
