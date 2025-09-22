import requests
import praw
import time
import redis
from flask import current_app
from datetime import datetime
from typing import Optional


def config(k: str) -> str:
    """Reads configuration from file."""
    with open(f"/configs/default/shared-data/{k}", "r") as f:
        return f.read()


def main():
    REDIS_DUPLICATE_SET_COMMENT = "reddit_comment_ids"
    reddit_user_agent = "Commentharvester"

    try:
        reddit = praw.Reddit(
            client_id=config("REDDIT_CLIENT_ID"),
            client_secret=config("REDDIT_CLIENT_SECRET"),
            user_agent=reddit_user_agent,
        )

        redis_client: redis.StrictRedis = redis.StrictRedis(
            host=config("REDIS_HOST"), socket_connect_timeout=5, decode_responses=False
        )

        subreddit_names = ["Adelaide", "australia", "brisbane", "melbourne", "sydney"]
        new_comments = []

        print("Fetching comments from recent posts")

        for subreddit_name in subreddit_names:
            subreddit = reddit.subreddit(subreddit_name)

            for submission in subreddit.search(
                query="coffee", sort="new", time_filter="week"
            ):
                submission.comments.replace_more(limit=None)

                for comment in submission.comments.list():
                    if redis_client.sismember(REDIS_DUPLICATE_SET_COMMENT, comment.id):
                        continue

                    c_doc = {
                        "created_utc": datetime.utcfromtimestamp(
                            comment.created_utc
                        ).isoformat()
                        + "Z",
                        "p_id": submission.id,
                        "c_id": comment.id,
                        "body": comment.body,
                        "subreddit": submission.subreddit.display_name,
                        "title": submission.title,
                        "score": comment.score,
                    }
                    new_comments.append(c_doc)
                    redis_client.sadd(REDIS_DUPLICATE_SET_COMMENT, comment.id)

                time.sleep(1)

            current_app.logger.info(
                f"Harvested {len(new_comments)} new comments from coffee posts"
            )

            if new_comments:
                comment_response: Optional[requests.Response] = requests.post(
                    url=config("ENQ_COMMENT_URL"),
                    headers={"Content-Type": "application/json"},
                    json=new_comments,
                )

                comment_response.raise_for_status()

        return "OK"

    except praw.exceptions.PRAWException as e:
        current_app.logger.error(f"Error interacting with Reddit API: {e}")
        return f"Error with Reddit API: {e}", 500
    except requests.exceptions.RequestException as e:
        current_app.logger.error(f"Error enqueuing data: {e}")
        return f"Error enqueuing data: {e}", 500
    except Exception as e:
        current_app.logger.error(f"An unexpected error occurred: {e}")
        return f"An unexpected error occurred: {e}", 500


if __name__ == "__main__":
    main()
