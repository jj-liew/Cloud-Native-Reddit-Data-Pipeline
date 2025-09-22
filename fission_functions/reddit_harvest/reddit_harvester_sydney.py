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
    reddit_user_agent = "SydneyHarvester2025"
    subreddit_name = "sydney"
    REDIS_DUPLICATE_SET = "reddit_post_ids"

    try:
        reddit = praw.Reddit(
            client_id=config("REDDIT_CLIENT_ID"),
            client_secret=config("REDDIT_CLIENT_SECRET"),
            user_agent=reddit_user_agent,
        )

        redis_client: redis.StrictRedis = redis.StrictRedis(
            host=config("REDIS_HOST"), socket_connect_timeout=5, decode_responses=False
        )

        subreddit = reddit.subreddit(subreddit_name)
        post_count = 0
        batch_size = 10
        new_posts = []
        new_comments = []

        print(f"Fetching up to 10 posts from r/{subreddit_name}")

        for submission in subreddit.search("coffee", limit=None):
            if redis_client.sismember(REDIS_DUPLICATE_SET, submission.id):
                continue

            doc = {
                "author": str(submission.author),
                "created_utc": datetime.utcfromtimestamp(
                    submission.created_utc
                ).isoformat()
                + "Z",
                "id": submission.id,
                "num_comments": submission.num_comments,
                "score": submission.score,
                "selftext": submission.selftext,
                "subreddit": submission.subreddit.display_name,
                "title": submission.title,
                "url": submission.url,
            }
            new_posts.append(doc)

            submission.comments.replace_more(limit=None)

            if submission.comments.list():
                for comment in submission.comments.list():
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
            else:
                current_app.logger.error("Comments not found")

            redis_client.sadd(REDIS_DUPLICATE_SET, submission.id)

            post_count += 1
            if post_count >= batch_size:
                break

            time.sleep(2)

        current_app.logger.info(
            f"Harvested {len(new_posts)} new posts from r/{subreddit_name}"
        )

        # Route to message queue
        post_response: Optional[requests.Response] = requests.post(
            url=config("ENQ_URL"),
            headers={"Content-Type": "application/json"},
            json=new_posts,
        )

        post_response.raise_for_status()

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
