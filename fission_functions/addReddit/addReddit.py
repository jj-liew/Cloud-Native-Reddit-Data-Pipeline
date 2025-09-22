import json
import redis
from typing import Dict, List, Any
from flask import current_app
from elasticsearch8 import Elasticsearch


def config(k: str) -> str:
    """Reads configuration from file."""
    with open(f"/configs/default/shared-data/{k}", "r") as f:
        return f.read()


def main() -> str:
    # Initialize Elasticsearch client
    es_client: Elasticsearch = Elasticsearch(
        config("ES_CLIENT"),
        verify_certs=False,
        ssl_show_warn=False,
        basic_auth=(config("ES_USERNAME"), config("ES_PASSWORD")),
    )

    redis_client: redis.StrictRedis = redis.StrictRedis(
        host=config("REDIS_HOST"), socket_connect_timeout=5, decode_responses=False
    )

    try:
        # Process post
        raw_batch_post = redis_client.lindex("rharvest", 0)
        if not raw_batch_post:
            current_app.logger.info("No Reddit post batch available in Redis list.")
            return "ok (no data)"

        try:
            batch_post_data: List[Dict[str, Any]] = json.loads(
                raw_batch_post.decode("utf-8")
            )
        except json.JSONDecodeError:
            current_app.logger.error("Failed to decode JSON post batch from Redis.")
            return "error: invalid post batch", 400

        current_app.logger.info(
            f"Processing batch of {len(batch_post_data)} posts from Redis."
        )

        all_success_post = True
        for post in batch_post_data:
            try:
                doc_id: str = str(post.get("id"))

                es_doc = {
                    "author": post.get("author"),
                    "created_utc": post.get("created_utc"),
                    "id": post.get("id"),
                    "num_comments": post.get("num_comments"),
                    "score": post.get("score"),
                    "selftext": post.get("selftext"),
                    "subreddit": post.get("subreddit"),
                    "title": post.get("title"),
                    "url": post.get("url"),
                }

                index_response = es_client.index(
                    index=config("ES_REDDIT_INDEX"), id=doc_id, body=es_doc
                )

                current_app.logger.info(
                    f"Indexed post {doc_id} - Version: {index_response['_version']}"
                )
            except Exception as e:
                all_success_post = False
                current_app.logger.error(f"Failed to index post: {doc_id} - {e}")

        if all_success_post:
            redis_client.lrem("rharvest", 1, raw_batch_post)
            current_app.logger.info(
                "Successfully removed processed post batch from Redis."
            )
        else:
            current_app.logger.warning(
                "Post batch not removed due to partial indexing failure."
            )

        # Process comment
        raw_batch_comment = redis_client.lindex("rharvestcomment", 0)
        if not raw_batch_comment:
            current_app.logger.info("No Reddit comment batch available in Redis list.")
            return "ok (no data)"

        try:
            batch_comment_data: List[Dict[str, Any]] = json.loads(
                raw_batch_comment.decode("utf-8")
            )
        except json.JSONDecodeError:
            current_app.logger.error("Failed to decode JSON comment batch from Redis.")
            return "error: invalid comment batch", 400

        current_app.logger.info(
            f"Processing batch of {len(batch_comment_data)} comments from Redis."
        )

        all_success_comment = True
        for comment in batch_comment_data:
            try:
                comment_id: str = str(comment.get("c_id"))

                c_doc = {
                    "created_utc": comment.get("created_utc"),
                    "p_id": comment.get("p_id"),
                    "c_id": comment.get("c_id"),
                    "score": comment.get("score"),
                    "body": comment.get("body"),
                    "subreddit": comment.get("subreddit"),
                    "title": comment.get("title"),
                }

                index_response = es_client.index(
                    index=config("ES_REDDIT_COMMENT_INDEX"), id=comment_id, body=c_doc
                )

                current_app.logger.info(
                    f"Indexed comment {doc_id} - Version: {index_response['_version']}"
                )
            except Exception as e:
                all_success_comment = False
                current_app.logger.error(f"Failed to index comment: {comment_id} - {e}")

        if all_success_comment:
            redis_client.lrem("rharvestcomment", 1, raw_batch_comment)
            current_app.logger.info(
                "Successfully removed processed comment batch from Redis."
            )
        else:
            current_app.logger.warning(
                "Comment batch not removed due to partial indexing failure."
            )

        return "ok"

    except redis.RedisError as e:
        current_app.logger.error(f"Redis error: {e}")
        return f"Error: Redis error - {e}", 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error: {e}")
        return f"Unexpected error: {e}", 500
