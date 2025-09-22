import string
import re
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime


def fetch_all_docs(es, index_name, query):
    results = []
    scroll = helpers.scan(
        client=es, query=query, index=index_name, scroll="2m", size=1000
    )

    for doc in scroll:
        results.append(doc["_source"])

    return results


def get_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()
    vs = analyzer.polarity_scores(text)
    compound_score = vs["compound"]

    if compound_score > 0.05:
        return (compound_score, "positive")
    elif compound_score < -0.05:
        return (compound_score, "negative")
    else:
        return (compound_score, "neutral")


def preprocess(text):
    text = text.lower()
    text_without_urls = re.sub(
        r"http\S+|www\S+|https\S+", "", text, flags=re.IGNORECASE
    )

    punctuation_pattern = r"[" + re.escape(string.punctuation) + r"]"
    text_without_punctuation = re.sub(punctuation_pattern, "", text_without_urls)

    text_cleaned = re.sub(
        r"\b(?:'ve|ve|this|just|like|dont|got|really|think|know|knew|actually|way|yeah|did|im|ive|st|want|tho|thats|yes)\b",
        "",
        text_without_punctuation,
        flags=re.IGNORECASE,
    )
    text_cleaned = re.sub(r"\s+", " ", text_cleaned).strip()

    return text_cleaned


def main():
    es: Elasticsearch = Elasticsearch(
        "https://elasticsearch-master.elastic.svc.cluster.local:9200",
        ssl_show_warn=False,
        verify_certs=False,
        basic_auth=("elastic", "elastic"),
    )

    index_name = "reddit-coffee-post"
    index_name_comment = "reddit-coffee-comment"

    query_get_coffee_post = {
        "_source": ["id", "created_utc", "subreddit", "score", "selftext", "title"],
        "query": {"multi_match": {"query": "coffee", "fields": ["title", "selftext"]}},
    }

    query_get_coffee_comment = {
        "_source": ["p_id", "created_utc", "subreddit", "score", "body"],
        "query": {"multi_match": {"query": "coffee", "fields": ["body"]}},
    }

    # prepare the data for modelling
    p_docs = fetch_all_docs(es, index_name, query_get_coffee_post)
    df_post = pd.DataFrame(p_docs)
    df_post = df_post.sort_values(by="created_utc")
    df_post["text"] = df_post["title"] + " " + df_post["selftext"]

    c_docs = fetch_all_docs(es, index_name_comment, query_get_coffee_comment)
    df_comment = pd.DataFrame(c_docs)
    df_comment = df_comment.sort_values(by="created_utc")
    df_comment = df_comment.rename(columns={"p_id": "id", "body": "text"})

    merged_df = pd.concat(
        [
            df_post[["id", "created_utc", "subreddit", "score", "text"]],
            df_comment[["id", "created_utc", "subreddit", "score", "text"]],
        ],
        ignore_index=True,
    )
    merged_df["text"] = merged_df["text"].apply(preprocess)

    merged_df[["sentiment_score", "sentiment"]] = merged_df["text"].apply(
        lambda x: pd.Series(get_sentiment(x))
    )

    # topic modelling
    texts = merged_df["text"].tolist()
    vectorizer = TfidfVectorizer(max_df=0.95, min_df=2, stop_words="english")
    doc_term_matrix = vectorizer.fit_transform(texts)
    lda = LatentDirichletAllocation(n_components=20, random_state=42)
    lda.fit(doc_term_matrix)
    topics = lda.transform(doc_term_matrix)
    topic_df = pd.DataFrame(
        topics, columns=[f"topic_{i + 1}" for i in range(lda.n_components)]
    )

    # Get the top words for each topic to assign meaningful names
    feature_names = vectorizer.get_feature_names_out()
    topic_names = []

    for topic_idx, topic in enumerate(lda.components_):
        top_words = [
            feature_names[i] for i in topic.argsort()[: -10 - 1 : -1]
        ]  # Top 10 words
        topic_name = " ".join(top_words)
        topic_names.append(f"topic_{topic_idx + 1}: {topic_name}")

    merged_df = merged_df.join(topic_df)
    tp_name_df = pd.DataFrame(
        [topic_names], columns=[f"topic_{i + 1}" for i in range(len(topic_names))]
    )

    batch_id = datetime.utcnow().isoformat() + "Z"
    merged_df["batch_id"] = batch_id
    tp_name_df["batch_id"] = batch_id

    actions_analysis = [
        {
            "_index": "reddit-analysis",
            "_id": f"{i}_{row['batch_id']}",
            "_source": row.to_dict(),
        }
        for i, row in merged_df.iterrows()
    ]

    helpers.bulk(es, actions_analysis)

    actions_topic = [
        {
            "_index": "reddit-topic",
            "_id": f"{row['batch_id']}",
            "_source": row.to_dict(),
        }
        for i, row in tp_name_df.iterrows()
    ]

    helpers.bulk(es, actions_topic)

    return {
        "message": f"Processed len({merged_df}) coffee posts/comments. Analysis + topics stored."
    }


if __name__ == "__main__":
    main()
