import argparse
import logging
import time

from mongoengine import connect
from opac_schema.v1.models import Article


logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dsn_db", help="MongoDB Host Connection String")
    args = parser.parse_args()

    print("Connecting to", args.dsn_db)
    logger.debug("Connecting to %s", args.dsn_db)
    connect(host=args.dsn_db)

    articles = Article.objects(abstracts__text="")
    num_of_articles = len(articles)
    print("Number of articles to fix:", num_of_articles)
    logger.debug("Number of articles to fix: %d", num_of_articles)

    while num_of_articles > 0:
        for i in range(0, num_of_articles, 1000):
            limit = i + 1000 if i + 1000 <= num_of_articles else num_of_articles
            print(f"{i + 1000} of {num_of_articles}")
            for article in articles[i:limit]:
                abstracts = [
                    abstract for abstract in article.abstracts if len(abstract.text) > 0
                ]
                article.update(
                    write_concern={"w": 1, "fsync": True},
                    abstracts=abstracts,
                    abstract_languages=[abstract.language for abstract in abstracts],
                )
            time.sleep(10)

        articles = Article.objects(abstracts__text="")
        num_of_articles = len(articles)
        print("Number of articles left:", num_of_articles)


if __name__ == "__main__":
    main()
