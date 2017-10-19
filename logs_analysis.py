#!/usr/bin/env python3

import psycopg2

# core data for the different queries and query results

query_1 = ("What are the most popular three articles of all time?")

query_1_results = (
    "select title, count(title) as views from articles, log where "
    "log.path = concat('/article/', articles.slug) group by title order "
    "by views desc limit 3")

query_2 = ("Who are the most popular article authors of all time?")

query_2_results = (
    "select authors.name, count(articles.author) as views from articles, "
    "log, authors where log.path = concat('/article/', articles.slug) and "
    "articles.author = authors.id group by authors.name order by views desc")

query_3 = ("On which days did more than 1% of requests lead to errors?")

query_3_results = (
    "select date, total, error, (error::float*100)/total::float as perc "
    "from (select time::timestamp::date as date, count(status) as total, "
    "sum(case when status = '404 NOT FOUND' then 1 else 0 end) as error "
    "from log group by time::timestamp::date) as result where "
    "(error::float*100)/total::float >= 1 order by perc desc")

DBNAME = "news"


def connect():
    """Connect to the PostgresSQL database. Returns a database connection."""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    return db, c


def get_query_results(query):
    """Get each query result for the given query"""
    db, c = connect()
    c.execute(query)
    return c.fetchall()
    db.close()


def print_articles_results(query_results):
    """Prints the most popular three articles of all time"""
    print(query_results[1])
    for i, line in enumerate(query_results[0]):
        print("\t", i+1, "-", line[0], "\t - ", str(line[1]), "views")


def print_authors_results(query_results):
    """Print the most popular article authors of all time"""
    print(query_results[1])
    for i, line in enumerate(query_results[0]):
        print("\t", i+1, "-", line[0], "\t - ", str(line[1]), "views")


def print_error_results(query_results):
    """Prints the days on which more than 1% of requests lead to errors"""
    print(query_results[1])
    for i in query_results[0]:
        print("\t", "*", "-", i[0], "-", str(round(i[3], 2)) + "% errors")


if __name__ == '__main__':
    # stores  the query_results per the given quary
    articles_results = get_query_results(query_1_results), query_1
    authors_results = get_query_results(query_2_results), query_2
    error_results = get_query_results(query_3_results), query_3

    # prints the results per the stored quary_results
    print_articles_results(articles_results)
    print_authors_results(authors_results)
    print_error_results(error_results)
