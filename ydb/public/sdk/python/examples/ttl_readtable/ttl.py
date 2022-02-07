# -*- coding: utf-8 -*-
from __future__ import print_function
import os
from concurrent.futures import TimeoutError

import ydb


DOC_TABLE_PARTITION_COUNT = 4
DELETE_BATCH_SIZE = 10

ADD_DOCUMENT_TRANSACTION = """
--!syntax_v1
PRAGMA TablePathPrefix("%s");
DECLARE $url AS Utf8;
DECLARE $html AS Utf8;
DECLARE $timestamp AS Uint64;

$doc_id = Digest::CityHash($url);

REPLACE INTO documents
    (`doc_id`, `url`, `html`, `timestamp`)
VALUES
    ($doc_id, $url, $html, $timestamp);
"""

READ_DOCUMENT_TRANSACTION = """
--!syntax_v1
PRAGMA TablePathPrefix("%s");
DECLARE $url AS Utf8;

$doc_id = Digest::CityHash($url);

SELECT `doc_id`, `url`, `html`, `timestamp`
FROM documents
WHERE `doc_id` = $doc_id;
"""
DELETE_EXPIRED_DOCUMENTS = """
--!syntax_v1
PRAGMA TablePathPrefix("%s");
DECLARE $keys AS List<Struct<doc_id: Uint64>>;

DECLARE $timestamp AS Uint64;

$expired = (
    SELECT d.doc_id AS doc_id
    FROM AS_TABLE($keys) AS k
    INNER JOIN documents AS d
    ON k.doc_id = d.doc_id
    WHERE `timestamp` <= $timestamp
);

DELETE FROM documents ON
SELECT * FROM $expired;
"""


def is_directory_exists(driver, path):
    try:
        return driver.scheme_client.describe_path(path).is_directory()
    except ydb.SchemeError:
        return False


def ensure_path_exists(driver, database, path):
    paths_to_create = list()
    path = path.rstrip("/")

    if path == "":
        return database

    while path not in ("", database):
        full_path = os.path.join(database, path)
        if is_directory_exists(driver, full_path):
            return full_path

        paths_to_create.append(full_path)
        path = os.path.dirname(path).rstrip("/")

    while True:
        full_path = paths_to_create.pop(-1)
        print("Creating directory %s" % full_path)
        driver.scheme_client.make_directory(full_path)

        if not paths_to_create:
            return full_path


# Creates Documents table and multiple ExpirationQueue tables
def create_tables(session_pool, path):
    def callee(session):
        # Documents table stores the contents of web pages.
        # The table is partitioned by hash(Url) in order to evenly distribute the load.
        print("Creating table %s" % os.path.join(path, "documents"))
        session.create_table(
            os.path.join(path, "documents"),
            ydb.TableDescription()
            .with_primary_keys("doc_id")
            .with_columns(
                ydb.Column("doc_id", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column("url", ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                ydb.Column("html", ydb.OptionalType(ydb.PrimitiveType.Utf8)),
                ydb.Column("timestamp", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            )
            .with_profile(
                ydb.TableProfile()
                # Partition Documents table by DocId
                .with_partitioning_policy(
                    ydb.PartitioningPolicy().with_uniform_partitions(
                        DOC_TABLE_PARTITION_COUNT
                    )
                )
            ),
        )

    return session_pool.retry_operation_sync(callee)


# Insert or replaces a document.
def add_document(session_pool, path, url, html, timestamp):
    # this will keep prepared query in cache
    def callee(session):
        prepared = session.prepare(ADD_DOCUMENT_TRANSACTION % path)
        print(
            "> AddDocument: \n"
            " Url: %s\n"
            " Timestamp %d"
            % (
                url,
                timestamp,
            )
        )
        session.transaction().execute(
            prepared,
            {"$url": url, "$html": html, "$timestamp": timestamp},
            commit_tx=True,
        )

    return session_pool.retry_operation_sync(callee)


# Reads document contents.
def read_document(session_pool, path, url):
    def callee(session):
        prepared = session.prepare(READ_DOCUMENT_TRANSACTION % path)
        print("> ReadDocument %s:" % url)
        result_sets = session.transaction().execute(
            prepared, {"$url": url}, commit_tx=True
        )
        result_set = result_sets[0]
        if len(result_set.rows) < 1:
            print(" Not found")
            return

        document = result_sets[0].rows[0]
        print(
            " DocId: %s\n"
            " Url: %s\n"
            " Timestamp: %d\n"
            " Html: %s"
            % (
                document.doc_id,
                document.url,
                document.timestamp,
                document.html,
            )
        )

    return session_pool.retry_operation_sync(callee)


def delete_expired_documents(session_pool, path, expired_documents, timestamp):
    def callee(session):
        print(
            "> DeleteExpiredDocuments: %s"
            % ", ".join(str(document.doc_id) for document in expired_documents)
        )
        prepared = session.prepare(DELETE_EXPIRED_DOCUMENTS % path)
        session.transaction().execute(
            prepared,
            commit_tx=True,
            parameters={
                "$timestamp": timestamp,
                "$keys": expired_documents,
            },
        )

    return session_pool.retry_operation_sync(callee)


def delete_expired_range(session_pool, path, key_range, timestamp):
    def callee(session):
        print(
            "> DeleteExpiredRange, From: %s, To %s"
            % (str(key_range.from_bound), str(key_range.to_bound))
        )
        # As single key range usually represents a single shard, so we batch deletions here
        # without introducing distributed transactions.
        return session.read_table(
            os.path.join(path, "documents"), key_range, columns=("doc_id", "timestamp")
        )

    table_iterator = session_pool.retry_operation_sync(callee)
    expired_documents = []
    while True:
        try:
            data_chunk = next(table_iterator)
        except StopIteration:
            break

        for document in data_chunk.rows:
            if document.timestamp <= timestamp:
                expired_documents.append(document)

            if len(expired_documents) >= DELETE_BATCH_SIZE:
                delete_expired_documents(
                    session_pool, path, expired_documents, timestamp
                )
                expired_documents = []

    if len(expired_documents) > 0:
        delete_expired_documents(session_pool, path, expired_documents, timestamp)


def delete_expired(session_pool, path, timestamp):
    print("> DeleteExpired, timestamp %s" % str(timestamp))
    table_description = session_pool.retry_operation_sync(
        lambda session: session.describe_table(
            path, ydb.DescribeTableSettings().with_include_shard_key_bounds(True)
        )
    )

    for key_range in table_description.shard_key_ranges:
        # DeleteExpiredRange can be run in parallel for different ranges.
        # Keep in mind that deletion RPS should be somehow limited in this case to avoid
        # spikes of cluster load due to TTL.
        delete_expired_range(session_pool, path, key_range, timestamp)


def _run(driver, session_pool, database, path):
    path = ensure_path_exists(driver, database, path)
    create_tables(session_pool, path)

    add_document(
        session_pool,
        path,
        "https://yandex.ru/",
        "<html><body><h1>Yandex</h1></body></html>",
        1,
    )

    add_document(
        session_pool,
        path,
        "https://ya.ru/",
        "<html><body><h1>Yandex</h1></body></html>",
        2,
    )

    read_document(session_pool, path, "https://yandex.ru/")
    read_document(session_pool, path, "https://ya.ru/")

    delete_expired(session_pool, path, 1)

    read_document(session_pool, path, "https://ya.ru/")

    add_document(
        session_pool,
        path,
        "https://yandex.ru/",
        "<html><body><h1>Yandex</h1></body></html>",
        2,
    )

    add_document(
        session_pool,
        path,
        "https://yandex.ru/",
        "<html><body><h1>Yandex</h1></body></html>",
        3,
    )

    delete_expired(session_pool, path, 2)

    read_document(session_pool, path, "https://yandex.ru/")
    read_document(session_pool, path, "https://ya.ru/")


def run(endpoint, database, path):
    driver_config = ydb.DriverConfig(
        endpoint, database, credentials=ydb.construct_credentials_from_environ()
    )
    with ydb.Driver(driver_config) as driver:
        try:
            driver.wait(timeout=5)
        except TimeoutError:
            raise RuntimeError("Connect failed to YDB")

        with ydb.SessionPool(driver, size=10) as session_pool:
            _run(driver, session_pool, database, path)
