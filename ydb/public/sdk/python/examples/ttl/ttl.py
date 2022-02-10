# -*- coding: utf-8 -*-
from __future__ import print_function
import os
from concurrent.futures import TimeoutError

import ydb
import random


EXPIRATION_QUEUE_COUNT = 4
DOC_TABLE_PARTITION_COUNT = 4

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
REPLACE INTO expiration_queue_%d
    (`timestamp`, `doc_id`)
VALUES
    ($timestamp, $doc_id);
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

READ_EXPIRED_BATCH_TRANSACTION = """
--!syntax_v1
PRAGMA TablePathPrefix("%s");
DECLARE $timestamp AS Uint64;
DECLARE $prev_timestamp AS Uint64;
DECLARE $prev_doc_id AS Uint64;

$part_1 = (
    SELECT *
    FROM expiration_queue_%d
    WHERE
        `timestamp` <= $timestamp
            AND
        `timestamp` > $prev_timestamp
    ORDER BY `timestamp`, `doc_id`
    LIMIT 100
);

$part_2 = (
    SELECT *
    FROM expiration_queue_%d
    WHERE
        `timestamp` = $prev_timestamp AND `doc_id` > $prev_doc_id
    ORDER BY `timestamp`, `doc_id`
    LIMIT 100
);

$union = (
    SELECT * FROM $part_1
    UNION ALL
    SELECT * FROM $part_2
);


SELECT `timestamp`, `doc_id`
FROM $union
ORDER BY `timestamp`, `doc_id`
LIMIT 100;
"""

DELETE_EXPIRED_DOCUMENT = """
--!syntax_v1
PRAGMA TablePathPrefix("%s");

DECLARE $doc_id AS Uint64;
DECLARE $timestamp AS Uint64;

DELETE FROM documents
WHERE `doc_id` = $doc_id AND `timestamp` = $timestamp;

DELETE FROM expiration_queue_%d
WHERE `timestamp` = $timestamp AND `doc_id` = $doc_id;
"""


def is_directory_exists(driver, path):
    try:
        return driver.scheme_client.describe_path(path).is_directory()
    except ydb.SchemeError:
        return False


def ensure_path_exists(driver, database, path):
    paths_to_create = list()
    path = path.rstrip("/")
    while path != "":
        full_path = os.path.join(database, path)
        if is_directory_exists(driver, full_path):
            break
        paths_to_create.append(full_path)
        path = os.path.dirname(path).rstrip("/")

    while len(paths_to_create) > 0:
        full_path = paths_to_create.pop(-1)
        driver.scheme_client.make_directory(full_path)


# Creates Documents table and multiple ExpirationQueue tables
def create_tables(table_client, path):
    session = table_client.session().create()
    # Documents table stores the contents of web pages.
    # The table is partitioned by hash(Url) in order to evenly distribute the load.
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

    # Multiple ExpirationQueue tables allow to scale the load.
    # Each ExpirationQueue table can be handled by a dedicated worker.
    for expiration_queue in range(EXPIRATION_QUEUE_COUNT):
        session.create_table(
            os.path.join(path, "expiration_queue_%d" % expiration_queue),
            ydb.TableDescription()
            .with_primary_keys("timestamp", "doc_id")
            .with_columns(
                ydb.Column("doc_id", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column("timestamp", ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            ),
        )


# Insert or replaces a document.
def add_document(session, path, url, html, timestamp):
    queue = random.randint(0, EXPIRATION_QUEUE_COUNT - 1)
    # this will keep prepared query in cache
    prepared = session.prepare(ADD_DOCUMENT_TRANSACTION % (path, queue))
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


# Reads document contents.
def read_document(session, path, url):
    prepared = session.prepare(READ_DOCUMENT_TRANSACTION % path)
    print("> ReadDocument %s:" % url)
    result_sets = session.transaction().execute(prepared, {"$url": url}, commit_tx=True)
    result_set = result_sets[0]
    if len(result_set.rows) > 0:
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
    else:
        print(" Not found")


def read_expired_document(
    session, path, expiration_queue, timestamp, last_timestamp, last_doc_id
):
    prepared = session.prepare(
        READ_EXPIRED_BATCH_TRANSACTION % (path, expiration_queue, expiration_queue)
    )
    result_sets = session.transaction().execute(
        prepared,
        {
            "$timestamp": timestamp,
            "$prev_timestamp": last_timestamp,
            "$prev_doc_id": last_doc_id,
        },
        commit_tx=True,
    )
    return result_sets[0]


def delete_expired_document(session, path, expiration_queue, doc_id, timestamp):
    prepared = session.prepare(DELETE_EXPIRED_DOCUMENT % (path, expiration_queue))
    session.transaction().execute(
        prepared,
        {"$doc_id": doc_id, "$timestamp": timestamp},
        commit_tx=True,
    )


def delete_expired(session, path, expiration_queue, timestamp):
    print("> DeleteExpired from queue #%d:" % expiration_queue)
    last_timestamp = 0
    last_doc_id = 0
    while True:
        result_set = read_expired_document(
            session, path, expiration_queue, timestamp, last_timestamp, last_doc_id
        )

        if not result_set.rows:
            break

        for document in result_set.rows:
            last_doc_id = document.doc_id
            last_timestamp = document.timestamp
            print(" DocId: %s Timestamp: %d" % (last_doc_id, timestamp))
            delete_expired_document(
                session, path, expiration_queue, last_doc_id, last_timestamp
            )


def _run(driver, database, path):
    ensure_path_exists(driver, database, path)
    full_path = os.path.join(database, path)

    create_tables(driver.table_client, full_path)
    session = driver.table_client.session().create()

    add_document(
        session,
        full_path,
        "https://yandex.ru/",
        "<html><body><h1>Yandex</h1></body></html>",
        1,
    )

    add_document(
        session,
        full_path,
        "https://ya.ru/",
        "<html><body><h1>Yandex</h1></body></html>",
        2,
    )

    read_document(session, full_path, "https://yandex.ru/")
    read_document(session, full_path, "https://ya.ru/")

    for expiration_queue in range(EXPIRATION_QUEUE_COUNT):
        delete_expired(session, full_path, expiration_queue, 1)

    read_document(session, full_path, "https://ya.ru/")

    add_document(
        session,
        full_path,
        "https://yandex.ru/",
        "<html><body><h1>Yandex</h1></body></html>",
        2,
    )

    add_document(
        session,
        full_path,
        "https://yandex.ru/",
        "<html><body><h1>Yandex</h1></body></html>",
        3,
    )

    for expiration_queue in range(EXPIRATION_QUEUE_COUNT):
        delete_expired(session, full_path, expiration_queue, 2)

    read_document(session, full_path, "https://yandex.ru/")
    read_document(session, full_path, "https://ya.ru/")


def run(endpoint, database, path):
    driver_config = ydb.DriverConfig(
        endpoint, database, credentials=ydb.construct_credentials_from_environ()
    )
    with ydb.Driver(driver_config) as driver:
        try:
            driver.wait(timeout=5)
        except TimeoutError:
            raise RuntimeError("Connect failed to YDB")

        _run(driver, database, path)
