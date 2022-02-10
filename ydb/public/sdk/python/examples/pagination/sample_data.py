# -*- coding: utf-8 -*-
import os

import ydb


class School(object):
    __slots__ = ("city", "number", "address")

    def __init__(self, city, number, address):
        self.city = city
        self.number = number
        self.address = address


def get_schools_data():
    return [
        School("Орлов", 1, "Ст.Халтурина, 2"),
        School("Орлов", 2, "Свободы, 4"),
        School("Яранск", 1, "Гоголя, 25"),
        School("Яранск", 2, "Кирова, 18"),
        School("Яранск", 3, "Некрасова, 59"),
        School("Кирс", 3, "Кирова, 6"),
        School("Нолинск", 1, "Коммуны, 4"),
        School("Нолинск", 2, "Федосеева, 2Б"),
        School("Котельнич", 1, "Урицкого, 21"),
        School("Котельнич", 2, "Октябрьская, 109"),
        School("Котельнич", 3, "Советская, 153"),
        School("Котельнич", 5, "Школьная, 2"),
        School("Котельнич", 15, "Октябрьская, 91"),
    ]


def create_table(session, path):
    session.create_table(
        os.path.join(path, "schools"),
        ydb.TableDescription()
        .with_column(ydb.Column("city", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
        .with_column(ydb.Column("number", ydb.OptionalType(ydb.PrimitiveType.Uint32)))
        .with_column(ydb.Column("address", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
        .with_primary_keys("city", "number"),
    )


def fill_table(session, path):
    query = """
    PRAGMA TablePathPrefix("{path}");

    DECLARE $schoolsData AS "List<Struct<
        city: Utf8,
        number: Uint32,
        address: Utf8>>";

    REPLACE INTO schools
    SELECT
        city,
        number,
        address
    FROM AS_TABLE($schoolsData);
    """.format(
        path=path
    )
    prepared_query = session.prepare(query)
    session.transaction(ydb.SerializableReadWrite()).execute(
        prepared_query,
        {"$schoolsData": get_schools_data()},
        commit_tx=True,
    )


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


def prepare_data(driver, session, database, path):
    ensure_path_exists(driver, database, path)
    db_path = os.path.join(database, path)
    create_table(session, db_path)
    fill_table(session, db_path)
