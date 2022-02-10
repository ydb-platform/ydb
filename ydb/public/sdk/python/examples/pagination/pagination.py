# -*- coding: utf-8 -*-
from __future__ import print_function
import os

import ydb
from concurrent.futures import TimeoutError
from . import sample_data


class SchoolsPaginated(object):
    def __init__(self, session, path, limit):
        self.session = session
        self.path = path
        self.limit = limit
        self.last_city = None
        self.last_number = None

        next_page_query = """
        PRAGMA TablePathPrefix("{path}");

        DECLARE $limit AS Uint32;
        DECLARE $lastCity AS Utf8;
        DECLARE $lastNumber AS Uint32;

        $Part1 = (
            SELECT * FROM schools
            WHERE `city` = $lastCity AND `number` > $lastNumber
            ORDER BY `city`, `number`
            LIMIT $limit
        );

        $Part2 = (
            SELECT * FROM schools
            WHERE `city` > $lastCity
            ORDER BY `city`, `number`
            LIMIT $limit
        );

        $Union = (SELECT * FROM $Part1 UNION ALL SELECT * FROM $Part2);
        SELECT * FROM $Union ORDER BY `city`, `number` LIMIT $limit;
        """.format(
            path=self.path
        )

        self.prepared_next_page_query = self.session.prepare(next_page_query)

    def get_first_page(self):
        query = """
        PRAGMA TablePathPrefix("{path}");

        DECLARE $limit AS Uint32;

        SELECT * FROM schools
        ORDER BY `city`, `number`
        LIMIT $limit;
        """.format(
            path=self.path
        )
        prepared_query = self.session.prepare(query)
        result_sets = self.session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query, {"$limit": self.limit}, commit_tx=True
        )
        return result_sets[0]

    def get_next_page(self):
        result_sets = self.session.transaction(ydb.SerializableReadWrite()).execute(
            self.prepared_next_page_query,
            {
                "$limit": self.limit,
                "$lastCity": self.last_city,
                "$lastNumber": self.last_number,
            },
            commit_tx=True,
        )
        return result_sets[0]

    def pages(self):
        while True:
            if self.last_city is None or self.last_number is None:
                result = self.get_first_page()
            else:
                result = self.get_next_page()
            if not result.rows:
                return
            last_row = result.rows[-1]
            self.last_city = last_row.city
            self.last_number = last_row.number
            yield result


def run(endpoint, database, path, auth_token):
    driver_config = ydb.DriverConfig(endpoint, database=database, auth_token=auth_token)
    try:
        driver = ydb.Driver(driver_config)
        driver.wait(timeout=5)
    except TimeoutError:
        raise RuntimeError("Connect failed to YDB")

    try:
        session = driver.table_client.session().create()
        sample_data.prepare_data(driver, session, database, path)
        db_path = os.path.join(database, path)

        page_num = 0
        for page in SchoolsPaginated(session, db_path, limit=3).pages():
            page_num = page_num + 1
            print("-- page: {} --".format(page_num))
            for row in page.rows:
                print(row.city.encode("utf-8"), row.number, row.address.encode("utf-8"))

    finally:
        driver.stop()
