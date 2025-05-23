# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb
from datetime import datetime, timedelta


class TestDatetimeUdfAll(MixedClusterFixture):
    rows = 100
    table_name = 'datetime_udf_table'

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def execute_query_with_retries(self, query):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            return session_pool.execute_with_retries(query)

    def prepare_tables(self):
        # ---------------- CREATE TABLE ------------------
        query = f"""
            CREATE TABLE {self.table_name} (
                id Uint32,
                event_date DateTime,
                PRIMARY KEY (id)
            ) WITH (
                PARTITION_AT_KEYS = ({", ".join(str(i) for i in range(1, self.rows))})
            );
        """
        self.execute_query_with_retries(query)

        # ---------------- INSERT ------------------
        start_date = datetime(2023, 1, 1)
        values = []

        for i in range(1, self.rows):
            date = (start_date + timedelta(days=i - 1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            values.append(f"({i}, CAST(\"{date}\" AS DateTime))")

        query = f"""
            UPSERT INTO {self.table_name} (id, event_date) VALUES {",\n    ".join(values)};
        """
        self.execute_query_with_retries(query)

    def test_all_datetime_udf(self):
        self.prepare_tables()


        # ---------------- Split ------------------
        query = f"""
            SELECT
                DateTime::Split(Date(event_date)).Year,
                DateTime::Split(Datetime(event_date)).Month,
                DateTime::Split(Timestamp(event_date)).Day,
                DateTime::Split(DateTime::MakeTzDate(event_date, "Europe/Moscow")).Hour,
                DateTime::Split(DateTime::MakeTzDatetime(event_date, "Europe/Moscow")).Minute,
                DateTime::Split(DateTime::MakeTzTimestamp(event_date, "Europe/Moscow")).Second,
                DateTime::MakeDatetime(DateTime::Split(event_date))
            FROM {self.table_name};
        """
        res = self.execute_query_with_retries(query)
        assert len(res) > 0

