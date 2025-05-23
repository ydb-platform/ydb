# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb

from datetime import datetime, timedelta


class TestDatetime2Format(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_simple(self):

        with ydb.QuerySessionPool(self.driver) as session_pool:
            rows = 100
            table_name = 'dates_table'

            # ---------------- CREATE TABLE ------------------
            query = f"""
                CREATE TABLE {table_name} (
                    id Uint32,
                    event_date DateTime,
                    PRIMARY KEY (id)
                ) WITH (
                    PARTITION_AT_KEYS = ({", ".join(str(i) for i in range(1, rows))})
                );
            """
            session_pool.execute_with_retries(query)

            # ---------------- INSERT ------------------
            start_date = datetime(2023, 1, 1)
            values = []

            for i in range(1, rows):
                date = (start_date + timedelta(days=i - 1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                values.append(f"({i}, CAST(\"{date}\" AS DateTime))")

            query = f"""
                UPSERT INTO {table_name} (id, event_date) VALUES {",\n    ".join(values)};
            """

            session_pool.execute_with_retries(query)

            # ---------------- SELECT ------------------
            query = f"""
                SELECT
                    DateTime::Format('%Y-%m-%d')(event_date) as date
                FROM {table_name} order by date;
            """
            result_sets = session_pool.execute_with_retries(query)
            assert result_sets[0].rows[0]['date'] == b'2023-01-01'
