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


class TestDatetimeUdfAll(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_all_datetime_udf(self):

        with ydb.QuerySessionPool(self.driver) as session_pool:
            rows = 10
            table_name = 'datetime_udf_table'

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
                    id,
                    DateTime::ToSeconds(event_date) AS seconds,
                    DateTime::ToMilliseconds(event_date) AS millis,
                    DateTime::ToMicroseconds(event_date) AS micros,
                    DateTime::Format('%Y-%m-%d')(event_date) AS formatted,
                    DateTime::MakeDatetime(DateTime::Split(event_date)) AS recombined,
                    DateTime::MakeDatetime(DateTime::Update(DateTime::Split(event_date), Year=2025)) AS year_updated,
                    DateTime::MakeDatetime(DateTime::ShiftYears(DateTime::Split(event_date), 1)) AS plus_1_year,
                    DateTime::MakeDatetime(DateTime::ShiftQuarters(DateTime::Split(event_date), 1)) AS plus_1_quarter,
                    DateTime::MakeDatetime(DateTime::ShiftMonths(DateTime::Split(event_date), 2)) AS plus_2_months,
                    DateTime::MakeDatetime(DateTime::Split(DateTime::FromSeconds(DateTime::ToSeconds(event_date)))) AS roundtrip_sec,
                    DateTime::MakeDatetime(DateTime::Split(DateTime::FromMilliseconds(DateTime::ToMilliseconds(event_date)))) AS roundtrip_ms,
                    DateTime::MakeDatetime(DateTime::Split(DateTime::FromMicroseconds(DateTime::ToMicroseconds(event_date)))) AS roundtrip_us
                FROM {table_name}
                ORDER BY id;
            """

            result_sets = session_pool.execute_with_retries(query)
            rows = result_sets[0].rows
            assert len(rows) == 9

            for row in rows:
                assert isinstance(row['seconds'], int)
                assert isinstance(row['millis'], int)
                assert isinstance(row['micros'], int)
                assert row['formatted'].startswith(b'2023')
                for key in ['recombined', 'year_updated', 'plus_1_year', 'plus_1_quarter', 'plus_2_months', 'roundtrip_sec', 'roundtrip_ms', 'roundtrip_us']:
                    assert isinstance(row[key], bytes)

            # ---------------- SELECT Parse Functions ------------------
            query = """
                SELECT
                    DateTime::MakeTimestamp(DateTime::ParseIso8601("2023-01-02T12:34:56+0000")) AS iso,
                    DateTime::MakeTimestamp(DateTime::ParseHttp("Sunday, 06-Nov-94 08:49:37 GMT")) AS http,
                    DateTime::MakeTimestamp(DateTime::ParseRfc822("Fri, 4 Mar 2005 19:34:45 EST")) AS rfc,
                    DateTime::MakeTimestamp(DateTime::ParseX509("20091014165533Z")) AS x509,
                    DateTime::MakeDatetime(DateTime::Parse("%Y-%m-%d %H:%M:%S")("2024-02-03 13:14:15")) AS parsed
                """
            result_sets = session_pool.execute_with_retries(query)
            row = result_sets[0].rows[0]
            for value in row.values():
                assert isinstance(value, bytes) or isinstance(value, ydb.Timestamp)

