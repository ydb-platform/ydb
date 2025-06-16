# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb

from datetime import datetime, timedelta, timezone
import random


class TestDatetime2(MixedClusterFixture):
    rows = 100
    table_name = 'datetime_test'

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def generate_insert(self):
        header = (
            f"UPSERT INTO {self.table_name} "
            "(id, d, dt, ts, val, rfc822_str, iso8601_str, http_str, x509_str) VALUES\n"
        )

        rows = []
        base_dt = datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        for i in range(1, self.rows + 1):
            delta = timedelta(days=random.randint(0, 10000), seconds=random.randint(0, 86400))
            dt = base_dt + delta
            dt_utc = dt.astimezone(timezone.utc)
            ts_micro = dt_utc.replace(microsecond=random.randint(0, 999999))
            val = random.randint(0, 32767)

            rfc822 = dt.strftime("%a, %-d %b %Y %H:%M:%S EST")
            iso8601 = dt.strftime("%Y-%m-%dT%H:%M:%S+0300")
            http = dt.strftime("%A, %d-%b-%y %H:%M:%S GMT")
            x509 = dt.strftime("%Y%m%d%H%M%SZ")

            row = f"""(
                {i},
                Date("{dt_utc.date()}"),
                Datetime("{dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")}"),
                Timestamp("{ts_micro.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}"),
                {val},
                "{rfc822}",
                "{iso8601}",
                "{http}",
                "{x509}"
            )"""
            rows.append(row)

        return header + ",\n".join(rows) + ";"

    def generate_create_table(self):
        return f"""
            CREATE TABLE {self.table_name} (
                id Uint32,
                d Date,
                dt Datetime,
                ts Timestamp,
                val Int16,

                rfc822_str String,
                iso8601_str String,
                http_str String,
                x509_str String,

                PRIMARY KEY(id)
            ) WITH (
                PARTITION_AT_KEYS = ({", ".join(str(i) for i in range(1, self.rows))}),
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {self.rows}
            );
        """

    def q_split(self):
        return f"""
        SELECT
            DateTime::MakeDate(DateTime::Split(d)),
            DateTime::MakeDate(DateTime::Split(dt)),
            DateTime::MakeDate(DateTime::Split(ts)),
            DateTime::MakeDate(DateTime::Split(AddTimezone(d, "Europe/Moscow"))),
            DateTime::MakeDate(DateTime::Split(AddTimezone(dt, "Europe/Moscow"))),
            DateTime::MakeDate(DateTime::Split(AddTimezone(ts, "Europe/Moscow")))
        FROM {self.table_name};
        """

    def q_make(self):
        table_name = 'datetime_test'
        return f"""
        SELECT
            DateTime::MakeDate(DateTime::Split(d)),
            DateTime::MakeDatetime(DateTime::Split(d)),
            DateTime::MakeTimestamp(DateTime::Split(d)),

            -- added DateTime::MakeDate(DateTime::Split( because python sdk doesn't support Tz
            DateTime::MakeDate(DateTime::Split(DateTime::MakeTzDate(DateTime::Split(d)))),
            DateTime::MakeDate(DateTime::Split(DateTime::MakeTzDatetime(DateTime::Split(d)))),
            DateTime::MakeDate(DateTime::Split(DateTime::MakeTzTimestamp(DateTime::Split(d))))
        FROM `{table_name}`;
        """

    def q_get(self):
        return f"""
        SELECT
            DateTime::GetYear(DateTime::Split(d)),
            DateTime::GetDayOfYear(DateTime::Split(d)),
            DateTime::GetMonth(DateTime::Split(d)),
            DateTime::GetMonthName(DateTime::Split(d)),
            DateTime::GetWeekOfYear(DateTime::Split(d)),
            DateTime::GetWeekOfYearIso8601(DateTime::Split(d)),
            DateTime::GetDayOfMonth(DateTime::Split(d)),
            DateTime::GetDayOfWeek(DateTime::Split(d)),
            DateTime::GetDayOfWeekName(DateTime::Split(d)),
            DateTime::GetHour(DateTime::Split(d)),
            DateTime::GetMinute(DateTime::Split(d)),
            DateTime::GetSecond(DateTime::Split(d)),
            DateTime::GetMillisecondOfSecond(DateTime::Split(d)),
            DateTime::GetMicrosecondOfSecond(DateTime::Split(d)),
            DateTime::GetTimezoneId(DateTime::Split(d)),
            DateTime::GetTimezoneName(DateTime::Split(d))
        FROM {self.table_name};
        """

    def q_update(self):
        return f"""
        SELECT
            DateTime::MakeDate(DateTime::Update(DateTime::Split(d), 2005))
        FROM {self.table_name};
        """

    def q_to_from(self):
        return f"""
        SELECT
            DateTime::FromSeconds(DateTime::ToSeconds(DateTime::MakeDate(DateTime::Split(d)))),
            DateTime::FromSeconds(DateTime::ToSeconds(DateTime::MakeDatetime(DateTime::Split(d)))),
            DateTime::FromSeconds(DateTime::ToSeconds(DateTime::MakeTimestamp(DateTime::Split(d)))),
            DateTime::FromSeconds(DateTime::ToSeconds(DateTime::MakeTzDate(DateTime::Split(d)))),
            DateTime::FromSeconds(DateTime::ToSeconds(DateTime::MakeTzDatetime(DateTime::Split(d)))),
            DateTime::FromSeconds(DateTime::ToSeconds(DateTime::MakeTzTimestamp(DateTime::Split(d)))),

            DateTime::FromMilliseconds(DateTime::ToMilliseconds(DateTime::MakeDate(DateTime::Split(d)))),
            DateTime::FromMilliseconds(DateTime::ToMilliseconds(DateTime::MakeDatetime(DateTime::Split(d)))),
            DateTime::FromMilliseconds(DateTime::ToMilliseconds(DateTime::MakeTimestamp(DateTime::Split(d)))),
            DateTime::FromMilliseconds(DateTime::ToMilliseconds(DateTime::MakeTzDate(DateTime::Split(d)))),
            DateTime::FromMilliseconds(DateTime::ToMilliseconds(DateTime::MakeTzDatetime(DateTime::Split(d)))),
            DateTime::FromMilliseconds(DateTime::ToMilliseconds(DateTime::MakeTzTimestamp(DateTime::Split(d)))),

            DateTime::FromMicroseconds(DateTime::ToMicroseconds(DateTime::MakeDate(DateTime::Split(d)))),
            DateTime::FromMicroseconds(DateTime::ToMicroseconds(DateTime::MakeDatetime(DateTime::Split(d)))),
            DateTime::FromMicroseconds(DateTime::ToMicroseconds(DateTime::MakeTimestamp(DateTime::Split(d)))),
            DateTime::FromMicroseconds(DateTime::ToMicroseconds(DateTime::MakeTzDate(DateTime::Split(d)))),
            DateTime::FromMicroseconds(DateTime::ToMicroseconds(DateTime::MakeTzDatetime(DateTime::Split(d)))),
            DateTime::FromMicroseconds(DateTime::ToMicroseconds(DateTime::MakeTzTimestamp(DateTime::Split(d))))
        FROM {self.table_name};
        """

    def q_interval(self):
        return f"""
        SELECT
            DateTime::IntervalFromDays(val),
            DateTime::IntervalFromHours(val),
            DateTime::IntervalFromMinutes(val),
            DateTime::IntervalFromSeconds(val),
            DateTime::IntervalFromMilliseconds(val),
            DateTime::IntervalFromMicroseconds(val),

            DateTime::ToDays(DateTime::IntervalFromDays(val)),
            DateTime::ToHours(DateTime::IntervalFromDays(val)),
            DateTime::ToMinutes(DateTime::IntervalFromDays(val)),
            DateTime::ToSeconds(DateTime::IntervalFromDays(val)),
            DateTime::ToMilliseconds(DateTime::IntervalFromDays(val)),
            DateTime::ToMicroseconds(DateTime::IntervalFromDays(val))
        FROM {self.table_name};
        """

    def q_start_end(self):
        return f"""
        SELECT
            DateTime::MakeDate(DateTime::StartOfYear(DateTime::Split(d))),
            DateTime::MakeDate(DateTime::StartOfQuarter(DateTime::Split(d))),
            DateTime::MakeDate(DateTime::StartOfMonth(DateTime::Split(d))),
            DateTime::MakeDate(DateTime::StartOfWeek(DateTime::Split(d))),
            DateTime::MakeDate(DateTime::StartOfDay(DateTime::Split(d))),
            DateTime::MakeDate(DateTime::StartOf(DateTime::Split(d), DateTime::IntervalFromDays(val))),
            -- not supported in 24-4: https://st.yandex-team.ru/YQL-19644
            -- DateTime::MakeDate(DateTime::EndOfQuarter(DateTime::Split(d))),
            -- DateTime::MakeDate(DateTime::EndOfMonth(DateTime::Split(d))),
            -- DateTime::MakeDate(DateTime::EndOfDay(DateTime::Split(d))),
            -- DateTime::MakeDate(DateTime::EndOfWeek(DateTime::Split(d))),
            -- DateTime::MakeDate(DateTime::EndOfYear(DateTime::Split(d))),
            -- DateTime::MakeDate(DateTime::EndOf(DateTime::Split(d), DateTime::IntervalFromDays(val)))
        FROM {self.table_name};
        """

    def q_shift(self):
        return f"""
        SELECT
            DateTime::MakeDate(DateTime::ShiftYears(DateTime::Split(d), 1)),
            DateTime::MakeDate(DateTime::ShiftQuarters(DateTime::Split(d), 1)),
            DateTime::MakeDate(DateTime::ShiftMonths(DateTime::Split(d), 1))
        FROM {self.table_name};
        """

    def q_format(self):
        return f"""
        SELECT
            DateTime::Format('%Y-%m-%d')(d)
        FROM {self.table_name};
        """

    def q_parse(self):
        return f"""
        SELECT
            DateTime::MakeDate(DateTime::ParseRfc822(rfc822_str)),
            DateTime::MakeDate(DateTime::ParseIso8601(iso8601_str)),
            DateTime::MakeDate(DateTime::ParseHttp(http_str)),
            DateTime::MakeDate(DateTime::ParseX509(x509_str)),
            DateTime::MakeDate(DateTime::Parse("%Y-%m-%dT%H:%M:%S")(iso8601_str))
        FROM {self.table_name};
        """

    def test_all(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:

            # ---------------- CREATE TABLE ------------------
            query = self.generate_create_table()
            session_pool.execute_with_retries(query)

            # ---------------- INSERT ------------------
            query = self.generate_insert()
            session_pool.execute_with_retries(query)

        # ---------------- SELECT ------------------
        queries = [
            self.q_split(),
            self.q_make(),
            self.q_get(),
            self.q_update(),
            self.q_to_from(),
            self.q_interval(),
            self.q_start_end(),
            self.q_shift(),
            self.q_format(),
            self.q_parse()
        ]

        """
        UDFs are compiled once on the node that initially receives the request.
        The compiled UDF is then propagated to all other nodes. Executing the query a single time only verifies
        compatibility in one direction—either from old to new or from new to old. Performing multiple retries
        increases the likelihood that the UDF will be compiled on both the old and new versions, thereby improving coverage of compatibility testing.

        Additionally, a session pool always sends requests to the same node. To ensure distribution across nodes, the session pool is recreated for each SELECT request.
        """
        for _ in range(10):
            with ydb.QuerySessionPool(self.driver) as session_pool:
                for query in queries:
                    result = session_pool.execute_with_retries(query)
                    assert len(result[0].rows) > 0
