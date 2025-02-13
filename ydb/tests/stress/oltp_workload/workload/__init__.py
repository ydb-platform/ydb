# -*- coding: utf-8 -*-
import ydb
import time
import threading

from ydb.tests.stress.common.common import WorkloadBase

ydb.interceptor.monkey_patch_event_handler()

supported_pk_types = [
    "Int64",
    "Uint64",
    "Int32",
    "Uint32",
    "Int16",
    "Uint16",
    "Int8",
    "Uint8",
    "Bool",
    "Decimal(1,0)",
    "Decimal(22,9)",
    "Decimal(35,10)",
    "DyNumber",

    "String",
    "Utf8",
    "Uuid",

    "Date",
    "Datetime",
    "Timestamp",
    "Interval",
    "Date32",
    "Datetime64",
    "Timestamp64",
    "Interval64"
]

supported_types = supported_pk_types + [
    "Float",
    "Double",
    "Json",
    "JsonDocument",
    "Yson"
]

null_types = [
    "Int64",
    "Decimal(22,9)",
    "Decimal(35,10)",
    "String",
]


class WorkloadInsertDeleteAllTypes(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "insert_delete_all_types", stop)
        self.inserted = 0
        self.current = 0
        self.table_name = "table"
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Inserted: {self.inserted}, Current: {self.current}"

    def _loop(self):
        table_path = self.get_table_path(self.table_name)
        create_sql = f"""
            CREATE TABLE `{table_path}` (
                {", ".join(["pk" + str(i) + " " + supported_pk_types[i] for i in range(len(supported_pk_types))])},
                {", ".join(["null_pk" + str(i) + " " + null_types[i] for i in range(len(null_types))])},
                {", ".join(["col" + str(i) + " " + supported_types[i] for i in range(len(supported_types))])},
                {", ".join(["null_col" + str(i) + " " + null_types[i] for i in range(len(null_types))])},
                PRIMARY KEY(
                {", ".join(["pk" + str(i) for i in range(len(supported_pk_types))])},
                {", ".join(["null_pk" + str(i) for i in range(len(null_types))])}
                )
            )
        """
        # print(create_sql)

        self.client.query(create_sql, True,)
        i = 1
        while not self.is_stop_requested():
            insert_sql = f"""
                INSERT INTO `{table_path}` (
                {", ".join(["pk" + str(i) for i in range(len(supported_pk_types))])},
                {", ".join(["null_pk" + str(i) for i in range(len(null_types))])},
                {", ".join(["col" + str(i) for i in range(len(supported_types))])},
                {", ".join(["null_col" + str(i) for i in range(len(null_types))])}
                )
                VALUES
                ({i * 2}, {i * 10},
                -2147483648, 0, -32768, 0, -128, 0, false,
                    CAST('1' AS Decimal(1,0)), CAST('1234567890123456789.000000001' AS Decimal(22,9)), CAST('1234567890123456789123456789.000000001' AS Decimal(35,10)),
                    CAST('-1.234' AS DyNumber), 'AnotherString', 'AnotherUtf8', CAST('123e4567-e89b-12d3-a456-556642440000' AS Uuid),
                    CAST('2023-10-02' AS Date), CAST('2023-10-02T11:00:00' AS Datetime), CAST(1696243200000000 AS Timestamp), CAST(-86400 AS Interval),
                    Date32('998-06-02'), CAST('2023-10-02T11:00:00.654321' AS Datetime64), Timestamp64('0998-06-02T12:30:00.123456Z'),Interval64('PT2S'),
                    NULL, NULL, NULL, NULL,
                -2000000, {i * 10}, -222, 222, -22, 22, -2, 2, true,
                    CAST('2' AS Decimal(1,0)), CAST('2234567890123456789.000000001' AS Decimal(22,9)), CAST('2234567890123456789123456789.000000001' AS Decimal(35,10)),
                    CAST('123E4' AS DyNumber), 'SampleString', 'SampleUtf8', CAST('550e8400-e29b-41d4-a716-446655440000' AS Uuid),
                    CAST('2023-10-01' AS Date), CAST('2023-10-01T10:00:00' AS Datetime), CAST(1696156800000000 AS Timestamp), CAST(3600 AS Interval),
                    Date32('998-06-01'), CAST('2023-10-01T10:00:00.123456' AS Datetime64), Timestamp64('0998-06-02T12:30:00.678901Z'),Interval64('-PT2S'), 3.14f, 2.71828,
                    CAST('{{"json_key":"json_value"}}' AS Json), CAST('{{"doc_key":"doc_value"}}' AS JsonDocument),  CAST('<yson><key1>value1</key1></yson>' AS Yson),
                    NULL, NULL, NULL, NULL),
                ({i * 2 + 1}, {i * 10 + 1},
                2147483647, 4294967295, 32767, 65535, 127, 255, true,
                    CAST('3' AS Decimal(1,0)), CAST('3234567890123456789.000000001' AS Decimal(22,9)), CAST('3234567890123456789123456789.000000001' AS Decimal(35,10)),
                    CAST('4.567E-3' AS DyNumber), 'ExampleString', 'ExampleUtf8', CAST('00112233-4455-6677-8899-aabbccddeeff' AS Uuid),
                    CAST('2022-12-31' AS Date), CAST('2022-12-31T23:59:59' AS Datetime), CAST(1672444799000000 AS Timestamp), CAST(172800 AS Interval),
                    Date32('1000-01-01'), CAST('2022-12-31T23:59:59.999999' AS Datetime64), Timestamp64('1000-01-01T00:00:00.000000Z'), Interval64('PT1440M'),
                    NULL, NULL, NULL, NULL,
                -4000000, {i * 10 + 1}, -444, 444, -44, 44, -4, 4, false,
                    CAST('4' AS Decimal(1,0)), CAST('4234567890123456789.000000001' AS Decimal(22,9)), CAST('4234567890123456789123456789.000000001' AS Decimal(35,10)),
                    CAST('-987E-4' AS DyNumber), 'NewString', 'NewUtf8', CAST('01234567-89ab-cdef-0123-456789abcdef' AS Uuid),
                    CAST('1980-03-15' AS Date), CAST('1980-03-15T08:00:00' AS Datetime), CAST(315532800000000 AS Timestamp), CAST(-31536000 AS Interval),
                    Date32('2000-02-29'), CAST('1980-03-15T08:00:00.123456' AS Datetime64), Timestamp64('2000-02-29T12:30:00.999999Z'), Interval64('-PT600S'), -0.123f, 2.71828,
                    CAST('{{"another_key":"another_value"}}' AS Json), CAST('{{"another_doc_key":"another_doc_value"}}' AS JsonDocument),  CAST('<yson><key2>value2</key2></yson>' AS Yson),
                    NULL, NULL, NULL, NULL);
            """
            # print(insert_sql)
            self.client.query(insert_sql, False,)

            self.client.query(
                f"""
                DELETE FROM `{table_path}`
                WHERE col1 % 2 == 1 AND null_pk0 IS NULL
            """,
                False,
            )

            actual = self.client.query(
                f"""
                SELECT COUNT(*) as cnt, SUM(col1) as vals, SUM(pk0) as ids FROM `{table_path}`
            """,
                False,
            )[0].rows[0]
            expected = {"cnt": i, "vals": i * (i + 1) * 5, "ids": i * (i + 1)}
            if actual != expected:
                raise Exception(f"Incorrect result: expected:{expected}, actual:{actual}")
            i += 1
            with self.lock:
                self.inserted += 2
                self.current = actual["cnt"]

    def get_workload_thread_funcs(self):
        return [self._loop]


class WorkloadRunner:
    def __init__(self, client, path, duration):
        self.client = client
        self.name = path
        self.tables_prefix = "/".join([self.client.database, self.name])
        self.duration = duration
        ydb.interceptor.monkey_patch_event_handler()

    def __enter__(self):
        self._cleanup()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()

    def _cleanup(self):
        print(f"Cleaning up {self.tables_prefix}...")
        deleted = self.client.remove_recursively(self.tables_prefix)
        print(f"Cleaning up {self.tables_prefix}... done, {deleted} tables deleted")

    def run(self):
        stop = threading.Event()

        workloads = [
            WorkloadInsertDeleteAllTypes(self.client, self.name, stop),
        ]

        for w in workloads:
            w.start()
        started_at = started_at = time.time()
        while time.time() - started_at < self.duration:
            print(f"Elapsed {(int)(time.time() - started_at)} seconds, stat:")
            for w in workloads:
                print(f"\t{w.name}: {w.get_stat()}")
            time.sleep(10)
        stop.set()
        print("Waiting for stop...")
        for w in workloads:
            w.join()
        print("Waiting for stop... stopped")
