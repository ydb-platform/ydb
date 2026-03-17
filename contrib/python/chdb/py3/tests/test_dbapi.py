#!/usr/bin/env python3

import os
import shutil
import tempfile
import unittest
from chdb import dbapi

# version should be string split by '.'
# eg. '0.12.0' or '0.12.0rc1' or '0.12.0beta1' or '0.12.0alpha1' or '0.12.0a1'
expected_version_pattern = r"^\d+\.\d+\.\d+(.*)?$"
expected_clickhouse_version_pattern = r"^\d+\.\d+\.\d+.\d+$"


class TestDBAPI(unittest.TestCase):

    def test_select_version(self):
        conn = dbapi.connect()
        cur = conn.cursor()
        cur.execute("select version()")  # ClickHouse version
        # description = cur.description
        data = cur.fetchone()
        cur.close()
        conn.close()

        # Add your assertions here to validate the description and data
        # print(description)
        print(data)
        self.assertRegex(data[0], expected_clickhouse_version_pattern)

    def test_insert_and_read_data(self):
        # make a tmp dir context
        with tempfile.TemporaryDirectory() as tmpdirname:
            conn = dbapi.connect(tmpdirname)
            print(conn)
            cur = conn.cursor()
            # cur.execute("CREATE DATABASE IF NOT EXISTS test_db ENGINE = Atomic")
            # cur.execute("USE test_db")
            cur.execute(
                """
            CREATE TABLE rate (
                day Date,
                value Int64
            ) ENGINE = ReplacingMergeTree ORDER BY day"""
            )

            # Insert single value
            cur.execute("INSERT INTO rate VALUES (%s, %s)", ("2021-01-01", 24))
            # Insert multiple values
            cur.executemany(
                "INSERT INTO rate VALUES (%s, %s)",
                [("2021-01-02", 128), ("2021-01-03", 256)],
            )
            # Test executemany outside optimized INSERT/REPLACE path
            cur.executemany(
                "ALTER TABLE rate UPDATE value = %s WHERE day = %s",
                [(72, "2021-01-02"), (96, "2021-01-03")],
            )

            # Test fetchone
            cur.execute("SELECT value FROM rate ORDER BY day DESC LIMIT 2")
            row1 = cur.fetchone()
            self.assertEqual(row1, (96,))
            row2 = cur.fetchone()
            self.assertEqual(row2, (72,))
            row3 = cur.fetchone()
            self.assertIsNone(row3)

            # Test fetchmany
            cur.execute("SELECT value FROM rate ORDER BY day DESC")
            result_set1 = cur.fetchmany(2)
            self.assertEqual(result_set1, ((96,), (72,)))
            result_set2 = cur.fetchmany(1)
            self.assertEqual(result_set2, ((24,),))

            # Test fetchall
            cur.execute("SELECT value FROM rate ORDER BY day DESC")
            rows = cur.fetchall()
            self.assertEqual(rows, ((96,), (72,), (24,)))

            # Clean up
            cur.close()
            conn.close()

    def test_select_chdb_version(self):
        ver = dbapi.get_client_info()  # chDB version liek '0.12.0'
        ver_tuple = dbapi.chdb_version  # chDB version tuple like ('0', '12', '0')
        print(ver)
        print(ver_tuple)
        self.assertEqual(ver, ".".join(ver_tuple))
        self.assertRegex(ver, expected_version_pattern)

    def test_insert_escape_slash(self):
        # make a tmp dir context
        with tempfile.TemporaryDirectory() as tmpdirname:
            conn = dbapi.connect(tmpdirname)
            print(conn)
            cur = conn.cursor()
            # cur.execute("CREATE DATABASE IF NOT EXISTS test_db ENGINE = Atomic")
            # cur.execute("USE test_db")
            cur.execute(
                """
            CREATE TABLE tmp (
                s String
            ) ENGINE = Log"""
            )

            # Insert single value
            s = "hello\\'world"
            print("Inserting string: ", s)
            cur.execute("INSERT INTO tmp VALUES (%s)", (s))

            # Test fetchone
            cur.execute("SELECT s FROM tmp")
            row1 = cur.fetchone()
            self.assertEqual(row1[0], s)

            # Clean up
            cur.close()
            conn.close()


if __name__ == "__main__":
    unittest.main()
