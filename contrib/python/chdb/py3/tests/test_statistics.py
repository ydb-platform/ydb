#!python3

import os
import unittest
import chdb
import shutil
from chdb import session

N = 1000

class TestQueryStatistics(unittest.TestCase):
    def setUp(self) -> None:
        # create tmp csv file
        with open(".test.csv", "w") as f:
            f.write("a,b,c\n")
            for i in range(N):
                f.write(f"{i},{i*2},{i*3}\n")
        return super().setUp()

    def tearDown(self) -> None:
        # remove tmp csv file
        os.remove(".test.csv")
        return super().tearDown()

    def test_csv_stats(self):
        ret = chdb.query("SELECT * FROM file('.test.csv', CSV)", "CSV")
        self.assertEqual(ret.rows_read(), N)
        self.assertGreater(ret.elapsed(), 0.000001)
        self.assertEqual(ret.bytes_read(), 27000)
        print(f"SQL read {ret.rows_read()} rows, {ret.bytes_read()} bytes, elapsed {ret.elapsed()} seconds")

    def test_storage_stats(self):
        test_query_statistics_dir = ".tmp_test_query_statistics_dir"
        shutil.rmtree(test_query_statistics_dir, ignore_errors=True)
        sess = session.Session(test_query_statistics_dir)

        total_rows = 0
        total_bytes = 0
        total_storage_rows_read = 0
        total_storage_bytes_read = 0
        with sess.send_query("SELECT * FROM numbers(1000000)") as stream:
            for chunk in stream:
                total_rows += chunk.rows_read()
                total_bytes += chunk.bytes_read()
                total_storage_rows_read += chunk.storage_rows_read()
                total_storage_bytes_read += chunk.storage_bytes_read()

        self.assertEqual(total_rows, 1000000)
        self.assertEqual(total_storage_rows_read, 1000000)
        self.assertEqual(total_storage_bytes_read, 8000000)
        self.assertEqual(total_storage_rows_read, 1000000)

        ret = sess.query("SELECT WatchID,JavaEnable FROM url('https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet', 'Parquet')")
        self.assertEqual(ret.rows_read(), 1000000)
        self.assertEqual(ret.bytes_read(), 12000000)
        self.assertEqual(ret.storage_rows_read(), 1000000)
        self.assertEqual(ret.storage_bytes_read(), 122401411)

        ret = sess.query("SELECT WatchID,JavaEnable FROM url('https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet', 'Parquet') limit 10")
        self.assertEqual(ret.rows_read(), 10)
        self.assertEqual(ret.bytes_read(), 120)
        self.assertEqual(ret.storage_rows_read(), 65409)
        # self.assertEqual(ret.storage_bytes_read(), 7563614) # storage_bytes_read() is unstable

        ret = sess.query("SELECT WatchID,JavaEnable FROM url('https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet', 'Parquet') where JavaEnable < 0")
        self.assertEqual(ret.rows_read(), 0)
        self.assertEqual(ret.bytes_read(), 0)
        self.assertEqual(ret.storage_rows_read(), 0)
        self.assertEqual(ret.storage_bytes_read(), 0)

        sess.close()
        shutil.rmtree(test_query_statistics_dir, ignore_errors=True)

    def test_non_exist_stats(self):
        with self.assertRaises(Exception):
            ret = chdb.query("SELECT * FROM file('notexist.parquet', Parquet)", "Parquet")


if __name__ == "__main__":
    unittest.main()
