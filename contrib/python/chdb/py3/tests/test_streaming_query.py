#!python3

import time
import shutil
import unittest
import threading
from chdb import session
import chdb

test_streaming_query_dir = ".tmp_test_streaming_query"
result_counter_streaming1 = 0
result_counter_streaming2 = 0
result_counter_normal1 = 0
result_counter_normal2 = 0
insert_counter = 0
stop_event = threading.Event()

class TestStreamingQuery(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(test_streaming_query_dir, ignore_errors=True)
        self.sess = session.Session(test_streaming_query_dir)
        return super().setUp()

    def tearDown(self) -> None:
        self.sess.close()
        shutil.rmtree(test_streaming_query_dir, ignore_errors=True)
        return super().tearDown()

    def test_streaming_query_with_session(self):
        self.sess.query("CREATE DATABASE IF NOT EXISTS test")
        self.sess.query("USE test")
        self.sess.query("CREATE TABLE IF NOT EXISTS streaming_test (id Int64) ENGINE = MergeTree() ORDER BY id")
        self.sess.query("INSERT INTO streaming_test SELECT number FROM numbers(1000000)")

        total_rows = 0
        with self.sess.send_query("SELECT * FROM streaming_test", "CSVWITHNAMES") as stream:
            for chunk in stream:
                rows = chunk.data().split('\n')
                total_rows += len(rows) - 2
                # print(f"Chunk received {len(rows) - 2} rows")
        self.assertEqual(total_rows, 1000000)

        self.sess.query("INSERT INTO streaming_test VALUES (1000001)")
        result = self.sess.query("SELECT * FROM streaming_test", "CSVWITHNAMES")
        self.assertEqual(result.rows_read(), 1000001)

        stream = self.sess.send_query("SELECT * FROM streaming_test WHERE id > 999999", "CSVWITHNAMES")
        chunks = list(stream)
        self.assertEqual(len(chunks), 1)
        self.assertIn("1000001", chunks[0].data())

        with self.assertRaises(Exception):
            ret = self.sess.send_query("SELECT * FROM streaming_test;SELECT * FROM streaming_test", "CSVWITHNAMES")
        with self.assertRaises(Exception):
            ret = self.sess.send_query("INSERT INTO streaming_test SELECT number FROM numbers(1000000)", "CSVWITHNAMES")
        with self.assertRaises(Exception):
            ret = self.sess.send_query("SELECT * FROM streaming_test;SELECT * FROM streaming_test", "CSVWITHNAMES")

    def test_streaming_query_with_connection(self):
        self.sess.close()
        conn = chdb.connect(":memory:")
        total_rows = 0
        with conn.send_query("SELECT * FROM numbers(200000)") as stream:
            for chunk in stream:
                total_rows += chunk.rows_read()
        self.assertEqual(total_rows, 200000)
        conn.close()
        self.sess = session.Session(test_streaming_query_dir)

    def test_large_table(self):
        # Test querying extremely large dataset (1 billion rows)
        # with stable memory usage around 56MB
        total_rows = 0
        with self.sess.send_query("SELECT * FROM numbers(1073741824)", "CSVWITHNAMES") as stream:
            for chunk in stream:
                total_rows += chunk.rows_read()
        self.assertEqual(total_rows, 1073741824)

    def test_cancel_streaming_query(self):
        self.sess.query("CREATE DATABASE IF NOT EXISTS test")
        self.sess.query("USE test")
        self.sess.query("CREATE TABLE IF NOT EXISTS cancel_streaming_test (id Int64) ENGINE = MergeTree() ORDER BY id")
        self.sess.query("INSERT INTO cancel_streaming_test SELECT number FROM numbers(10000)")

        stream_result =  self.sess.send_query("SELECT * FROM cancel_streaming_test", "CSVWITHNAMES")
        stream_result.cancel()

        result = self.sess.query("SELECT * FROM cancel_streaming_test", "CSVWITHNAMES")
        self.assertEqual(result.rows_read(), 10000)

        stream_result = self.sess.send_query("SELECT * FROM cancel_streaming_test", "CSVWITHNAMES")
        chunks = list(stream_result)
        self.assertEqual(len(chunks), 1)

        result = self.sess.query("SELECT * FROM cancel_streaming_test", "CSVWITHNAMES")
        self.assertEqual(result.rows_read(), 10000)

        stream_result = self.sess.send_query("SELECT * FROM cancel_streaming_test", "CSVWITHNAMES")

    def streaming_worker1(self):
        global result_counter_streaming1
        query_count = 0
        while not stop_event.is_set():
            query_count += 1
            if query_count % 10 == 0:
                with self.assertRaises(Exception):
                    ret = self.sess.send_query("SELECT * FROM streaming_test2;SELECT * FROM streaming_test2", "CSVWITHNAMES")

            if query_count % 10 == 1:
                ret = self.sess.send_query("SELECT * FROM streaming_test2;", "CSVWITHNAMES")
                ret.cancel()

            stream = self.sess.send_query("SELECT * FROM streaming_test2", "CSVWITHNAMES")
            for chunk in stream:
                result_counter_streaming1 += chunk.rows_read()

    def streaming_worker2(self):
        global result_counter_streaming2
        query_count = 0
        while not stop_event.is_set():
            query_count += 1
            if query_count % 10 == 0:
                with self.assertRaises(Exception):
                    ret = self.sess.send_query("SELECT * FROM streaming_test2;SELECT * FROM streaming_test2", "CSVWITHNAMES")

            if query_count % 10 == 2:
                ret = self.sess.send_query("SELECT * FROM streaming_test2;", "CSVWITHNAMES")
                ret.cancel()

            stream = self.sess.send_query("SELECT * FROM streaming_test2", "CSVWITHNAMES")
            for chunk in stream:
                result_counter_streaming2 += chunk.rows_read()

    def normal_query_worker1(self):
        global result_counter_normal1
        query_count = 0
        while not stop_event.is_set():
            query_count += 1
            if query_count % 10 == 0:
                with self.assertRaises(Exception):
                    ret = self.sess.send_query("SELECT * FROM streaming_test2;SELECT * FROM streaming_test2", "CSVWITHNAMES")

            if query_count % 10 == 3:
                ret = self.sess.send_query("SELECT * FROM streaming_test2;", "CSVWITHNAMES")
                ret.cancel()

            result = self.sess.query("SELECT * FROM streaming_test2", "CSVWITHNAMES")
            result_counter_normal1 += result.rows_read()

    def normal_query_worker2(self):
        global result_counter_normal2
        query_count = 0
        while not stop_event.is_set():
            query_count += 1
            if query_count % 10 == 0:
                with self.assertRaises(Exception):
                    ret = self.sess.send_query("SELECT * FROM streaming_test2;SELECT * FROM streaming_test2", "CSVWITHNAMES")

            if query_count % 10 == 4:
                ret = self.sess.send_query("SELECT * FROM streaming_test2;", "CSVWITHNAMES")
                ret.cancel()

            result = self.sess.query("SELECT * FROM streaming_test2", "CSVWITHNAMES")
            result_counter_normal2 += result.rows_read()

    def normal_insert_worker(self):
        global insert_counter
        query_count = 0
        i = 500000
        while not stop_event.is_set():
            query_count += 1
            if query_count % 10 == 0:
                with self.assertRaises(Exception):
                    ret = self.sess.send_query("SELECT * FROM streaming_test2;SELECT * FROM streaming_test2", "CSVWITHNAMES")

            if query_count % 10 == 5:
                ret = self.sess.send_query("SELECT * FROM streaming_test2;", "CSVWITHNAMES")
                ret.cancel()

            self.sess.query(f"INSERT INTO streaming_test2 VALUES ({i})")
            insert_counter += 1
            i += 1

    def test_multi_thread_streaming_query(self):
        self.sess.query("CREATE DATABASE IF NOT EXISTS test")
        self.sess.query("USE test")
        self.sess.query("CREATE TABLE IF NOT EXISTS streaming_test2 (id Int64) ENGINE = MergeTree() ORDER BY id")
        self.sess.query("INSERT INTO streaming_test2 SELECT number FROM numbers(500000)")

        threads = [
            threading.Thread(target=self.streaming_worker1),
            threading.Thread(target=self.streaming_worker2),
            threading.Thread(target=self.normal_query_worker1),
            threading.Thread(target=self.normal_query_worker2),
            threading.Thread(target=self.normal_insert_worker)
        ]

        for t in threads:
            t.start()

        time.sleep(10)
        stop_event.set()

        for t in threads:
            t.join()

        print(f"Total inserted rows: {insert_counter}")
        print(f"Total normal selected rows1: {result_counter_normal1}")
        print(f"Total normal selected rows2: {result_counter_normal2}")
        print(f"Total streaming selected rows1: {result_counter_streaming1}")
        print(f"Total streaming selected rows2: {result_counter_streaming2}")

        self.assertGreater(result_counter_normal1, 500000)
        self.assertGreater(result_counter_normal2, 500000)
        self.assertGreater(result_counter_streaming1, 500000)
        self.assertGreater(result_counter_streaming2, 500000)

        final_count = self.sess.query("SELECT count() FROM streaming_test2", "CSV").data().strip()
        self.assertEqual(int(final_count), 500000 + insert_counter)

if __name__ == "__main__":
    shutil.rmtree(test_streaming_query_dir, ignore_errors=True)
    unittest.main()
