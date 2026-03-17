#!python3

import time
import shutil
import psutil
import unittest
from chdb import session
import chdb


test_state_dir = ".state_tmp_auxten_test_stateful"
current_process = psutil.Process()
check_thread_count = False


class TestStateful(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(test_state_dir, ignore_errors=True)
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(test_state_dir, ignore_errors=True)
        return super().tearDown()

    def test_path(self):
        sess = session.Session(test_state_dir)
        sess.query("CREATE FUNCTION chdb_xxx AS () -> '0.12.0'", "CSV")
        ret = sess.query("SELECT chdb_xxx()", "CSV")
        self.assertEqual(str(ret), '"0.12.0"\n')

        sess.query("CREATE DATABASE IF NOT EXISTS db_xxx ENGINE = Atomic", "CSV")
        ret = sess.query("SHOW DATABASES", "CSV")
        self.assertIn("db_xxx", str(ret))

        sess.query(
            "CREATE TABLE IF NOT EXISTS db_xxx.log_table_xxx (x UInt8) ENGINE = Log;"
        )
        sess.query("INSERT INTO db_xxx.log_table_xxx VALUES (1), (2), (3), (4);")

        sess.query(
            "CREATE VIEW db_xxx.view_xxx AS SELECT * FROM db_xxx.log_table_xxx LIMIT 2;"
        )
        ret = sess.query("SELECT * FROM db_xxx.view_xxx", "CSV")
        self.assertEqual(str(ret), "1\n2\n")

        sess.close()

        sess = session.Session(test_state_dir)
        ret = sess.query("SELECT chdb_xxx()", "CSV")
        self.assertEqual(str(ret), '"0.12.0"\n')

        ret = sess.query("SHOW DATABASES", "CSV")
        self.assertIn("db_xxx", str(ret))

        ret = sess.query("SELECT * FROM db_xxx.log_table_xxx", "CSV")
        self.assertEqual(str(ret), "1\n2\n3\n4\n")
        ret.show()
        sess.close()

        # reuse session
        sess2 = session.Session(test_state_dir)

        ret = sess2.query("SELECT chdb_xxx()", "CSV")
        self.assertEqual(str(ret), '"0.12.0"\n')

        # remove session dir
        sess2.cleanup()

        with self.assertRaises(Exception):
            ret = sess2.query("SELECT chdb_xxx()", "CSV")

    def test_mergetree(self):
        sess = session.Session()
        sess.query("CREATE DATABASE IF NOT EXISTS db_xxx_merge ENGINE = Atomic;", "CSV")
        sess.query(
            "CREATE TABLE IF NOT EXISTS db_xxx_merge.log_table_xxx (x String, y Int) ENGINE = MergeTree ORDER BY x;"
        )
        # insert 1000000 random rows
        sess.query(
            "INSERT INTO db_xxx_merge.log_table_xxx (x, y) SELECT toString(rand()), rand() FROM numbers(1000000);"
        )
        sess.query("Optimize TABLE db_xxx_merge.log_table_xxx;")
        ret = sess.query("SELECT count(*) FROM db_xxx_merge.log_table_xxx;")
        self.assertEqual(str(ret), "1000000\n")
        sess.cleanup()

    def test_tmp(self):
        sess = session.Session()
        sess.query("CREATE FUNCTION chdb_xxx AS () -> '0.12.0'", "CSV")
        ret = sess.query("SELECT chdb_xxx()", "CSV")
        self.assertEqual(str(ret), '"0.12.0"\n')
        sess.cleanup()

    def test_two_sessions(self):
        sess1 = None
        sess2 = None
        try:
            sess1 = session.Session()
            with self.assertWarns(Warning):
                sess2 = session.Session()
            self.assertIsNone(sess1._conn)
        finally:
            if sess1:
                sess1.cleanup()
            if sess2:
                sess2.cleanup()

    def test_context_mgr(self):
        with session.Session() as sess:
            sess.query("CREATE FUNCTION chdb_xxx_mgr AS () -> '0.12.0_mgr'", "CSV")
            ret = sess.query("SELECT chdb_xxx_mgr()", "CSV")
            self.assertEqual(str(ret), '"0.12.0_mgr"\n')

        with session.Session() as sess:
            with self.assertRaises(Exception):
                ret = sess.query("SELECT chdb_xxx_notexist()", "CSV")

    def test_query_fmt(self):
        with session.Session() as sess:
            # Dataframe result
            ret = sess.query("SELECT 1 AS x", "DataFrame")
            self.assertEqual(ret.x[0], 1)
            # ArrowTable
            ret = sess.query("SELECT 1 AS x", "ArrowTable")
            self.assertEqual(
                str(ret),
                """pyarrow.Table
x: uint8 not null
----
x: [[1]]""",
            )

    # def test_zfree_thread_count(self):
    #     time.sleep(3)
    #     thread_count = current_process.num_threads()
    #     print("Number of threads using psutil library: ", thread_count)
    #     if check_thread_count:
    #         self.assertEqual(thread_count, 1)


if __name__ == "__main__":
    shutil.rmtree(test_state_dir, ignore_errors=True)
    check_thread_count = True
    unittest.main()
