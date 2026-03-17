#!python3

import unittest
import shutil
import psutil
from chdb import session


test_state_dir = ".state_tmp_auxten_usedb_"
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

        sess.query("CREATE DATABASE IF NOT EXISTS db_xxx ENGINE = Atomic", "CSV")
        ret = sess.query("SHOW DATABASES", "CSV")
        self.assertIn("db_xxx", str(ret))

        sess.query("CREATE TABLE IF NOT EXISTS db_xxx.log_table_xxx (x UInt8) ENGINE = Log;")
        sess.query("INSERT INTO db_xxx.log_table_xxx VALUES (1), (2), (3), (4);")

        ret = sess.query("USE db_xxx; SELECT * FROM log_table_xxx", "Debug")
        self.assertEqual(str(ret), "1\n2\n3\n4\n")

        sess.query("USE db_xxx")
        ret = sess.query("SELECT * FROM log_table_xxx", "Debug")
        self.assertEqual(str(ret), "1\n2\n3\n4\n")
        sess.close()


if __name__ == '__main__':
    unittest.main()
