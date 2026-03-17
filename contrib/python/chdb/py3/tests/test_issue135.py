#!python3

import shutil
import unittest
from chdb import session as chs


test_dir = ".state_tmp_auxten_issue135"


class TestReplaceTable(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)
        return super().tearDown()

    def test_replace_table(self):
        with chs.Session(test_dir) as sess:
            sess.query("CREATE DATABASE IF NOT EXISTS a;")
            sess.query(
                "CREATE OR REPLACE TABLE a.test (id UInt64, updated_at DateTime DEFAULT now(),updated_at_date Date DEFAULT toDate(updated_at)) "
                "ENGINE = MergeTree ORDER BY id;"
            )
            sess.query("INSERT INTO a.test (id) Values (1);")
            ret = sess.query("SELECT * FROM a.test;", "CSV")
            # something like 1,"2023-11-20 21:59:57","2023-11-20"
            parts = str(ret).split(",")
            self.assertEqual(len(parts), 3)
            self.assertEqual(parts[0], "1")
            # regex for datetime
            self.assertRegex(parts[1], r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
            # regex for date
            self.assertRegex(parts[2], r"\d{4}-\d{2}-\d{2}")

            # replace table
            sess.query(
                "CREATE OR REPLACE TABLE a.test (id UInt64, updated_at DateTime DEFAULT now(),updated_at_date Date DEFAULT toDate(updated_at)) "
                "ENGINE = MergeTree ORDER BY id;"
            )
            ret = sess.query("SELECT * FROM a.test;", "CSV")
            self.assertEqual(str(ret), "")


if __name__ == "__main__":
    unittest.main()
