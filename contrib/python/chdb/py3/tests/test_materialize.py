#!/usr/bin/env python3

import unittest
from chdb import session


class TestMaterialize(unittest.TestCase):
    def test_materialize(self):
        with session.Session() as sess:
            ret = sess.query("CREATE DATABASE IF NOT EXISTS db_xxx ENGINE = Atomic")
            self.assertFalse(ret.has_error())
            ret = sess.query("USE db_xxx")
            self.assertFalse(ret.has_error())
            ret = sess.query(
                """
            CREATE TABLE download (
            when DateTime,
            userid UInt32,
            bytes Float32
            ) ENGINE=MergeTree
            PARTITION BY toYYYYMM(when)
            ORDER BY (userid, when)"""
            )
            self.assertFalse(ret.has_error())
            sess.query(
                """
            INSERT INTO download
            SELECT
                now() + number * 60 as when,
                25,
                rand() % 100000000
            FROM system.numbers
            LIMIT 5000"""
            )
            ret = sess.query(
                """
            SELECT
            toStartOfDay(when) AS day,
            userid,
            count() as downloads,
            sum(bytes) AS bytes
            FROM download
            GROUP BY userid, day
            ORDER BY userid, day"""
            )
            print("Result from agg:", ret)

            sess.query(
                """CREATE MATERIALIZED VIEW download_daily_mv
            ENGINE = SummingMergeTree
            PARTITION BY toYYYYMM(day) ORDER BY (userid, day)
            POPULATE
            AS SELECT
            toStartOfDay(when) AS day,
            userid,
            count() as downloads,
            sum(bytes) AS bytes
            FROM download
            GROUP BY userid, day"""
            )
            ret1 = sess.query(
                """SELECT * FROM download_daily_mv
            ORDER BY day, userid 
            LIMIT 5"""
            )
            print("Result from mv:", ret1)
            print("Show result:")
            ret1.show()
            self.assertEqual(str(ret), str(ret1))

            sess.query(
                """
            INSERT INTO download
                    SELECT
                now() + number * 60 as when,
                25,
                rand() % 100000000
            FROM system.numbers
            LIMIT 5000"""
            )
            ret2 = sess.query(
                """SELECT * FROM download_daily_mv
            ORDER BY day, userid
            LIMIT 5"""
            )
            print("Result from mv after insert:", ret2)

            self.assertNotEqual(str(ret1), str(ret2))


if __name__ == "__main__":
    unittest.main()
