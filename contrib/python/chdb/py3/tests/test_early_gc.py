#!python3

import unittest
import gc
import chdb


class TestEarlyGC(unittest.TestCase):
    def test_gc(self):
        print("query 1 started")
        chdb.query("SELECT 1", "CSV")
        gc.collect()
        print("query 1 finished")
        chdb.query("CREATE FUNCTION chdb_xxx AS () -> '0.12.0'", "Debug")
        gc.collect()
        print("query 2 finished")
        ret = chdb.query("CREATE FUNCTION chdb_xxx2 AS () -> '0.12.0'", "Debug")
        print(ret)


if __name__ == "__main__":
    unittest.main()
