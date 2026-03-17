#!python3

import unittest
from chdb.udf import chdb_udf
from chdb.session import Session
from chdb import query, sql


@chdb_udf()
def sum_udf(lhs, rhs):
    return int(lhs) + int(rhs)


@chdb_udf(return_type="Int32")
def mul_udf(lhs, rhs):
    return int(lhs) * int(rhs)


class TestUDF(unittest.TestCase):
    def test_sum_udf(self):
        ret = query("select sum_udf(12,22)", "Debug")
        self.assertEqual(str(ret), '"34"\n')

    def test_return_Int32(self):
        ret = query("select mul_udf(12,22) + 1", "Debug")
        self.assertEqual(str(ret), "265\n")

    def test_define_in_function(self):
        @chdb_udf()
        def sum_udf2(lhs, rhs):
            return int(lhs) + int(rhs)

        # sql is a alias for query
        ret = sql("select sum_udf2(11, 22)", "Debug")
        self.assertEqual(str(ret), '"33"\n')


class TestUDFinSession(unittest.TestCase):
    def test_sum_udf(self):
        with Session(":memory:?verbose&log-level=test") as session:
            ret = session.query("select sum_udf(12,22)")
            self.assertEqual(str(ret), '"34"\n')

    def test_return_Int32(self):
        with Session("file::memory:") as session:
            ret = session.query("select mul_udf(12,22) + 1")
            self.assertEqual(str(ret), "265\n")

    def test_define_in_function(self):
        @chdb_udf()
        def sum_udf2(lhs, rhs):
            return int(lhs) + int(rhs)

        with Session() as session:
            # sql is a alias for query
            ret = session.sql("select sum_udf2(11, 22)", "CSV")
            self.assertEqual(str(ret), '"33"\n')

if __name__ == "__main__":
    unittest.main()
