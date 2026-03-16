#!python3

import unittest
import pandas as pd
from chdb import dataframe as cdf


class TestJoinDf(unittest.TestCase):
    def test_1df(self):
        df1 = pd.DataFrame({"a": [1, 2, 3], "b": ["one", "two", "three"]})
        cdf1 = cdf.Table(dataframe=df1)
        ret1 = cdf.query(sql="select * from __tbl1__", tbl1=cdf1)
        self.assertEqual(str(ret1), str(df1))
        self.assertEqual(ret1.rows_read(), 3)
        self.assertEqual(ret1.bytes_read(), 68)
        self.assertGreater(ret1.elapsed(), 0.000001)

    def test_2df(self):
        df1 = pd.DataFrame({"a": [1, 2, 3], "b": ["one", "two", "three"]})
        df2 = pd.DataFrame({"c": [1, 2, 3], "d": ["①", "②", "③"]})
        ret_tbl = cdf.query(
            sql="select * from __tbl1__ t1 join __tbl2__ t2 on t1.a = t2.c",
            tbl1=df1,
            tbl2=df2,
        )
        self.assertEqual(
            str(ret_tbl),
            str(
                pd.DataFrame(
                    {
                        "a": [1, 2, 3],
                        "b": ["one", "two", "three"],
                        "c": [1, 2, 3],
                        "d": ["①", "②", "③"],
                    }
                )
            ),
        )

        ret_tbl2 = ret_tbl.query(
            "select b, a+c s from __table__ order by s"
        )
        self.assertEqual(
            str(ret_tbl2),
            str(pd.DataFrame({"b": ["one", "two", "three"], "s": [2, 4, 6]})),
        )
        self.assertEqual(ret_tbl2.rows_read(), 3)
        self.assertEqual(ret_tbl2.bytes_read(), 68)
        self.assertGreater(ret_tbl2.elapsed(), 0.000001)


if __name__ == "__main__":
    unittest.main()
