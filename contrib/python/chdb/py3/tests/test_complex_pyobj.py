import unittest
import pandas as pd
import chdb

df_with_na = pd.DataFrame(
    {
        "A": [1, 2, 3, pd.NA],
        "B": [4.0, 5.0, 6.0, pd.NA],
        "C": [True, False, True, pd.NA],
        "D": ["a", "b", "c", pd.NA],
        "E": [pd.NA, pd.NA, pd.NA, pd.NA],
        "F": [[1, 2], [3, 4], [5, 6], pd.NA],
        "G": [{"a": 1, "b": 2}, {"c": 3, "d": 4}, {"e": 5, "f": 6}, pd.NA],
    }
)

df_without_na = pd.DataFrame(
    {
        "A": [1, 2, 3, 4],
        "B": [4.0, 5.0, 6.0, 7.0],
        "C": [True, False, True, False],
        "D": ["a", "b", "c", "d"],
        "E": ["a", "b", "c", "d"],
        "F": [[1, 2], [3, 4], [5, 6], [7, 8]],
        "G": [{"a": 1, "b": 2}, {"c": 3, "d": 4}, {"e": 5, "f": 6}, {"g": 7, "h": 8}],
    }
)


class TestComplexPyObj(unittest.TestCase):
    def test_df_with_na(self):
        ret = chdb.query(
            """
            select * from Python(df_with_na) limit 10
            """,
            "dataframe",
        )
        self.assertEqual(ret.dtypes["A"], "object")
        self.assertEqual(ret.dtypes["B"], "object")
        self.assertEqual(ret.dtypes["C"], "object")
        self.assertEqual(ret.dtypes["D"], "object")
        self.assertEqual(ret.dtypes["E"], "object")
        self.assertEqual(ret.dtypes["F"], "object")
        self.assertEqual(ret.dtypes["G"], "object")
        self.assertEqual(
            str(ret),
            """      A     B      C     D     E       F                 G
0     1   4.0   True     a  <NA>  [1, 2]  {"a": 1, "b": 2}
1     2   5.0  False     b  <NA>  [3, 4]  {"c": 3, "d": 4}
2     3   6.0   True     c  <NA>  [5, 6]  {"e": 5, "f": 6}
3  <NA>  <NA>   <NA>  <NA>  <NA>    <NA>              <NA>""",
        )

    def test_df_without_na(self):
        ret = chdb.query(
            """
            select * from Python(df_without_na) limit 10
            """,
            "dataframe",
        )
        self.assertEqual(ret.dtypes["A"], "int64")
        self.assertEqual(ret.dtypes["B"], "float64")
        self.assertEqual(ret.dtypes["C"], "uint8")
        self.assertEqual(ret.dtypes["D"], "object")
        self.assertEqual(ret.dtypes["E"], "object")
        self.assertEqual(ret.dtypes["F"], "object")
        self.assertEqual(ret.dtypes["G"], "object")
        self.assertEqual(
            str(ret),
            """   A    B  C  D  E       F                 G
0  1  4.0  1  a  a  [1, 2]  {"a": 1, "b": 2}
1  2  5.0  0  b  b  [3, 4]  {"c": 3, "d": 4}
2  3  6.0  1  c  c  [5, 6]  {"e": 5, "f": 6}
3  4  7.0  0  d  d  [7, 8]  {"g": 7, "h": 8}""",
        )


if __name__ == "__main__":
    unittest.main()
