import atexit
import io
import os.path
import time
import unittest

import pandas as pd
from chdb.dataframe import Table, pandas_read_parquet
from utils import current_dir

# if hits_0.parquet is not available, download it:
# https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet
if not os.path.exists(os.path.join(current_dir, "hits_0.parquet")):
    import urllib.request

    opener = urllib.request.URLopener()
    opener.addheader(
        "User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
    opener.retrieve("https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet",
                    os.path.join(current_dir, "hits_0.parquet"))

# 122MB parquet file
hits_0 = os.path.join(current_dir, "hits_0.parquet")
sql = """SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID)
                        FROM __table__ GROUP BY RegionID ORDER BY c DESC LIMIT 10"""

expected_query_output = """   RegionID  SUM(AdvEngineID)       c  AVG(ResolutionWidth)  COUNTDistinct(UserID)
0       229             38044  426435           1612.787187                  27961
1         2             12801  148193           1593.870891                  10413
2       208              2673   30614           1490.615111                   3073
3         1              1802   28577           1623.851699                   1720
4        34               508   14329           1592.897201                   1428
5        47              1041   13661           1637.851914                    943
6       158                78   13294           1576.340605                   1110
7         7              1166   11679           1627.319034                    647
8        42               642   11547           1625.601022                    956
9       184                30   10157           1614.693807                    987"""

output = io.StringIO()
# run print at exit
atexit.register(lambda: print("\n" + output.getvalue()))

pd.set_option("display.max_columns", 10)
pd.set_option("display.width", 200)

class TestRunOnDf(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        return super().setUp()

    def test_run_parquet(self):
        pq_table = Table(parquet_path=hits_0)
        self.assertEqual((1000000, 105), pq_table.to_pandas().shape)
        pq_table.show()
        t = time.time()
        ret = pq_table.query(sql)
        print("Run on parquet file. Time cost:", time.time() - t, "s", file=output)
        print("size:", len(ret._parquet_memoryview.tobytes()), "bytes", file=output)
        ret.show()
        ret.flush_to_disk()
        ret.show()
        print("file path:", ret._temp_parquet_path, file=output)
        print("size:", os.path.getsize(ret._temp_parquet_path), "bytes", file=output)
        # copy temp file to ./
        import shutil
        shutil.copy(ret._temp_parquet_path, "./ret.parquet")
        self.assertEqual(expected_query_output, str(ret))
        pq_table.flush_to_disk()
        self.assertIsNotNone(pq_table._parquet_path)

    def test_run_parquet_buf(self):
        with open(hits_0, "rb") as f:
            parquet_buf = f.read()
        pq_table = Table(parquet_memoryview=memoryview(parquet_buf), use_memfd=True)
        self.assertEqual((1000000, 105), pq_table.to_pandas().shape)
        t = time.time()
        ret = pq_table.query(sql)
        print("Run on parquet buffer. Time cost:", time.time() - t, "s", file=output)
        self.assertEqual(expected_query_output, str(ret))
        pq_table.flush_to_disk()
        self.assertIsNone(pq_table._parquet_memoryview)
        self.assertIsNotNone(pq_table._temp_parquet_path)

    def test_run_arrow_table(self):
        import pyarrow.parquet as pq
        arrow_table = pq.read_table(hits_0)
        pq_table = Table(arrow_table=arrow_table, use_memfd=True)
        self.assertEqual((1000000, 105), pq_table.to_pandas().shape)
        t = time.time()
        ret = pq_table.query(sql)
        print("Run on arrow table. Time cost:", time.time() - t, "s", file=output)
        self.assertEqual(expected_query_output, str(ret))
        pq_table.flush_to_disk()
        self.assertIsNone(pq_table._arrow_table)
        self.assertIsNotNone(pq_table._temp_parquet_path)

    def test_run_df(self):
        import pandas as pd
        df = pd.read_parquet(hits_0)
        pq_table = Table(dataframe=df, use_memfd=True)
        self.assertEqual((1000000, 105), pq_table.to_pandas().shape)
        t = time.time()
        ret = pq_table.query(sql)
        print("Run on dataframe. Time cost:", time.time() - t, "s", file=output)
        self.assertEqual(expected_query_output, str(ret))
        pq_table.flush_to_disk()
        self.assertIsNone(pq_table._dataframe)
        self.assertIsNotNone(pq_table._temp_parquet_path)

    def test_run_df_arrow(self):
        import pandas as pd
        df = pandas_read_parquet(hits_0)
        pq_table = Table(dataframe=df, use_memfd=True)
        self.assertEqual((1000000, 105), pq_table.to_pandas().shape)
        t = time.time()
        ret = pq_table.query(sql)
        print("Run on dataframe arrow. Time cost:", time.time() - t, "s", file=output)
        self.assertEqual(expected_query_output, str(ret))
        pq_table.flush_to_disk()
        self.assertIsNone(pq_table._dataframe)
        self.assertIsNotNone(pq_table._temp_parquet_path)

    def test_run_temp_file(self):
        import tempfile
        import shutil
        temp_dir = tempfile.mkdtemp()
        try:
            temp_file = os.path.join(temp_dir, "hits_0.parquet")
            shutil.copyfile(hits_0, temp_file)
            pq_table = Table(temp_parquet_path=temp_file)
            self.assertEqual((1000000, 105), pq_table.to_pandas().shape)
            t = time.time()
            ret = pq_table.query(sql)
            print("Run on temp file. Time cost:", time.time() - t, "s", file=output)
            self.assertEqual(expected_query_output, str(ret))
            # temp file should be deleted after del pq_table
            del pq_table
            print(temp_file)
            self.assertFalse(os.path.exists(temp_file))
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
