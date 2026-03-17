#!python3

import os
import time
import chdb
import chdb.dataframe as cdf
import chdb.session as chs
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

# from pyarrow.interchange import from_dataframe
from utils import current_dir

# # if hits_0.parquet is not available, download it:
# # https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet
# if not os.path.exists(os.path.join(current_dir, "hits_0.parquet")):
#     opener = urllib.request.URLopener()
#     opener.addheader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
#     opener.retrieve("https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet",
#                     os.path.join(current_dir, "hits_0.parquet"))

# 122MB parquet file
# hits_0 = os.path.join(current_dir, "hits_0.parquet")

# 14GB parquet file
# hits_0 = os.path.join(current_dir, "hits.parquet")

# 1.3G parquet file
hits_0 = os.path.join(current_dir, "hits1.parquet")

# sql = """SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID)
#                         FROM __table__ GROUP BY RegionID ORDER BY c DESC LIMIT 10"""

# sql = "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;"

sql = "SELECT COUNT(DISTINCT UserID) FROM hits;"

# sql = "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;"

t = time.time()
# read parquet file into memory
with open(hits_0, "rb") as f:
    data = f.read()
print("Read parquet file into memory. Time cost:", time.time() - t, "s")
print("Parquet file size:", len(data), "bytes")
del data

# read parquet file as old pandas dataframe
t = time.time()
df_old = pd.read_parquet(hits_0)
print("Read parquet file as old pandas dataframe. Time cost:", time.time() - t, "s")
print("Dataframe size:", df_old.memory_usage().sum(), "bytes")

hits = df_old
# # print(hits["EventTime"][0:10])
# hits["EventTime"] = pd.to_datetime(hits["EventTime"], unit="s")
# # print(hits["EventTime"][0:10])

# hits["EventDate"] = pd.to_datetime(hits["EventDate"], unit="D")
# # print(hits["EventDate"][0:10])

# # fix all object columns to string
# for col in hits.columns:
#     if hits[col].dtype == "O":
#         # hits[col] = hits[col].astype('string')
#         hits[col] = hits[col].astype(str)

hits["Referer"] = hits["Referer"].astype(str)

# title = hits["Title"]
# title.values.data

# hits.dtypes

# # read parquet file as pandas dataframe
# t = time.time()
# # df = pd.read_parquet(hits_0, engine="pyarrow", dtype_backend="pyarrow")
# df = pd.read_parquet(hits_0, engine="pyarrow", dtype_backend="pyarrow")
# print("Read parquet file as pandas dataframe(arrow). Time cost:", time.time() - t, "s")
# print("Dataframe size:", df.memory_usage().sum(), "bytes")

# # convert dataframe to numpy array
# t = time.time()
# df_npy = df_old["RegionID"].to_numpy()
# print("Convert old dataframe to numpy array. Time cost:", time.time() - t, "s")

# t = time.time()
# df_npy = df["RegionID"].to_numpy()
# print("Convert dataframe(arrow) to numpy array. Time cost:", time.time() - t, "s")

# # pandas to parquet
# t = time.time()
# df_old.to_parquet("/dev/null")
# print("Convert old dataframe to parquet. Time cost:", time.time() - t, "s")

# # read parquet file with parquet output
# t = time.time()
# # ret = chdb.query(sql.replace("__table__", f"file('{hits_0}')"), "Parquet")
# ret = chdb.query(f"SELECT * FROM file('{hits_0}')", "Native")
# print("Read parquet file with native output. Time cost:", time.time() - t, "s")
# print("Native file size:", len(ret), "bytes")

# t = time.time()
# arrow_table = pq.read_table(hits_0)
# print("Read parquet file as arrow table. Time cost:", time.time() - t, "s")
# print("Arrow table size:", arrow_table.nbytes, "bytes")

# t = time.time()
# # serialize the arrow table to the buffer
# buf1 = pa.BufferOutputStream()
# pa.parquet.write_table(arrow_table, buf1)
# buff1 = buf1.getvalue()
# print("Convert arrow table to arrow buffer. Time cost:", time.time() - t, "s")
# print("Arrow buffer size:", len(buff1), "bytes")

# t = time.time()
# # serialize the dataframe to the buffer
# buf = pa.ipc.serialize_pandas(df)
# print("Convert dataframe to arrow buffer. Time cost:", time.time() - t, "s")
# print("Arrow buffer size:", len(buf), "bytes")

# t = time.time()
# # serialize the old dataframe to the buffer
# buf_old = pa.ipc.serialize_pandas(df_old)
# print("Convert old dataframe to arrow buffer. Time cost:", time.time() - t, "s")
# print("Arrow buffer size:", len(buf_old), "bytes")

# t = time.time()
# # df to numpy array
# np_array = df.to_numpy()
# print("Convert dataframe to numpy array. Time cost:", time.time() - t, "s")
# print("Numpy array size:", np_array.nbytes, "bytes")

# t = time.time()
# # df_old to numpy array
# np_array_old = df_old.to_numpy()
# print("Convert old dataframe to numpy array. Time cost:", time.time() - t, "s")
# print("Numpy array size:", np_array_old.nbytes, "bytes")


t = time.time()
# query the old dataframe with duckdb
con = duckdb.connect()
ret = con.execute(
    #     """
    # SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID)
    # FROM df_old GROUP BY RegionID ORDER BY c DESC LIMIT 10
    # """
    # "SELECT COUNT(DISTINCT Title) FROM df_old;"
    sql
).fetchdf()
print("Run duckdb on dataframe. Time cost:", time.time() - t, "s")
print(ret)

# t = time.time()
# # query the dataframe with duckdb
# con = duckdb.connect()
# ret = con.execute(
#     """
# SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID)
# FROM df GROUP BY RegionID ORDER BY c DESC LIMIT 10
# """
# )
# ret.fetchdf()
# print("Run duckdb on dataframe(arrow). Time cost:", time.time() - t, "s")


# # t = time.time()
# # # serialize dataframe to arrow buffer with from_dataframe
# # buf = from_dataframe(df)
# # print("Convert dataframe to arrow buffer with from_dataframe. Time cost:", time.time() - t, "s")
# # print("size:", len(buf), "bytes")

# t = time.time()
# pq_table = cdf.Table(arrow_table=arrow_table)
# ret = pq_table.query(sql)
# print("Run on arrow table. Time cost:", time.time() - t, "s")

# t = time.time()
# pq_table = cdf.Table(dataframe=df)
# ret = pq_table.query(sql)
# print("Run with legacy chDB on dataframe(arrow). Time cost:", time.time() - t, "s")

# t = time.time()
# pq_table = cdf.Table(dataframe=df_old)
# ret = pq_table.query(sql)
# print("Run with legacy chDB on dataframe. Time cost:", time.time() - t, "s")

# t = time.time()
# pq_table = cdf.Table(parquet_path=hits_0)
# ret = pq_table.query(sql)
# print("Run on parquet file. Time cost:", time.time() - t, "s")

# show df_old full schema
# pd.set_option("display.max_rows", 200)
# print("df_old schema:")
# print(df_old.dtypes)


class myReader(chdb.PyReader):
    def __init__(self, data):
        self.data = data
        self.cursor = 0
        super().__init__(data)

    def read(self, col_names, count):
        # print("read", col_names, count)
        # get the columns from the data with col_names
        block = [self.data[col] for col in col_names]
        # print("columns and rows", len(block), len(block[0]))
        # get the data from the cursor to cursor + count
        block = [col[self.cursor : self.cursor + count] for col in block]
        # print("columns and rows", len(block), len(block[0]))
        # move the cursor
        self.cursor += block[0].shape[0]
        return block


reader = myReader(df_old)

# sess.query("set aggregation_memory_efficient_merge_threads=2;")

sql = sql.replace("STRLEN", "length")

def bench_chdb(i):
    if i == 0:
        format = "Debug"
    else:
        format = "DataFrame"
    ret = sess.query(
        # """ SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID)
        #                     FROM Python(reader) GROUP BY RegionID ORDER BY c DESC LIMIT 10""",
        # "SELECT COUNT(DISTINCT Title) FROM Python(reader);",
        "set aggregation_memory_efficient_merge_threads=3;"
        + sql.replace("hits", "Python(hits)"),
        format,
    )
    return ret

sess = chs.Session()

# run 5 times, remove the fastest and slowest, then calculate the average
times = []
for i in range(5):
    t = time.time()
    ret = bench_chdb(i)
    times.append(time.time() - t)
    print(ret)
times.remove(max(times))
times.remove(min(times))
print("Run with new chDB on dataframe. Time cost:", sum(times) / len(times), "s")

sess.cleanup()
# t = time.time()
# df_arr_reader = myReader(df)
# ret = chdb.query(
#     """SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID)
#                         FROM Python(df_arr_reader) GROUP BY RegionID ORDER BY c DESC LIMIT 10""",
#     "CSV",
# )
# # print(ret)
# print("Run with new chDB on dataframe(arrow). Time cost:", time.time() - t, "s")
