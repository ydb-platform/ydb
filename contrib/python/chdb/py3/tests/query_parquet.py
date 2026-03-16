#!python3

import os
import time
import chdb
from chdb import dataframe as cdf
from chdb.dataframe import Table, pandas_read_parquet

sql = """SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID)
                        FROM `hits.arrow` GROUP BY RegionID ORDER BY c DESC LIMIT 10"""
t = time.time()
# chdb.query(
#     """
# SET input_format_csv_use_best_effort_in_schema_inference = 0;
# SET input_format_csv_skip_first_lines = 1;
# SET input_format_csv_allow_variable_number_of_columns = 1 ;"""
# )

chdb.query(
    """SELECT 123 SETTINGS input_format_csv_use_best_effort_in_schema_inference = 1;"""
)
ret = chdb.query(sql)
print("Run on parquet file. Time cost:", time.time() - t, "s")
print("size:", len(ret), "bytes")
print(ret)
