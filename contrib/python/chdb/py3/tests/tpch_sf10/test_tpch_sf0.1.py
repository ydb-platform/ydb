#!/usr/bin/env python3

import time
import unittest
import os
from sqls import get_queries
import chdb

current_dir = os.path.dirname(os.path.abspath(__file__))

# Set the path to the TPCH data directory
small_data_dir = os.path.join(current_dir, "../data/tpchsf-small/")

queries = get_queries(small_data_dir)


class TestTPCH_SF10(unittest.TestCase):
    def test_tpch_sf10_original_sql(self):
        # Run each query and record the time of each query cost
        for i, query in enumerate(queries):
            # Start the timer
            start_time = time.time()

            # Run the query
            ret = chdb.query(query, "CSV")

            # Calculate the query execution time
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Query {i+1} execution time: {execution_time} seconds")
            # max 10 lines of str(ret)
            lines = str(ret).split("\n")
            print("\n".join(lines[:10]))
            print("#############################################")


if __name__ == "__main__":
    unittest.main()
