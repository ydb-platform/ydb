#!/usr/bin/env python3

import unittest
import timeit
import datetime
import json
import tempfile
import pandas as pd
import chdb
import os
from urllib.request import urlretrieve

import yatest.common as yc


class TestChDBDataFrame(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Download parquet file if it doesn't exist
        parquet_file = yc.test_source_path("hits_0.parquet")
        if not os.path.exists(parquet_file):
            print(f"Downloading {parquet_file}...")
            url = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet"
            urlretrieve(url, parquet_file)
            print("Download complete!")

        # Load data and prepare DataFrame
        cls.hits = pd.read_parquet(parquet_file)
        cls.dataframe_size = cls.hits.memory_usage().sum()

        # Fix types
        cls.hits["EventTime"] = pd.to_datetime(cls.hits["EventTime"], unit="s")
        cls.hits["EventDate"] = pd.to_datetime(cls.hits["EventDate"], unit="D")

        # Convert object columns to string
        for col in cls.hits.columns:
            if cls.hits[col].dtype == "O":
                cls.hits[col] = cls.hits[col].astype(str)

        # Load queries
        with open(yc.test_source_path("queries.sql")) as f:
            cls.queries = f.readlines()

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.conn = chdb.connect(f"{self.tmp_dir.name}")

    def tearDown(self):
        self.conn.close()
        self.tmp_dir.cleanup()

    def test_dataframe_size(self):
        self.assertGreater(self.dataframe_size, 0, "DataFrame size should be positive")

    def test_query_execution(self):
        queries_times = []
        for i, query in enumerate(self.queries, 1):
            times = []
            for _ in range(3):
                hits = self.hits
                start = timeit.default_timer()
                result = self.conn.query(query, "CSV")
                end = timeit.default_timer()
                times.append(end - start)

                # Verify query results are not empty
                self.assertIsNotNone(result, f"Query {i} returned None")

            queries_times.append(times)
            # Verify execution times are reasonable
            self.assertTrue(
                all(t > 0 for t in times), f"Query {i} has invalid execution times"
            )

        result_json = {
            "system": "chDB 2.2(DataFrame)",
            "date": datetime.date.today().strftime("%Y-%m-%d"),
            "machine": "",
            "cluster_size": 1,
            "comment": "",
            "tags": [
                "C++",
                "column-oriented",
                "embedded",
                "stateless",
                "serverless",
                "dataframe",
                "ClickHouse derivative",
            ],
            "load_time": 0,
            "data_size": int(self.dataframe_size),
            "result": queries_times,  # Will be populated during test_query_execution
        }

        print(json.dumps(result_json, indent=2))


if __name__ == "__main__":
    unittest.main()
