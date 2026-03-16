#!python3

import os
import unittest
import zipfile
import urllib.request

import pandas as pd
import chdb


class TestIssue251(unittest.TestCase):
    def setUp(self):
        # if /tmp/issue251/artifacts/create_final_community_reports.parquet not exists,
        # download https://github.com/user-attachments/files/16361689/parquet-test-data.zip
        # and unzip it to /tmp/issue251/
        if not os.path.exists(
            "/tmp/issue251/artifacts/create_final_community_reports.parquet"
        ):
            print("Downloading parquet-test-data.zip")

            url = "https://github.com/user-attachments/files/16361689/parquet-test-data.zip"
            os.makedirs("/tmp/issue251/", exist_ok=True)
            urllib.request.urlretrieve(url, "/tmp/issue251/parquet-test-data.zip")
            with zipfile.ZipFile("/tmp/issue251/parquet-test-data.zip", "r") as zip_ref:
                zip_ref.extractall("/tmp/issue251/")

    def test_issue251(self):
        df = pd.read_parquet(
            "/tmp/issue251/artifacts/create_final_community_reports.parquet",
            columns=[
                "id",
                "community",
                "level",
                "title",
                "summary",
                "findings",
                "rank",
                "rank_explanation",
            ],
        )

        # make pandas show all columns
        pd.set_option("display.max_columns", None)
        print(df.head(2))
        print(df.dtypes)
        try:
            chdb.query("FROM Python(df) SELECT * LIMIT 10")
        except Exception as e:
            self.assertTrue(
                "Unsupported Python object type numpy.ndarray" in str(e)
            )


if __name__ == "__main__":
    unittest.main()
