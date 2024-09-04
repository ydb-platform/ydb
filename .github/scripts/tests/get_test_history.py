#!/usr/bin/env python3

import configparser
import datetime
import os
import time
import ydb


dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]


def get_test_history(test_names_array, last_n_runs_of_test_amount, build_type):
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        return {}
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    results = {}
    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        batch_size = 500
        start_time = time.time()
        for start in range(0, len(test_names_array), batch_size):
            test_names_batch = test_names_array[start:start + batch_size]
            history_query = f"""
        PRAGMA AnsiInForEmptyOrNullableItemsCollections;
        DECLARE $test_names AS List<Utf8>;
        DECLARE $rn_max AS Int32;
        DECLARE $build_type AS Utf8;
        
        $test_names=[{','.join("'{0}'".format(x) for x in test_names_batch)}];
        $rn_max = {last_n_runs_of_test_amount};
        $build_type = '{build_type}';

        $tests=(
            SELECT 
                suite_folder ||'/' || test_name as full_name,test_name,build_type, commit, branch, run_timestamp, status, status_description,
                ROW_NUMBER() OVER (PARTITION BY test_name ORDER BY run_timestamp DESC) AS rn
            FROM 
                `test_results/test_runs_column`
            where (job_name ='Nightly-run' or job_name like 'Postcommit%') and
            build_type = $build_type and
            suite_folder ||'/' || test_name in  $test_names
            and status != 'skipped'
        );

        select full_name,test_name,build_type, commit, branch, run_timestamp, status, status_description,rn
        from  $tests
        WHERE rn <= $rn_max
        ORDER BY test_name, run_timestamp;  
    """
            query = ydb.ScanQuery(history_query, {})
            it = driver.table_client.scan_query(query)
            query_result = []

            while True:
                try:
                    result = next(it)
                    query_result = query_result + result.result_set.rows
                except StopIteration:
                    break

            for row in query_result:
                if not row["full_name"].decode("utf-8") in results:
                    results[row["full_name"].decode("utf-8")] = {}

                results[row["full_name"].decode("utf-8")][row["run_timestamp"]] = {
                    "status": row["status"],
                    "commit": row["commit"],
                    "datetime": datetime.datetime.fromtimestamp(int(row["run_timestamp"] / 1000000)).strftime("%H:%m %B %d %Y"),
                    "status_description": row["status_description"],
                }
        end_time = time.time()
        print(
            f'durations of getting history for {len(test_names_array)} tests :{end_time-start_time} sec')
        return results


if __name__ == "__main__":
    get_test_history(test_names_array, last_n_runs_of_test_amount, build_type)
