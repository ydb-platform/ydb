#!/usr/bin/env python3

import configparser
import os
import ydb
import datetime


dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)

build_preset = os.environ.get("build_preset")
branch = os.environ.get("branch_to_compare")

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]


def get_test_history():
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]="/home/kirrysin/fork/ydb/.github/scripts/my-robot-key.json"
        #return 0
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    test_names_array = [
        'ydb/core/kqp/ut/scheme/KqpScheme.AlterTableAddExplicitSyncVectorKMeansTreeIndex',
        'ydb/library/yql/tests/sql/hybrid_file/part5/test.py.test[in-in_ansi_join--Debug]',
        'ydb/services/ydb/ut/YdbOlapStore.LogWithUnionAllAscending'
        ]
    last_n_runs_of_test_amount = 5

 
    query = f"""
        PRAGMA AnsiInForEmptyOrNullableItemsCollections;
        DECLARE $test_names AS List<Utf8>;
        DECLARE $rn_max AS Int32;
        $tests=(
            SELECT 
                suite_folder ||'/' || test_name as full_name,test_name,build_type, commit, branch, run_timestamp, status, status_description,
                ROW_NUMBER() OVER (PARTITION BY test_name ORDER BY run_timestamp DESC) AS rn
            FROM 
                `test_results/test_runs_results`
            where job_name = 'Postcommit_relwithdebinfo' and
            suite_folder ||'/' || test_name in  $test_names
            and status != 'skipped'
        );

        select * from  $tests
        WHERE rn <= $rn_max
        ORDER BY test_name, run_timestamp DESC;  
    """

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )

        with session.transaction() as transaction:
            
            prepared_query = session.prepare(query)
            query_params = {
                '$test_names': test_names_array,
                '$rn_max': last_n_runs_of_test_amount
            }

            result_set = session.transaction(ydb.SerializableReadWrite()).execute(prepared_query, parameters=query_params, commit_tx=True)

            results = {}
            for row in result_set[0].rows:
                results[row['full_name'].decode("utf-8")]={
                    'status': row['status'],
                    'commit': row['commit'],
                    'timestamp': datetime.datetime.fromtimestamp(int(row['run_timestamp']/1000000)).strftime("%H:%m %B %d, %Y") 
                    }
            return results


if __name__ == "__main__":
    get_test_history()
