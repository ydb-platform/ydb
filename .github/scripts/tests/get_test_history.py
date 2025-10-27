#!/usr/bin/env python3

import datetime
import os
import sys
import time
import ydb

# Add analytics directory to path for ydb_wrapper import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
from ydb_wrapper import YDBWrapper


def get_test_history(test_names_array, last_n_runs_of_test_amount, build_type, branch):
    script_name = os.path.basename(__file__)
    
    # Initialize YDB wrapper with context manager for automatic cleanup
    with YDBWrapper(script_name=script_name) as ydb_wrapper:
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return {}

        results = {}
        batch_size = 500
        
        for start in range(0, len(test_names_array), batch_size):
            test_names_batch = test_names_array[start:start + batch_size]
            history_query = f"""
    PRAGMA AnsiInForEmptyOrNullableItemsCollections;
    DECLARE $test_names AS List<Utf8>;
    DECLARE $rn_max AS Int32;
    DECLARE $build_type AS Utf8;
    DECLARE $branch AS Utf8;

    $test_names = [{','.join("'{0}'".format(x) for x in test_names_batch)}];
    $rn_max = {last_n_runs_of_test_amount};
    $build_type = '{build_type}';
    $branch = '{branch}';

    -- Оптимизированный запрос с учетом особенностей YDB
    $filtered_tests = (
        SELECT 
            suite_folder || '/' || test_name AS full_name,
            test_name,
            build_type, 
            commit, 
            branch, 
            run_timestamp, 
            status, 
            status_description,
            job_id,
            job_name,
            ROW_NUMBER() OVER (PARTITION BY test_name ORDER BY run_timestamp DESC) AS rn
        FROM 
            `test_results/test_runs_column` AS t
        WHERE 
            t.build_type = $build_type
            AND t.branch = $branch
            AND t.job_name IN (
                'Nightly-run',
                'Regression-run',
                'Regression-whitelist-run',
                'Postcommit_relwithdebinfo', 
                'Postcommit_asan'
            )
            AND t.status != 'skipped'
            AND suite_folder || '/' || test_name IN $test_names
    );

    -- Финальный запрос с ограничением по количеству запусков
    SELECT 
        full_name,
        test_name,
        build_type, 
        commit, 
        branch, 
        run_timestamp, 
        status, 
        status_description,
        job_id,
        job_name,
        rn
    FROM 
        $filtered_tests
    WHERE 
        rn <= $rn_max
    ORDER BY 
        test_name, 
        run_timestamp;

"""
            query_result = ydb_wrapper.execute_scan_query(history_query)

            for row in query_result:
                if not row["full_name"].decode("utf-8") in results:
                    results[row["full_name"].decode("utf-8")] = {}

                results[row["full_name"].decode("utf-8")][row["run_timestamp"]] = {
                    "branch": row["branch"],
                    "status": row["status"],
                    "commit": row["commit"],
                    "datetime": datetime.datetime.fromtimestamp(int(row["run_timestamp"] / 1000000)).strftime("%H:%m %B %d %Y"),
                    "status_description": row["status_description"].replace(';;','\n'),
                    "job_id": row["job_id"],
                    "job_name": row["job_name"]
                }
        
        print(f'Retrieved history for {len(test_names_array)} tests')
        return results


if __name__ == "__main__":
    get_test_history(test_names_array, last_n_runs_of_test_amount, build_type, branch)
