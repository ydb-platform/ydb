#!/usr/bin/env python3

import datetime
import os
import sys
import time
import ydb

# Add analytics directory to path for ydb_wrapper import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
from ydb_wrapper import YDBWrapper


def get_test_history(test_names_array, days_back, build_type, branch):
    """
    Get test history for the specified number of days back.
    
    Args:
        test_names_array: List of test full names (suite_folder/test_name)
        days_back: Number of days to look back (instead of last N runs)
        build_type: Build type filter (can be empty string for all)
        branch: Branch filter (can be empty string for all)
    
    Returns:
        Dictionary with test history: {test_name: {timestamp: {status, datetime, ...}}}
    """
    # Cap days_back to prevent performance issues
    MAX_DAYS_BACK = 180
    if days_back > MAX_DAYS_BACK:
        print(f"Warning: days_back ({days_back}) exceeds maximum ({MAX_DAYS_BACK}), capping to {MAX_DAYS_BACK}")
        days_back = MAX_DAYS_BACK
    
    with YDBWrapper() as ydb_wrapper:
        # Check credentials
        if not ydb_wrapper.check_credentials():
            print(f"Warning: YDB credentials not found, returning empty history")
            return {}

        # Get table paths from config
        test_runs_table = ydb_wrapper.get_table_path("test_results")
        
        print(f"Querying history for {len(test_names_array)} tests:")
        print(f"  build_type: {build_type}")
        print(f"  branch: {branch}")
        print(f"  days_back: {days_back}")
        
        results = {}
        batch_size = 500
        
        for start in range(0, len(test_names_array), batch_size):
            test_names_batch = test_names_array[start:start + batch_size]
            history_query = f"""
    PRAGMA AnsiInForEmptyOrNullableItemsCollections;
    DECLARE $test_names AS List<Utf8>;
    DECLARE $days_back AS Int32;
    DECLARE $build_type AS Utf8;
    DECLARE $branch AS Utf8;

    $test_names = [{','.join("'{0}'".format(x) for x in test_names_batch)}];
    $days_back = {days_back};
    $build_type = '{build_type}';
    $branch = '{branch}';

    -- Query to get test history for the specified number of days
    -- Results are ordered by test_name, then by run_timestamp DESC (newest first)
    -- This order is expected by the UI which displays results from newest to oldest
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
        job_name
    FROM 
        `{test_runs_table}` AS t
    WHERE 
        t.status != 'skipped'
        AND suite_folder || '/' || test_name IN $test_names
        AND t.run_timestamp > CurrentUtcDate() - $days_back * Interval("P1D")
        AND ($build_type = '' OR t.build_type = $build_type)
        AND ($branch = '' OR t.branch = $branch)
        AND t.job_name != 'Run-tests'
    ORDER BY 
        test_name, 
        run_timestamp DESC;

"""
            query_result = ydb_wrapper.execute_scan_query(history_query)
            
            rows_found = 0
            for row in query_result:
                rows_found += 1
                full_name = row["full_name"].decode("utf-8") if isinstance(row["full_name"], bytes) else row["full_name"]
                if full_name not in results:
                    results[full_name] = {}

                results[full_name][row["run_timestamp"]] = {
                    "branch": row["branch"].decode("utf-8") if isinstance(row["branch"], bytes) else row["branch"],
                    "status": row["status"].decode("utf-8") if isinstance(row["status"], bytes) else row["status"],
                    "commit": row["commit"].decode("utf-8") if isinstance(row["commit"], bytes) else row["commit"],
                    "datetime": datetime.datetime.fromtimestamp(int(row["run_timestamp"] / 1000000)).strftime("%Y-%m-%d %H:%M:%S"),
                    "status_description": (row["status_description"].decode("utf-8") if isinstance(row["status_description"], bytes) else row["status_description"]).replace(';;','\n'),
                    "job_id": row["job_id"].decode("utf-8") if isinstance(row["job_id"], bytes) else row["job_id"],
                    "job_name": row["job_name"].decode("utf-8") if isinstance(row["job_name"], bytes) else row["job_name"]
                }
            
            if rows_found == 0:
                print(f"  Warning: No rows found in YDB for batch {start // batch_size + 1}")
                print(f"  This could mean:")
                print(f"    - No data with build_type='{build_type}' and branch='{branch}' in last {days_back} days")
                print(f"    - Test names don't match (check format: suite_folder/test_name)")
        
        print(f'Retrieved history: {len(results)} tests with data out of {len(test_names_array)} requested')
        if results:
            sample_test = list(results.keys())[0]
            print(f"  Sample: {sample_test} has {len(results[sample_test])} runs")
        return results


if __name__ == "__main__":
    get_test_history(test_names_array, days_back, build_type, branch)
