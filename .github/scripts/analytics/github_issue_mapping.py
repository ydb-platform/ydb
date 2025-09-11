#!/usr/bin/env python3

"""
Create a mapping table between test names and GitHub issues.
This table will be used by SQL queries to join muted test data with GitHub issue information.
"""

import os
import ydb
import configparser
import time
import sys

# Import shared GitHub issue utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from github_issue_utils import create_test_issue_mapping

# Load YDB configuration
dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]




def get_github_issues_data(driver):
    """Get GitHub issues data from the github_data/issues table"""
    query = """
    SELECT 
        issue_number,
        title,
        url,
        state,
        body,
        created_at,
        updated_at
    FROM `github_data/issues`
    WHERE body IS NOT NULL
    AND body != ''
    """
    
    print("Fetching GitHub issues data...")
    start_time = time.time()
    
    results = []
    try:
        scan_query = ydb.ScanQuery(query, {})
        it = driver.table_client.scan_query(scan_query)
        
        while True:
            try:
                result = next(it)
                results.extend(result.result_set.rows)
            except StopIteration:
                break
    except Exception as e:
        print(f"Warning: Could not fetch GitHub issues data: {e}")
        print("This might be because the github_data/issues table doesn't exist yet.")
        return []

    end_time = time.time()
    print(f'Fetched {len(results)} GitHub issues, duration: {end_time - start_time}s')
    return results


def create_test_issue_mapping_table(session, table_path):
    """Create the test-to-issue mapping table"""
    print(f"Creating test-to-issue mapping table: {table_path}")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        `full_name` Utf8 NOT NULL,
        `branch` Utf8 NOT NULL,
        `github_issue_url` Utf8,
        `github_issue_title` Utf8,
        `github_issue_number` Uint64 NOT NULL,
        `github_issue_state` Utf8 NOT NULL,
        `github_issue_created_at` Timestamp,
        PRIMARY KEY (full_name,branch,github_issue_number,github_issue_state)
    )
    PARTITION BY HASH(full_name)
    WITH (
        STORE = COLUMN
    )
    """

    print(f"Creating table with query: {create_table_sql}")
    session.execute_scheme(create_table_sql)


def convert_mapping_to_table_data(test_to_issue_mapping):
    """Convert the test-to-issue mapping to table data format"""
    table_data = []
    
    for test_name, issues in test_to_issue_mapping.items():
        if issues:
            # Sort issues by created_at (most recent first) and take the latest one
            sorted_issues = sorted(issues, key=lambda x: x.get('created_at', 0), reverse=True)
            latest_issue = sorted_issues[0]
            
            # Create a separate record for each branch of the latest issue
            for branch in latest_issue['branches']:
                table_data.append({
                    'full_name': test_name,
                    'branch': branch,
                    'github_issue_url': latest_issue['url'],
                    'github_issue_title': latest_issue['title'],
                    'github_issue_number': latest_issue['issue_number'],
                    'github_issue_state': latest_issue['state'],
                    'github_issue_created_at': latest_issue.get('created_at'),
                })
    
    return table_data


def bulk_upsert_mapping_data(table_client, table_path, mapping_data):
    """Bulk upsert mapping data into the table"""
    print(f"Bulk upserting {len(mapping_data)} test-to-issue mappings to {table_path}")
    start_time = time.time()
    
    column_types = ydb.BulkUpsertColumns()
    column_types.add_column('full_name', ydb.PrimitiveType.Utf8)
    column_types.add_column('branch', ydb.PrimitiveType.Utf8)
    column_types.add_column('github_issue_url', ydb.OptionalType(ydb.PrimitiveType.Utf8))
    column_types.add_column('github_issue_title', ydb.OptionalType(ydb.PrimitiveType.Utf8))
    column_types.add_column('github_issue_number', ydb.OptionalType(ydb.PrimitiveType.Uint64))
    column_types.add_column('github_issue_state', ydb.PrimitiveType.Utf8)
    column_types.add_column('github_issue_created_at', ydb.OptionalType(ydb.PrimitiveType.Timestamp))
    table_client.bulk_upsert(table_path, mapping_data, column_types)
    
    end_time = time.time()
    print(f"Bulk upsert completed, duration: {end_time - start_time}s")


def main():
    """Main function to create the test-to-issue mapping table"""
    print("Starting GitHub issue mapping table creation")
    script_start_time = time.time()
    
    # Check environment variables
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
        return 1
    
    os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
        "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
    ]
    
    table_path = "test_results/analytics/github_issue_mapping"
    full_table_path = f"{DATABASE_PATH}/{table_path}"
    
    try:
        with ydb.Driver(
            endpoint=DATABASE_ENDPOINT,
            database=DATABASE_PATH,
            credentials=ydb.credentials_from_env_variables()
        ) as driver:
            driver.wait(timeout=10, fail_fast=True)
            
            with ydb.SessionPool(driver) as pool:
                # Get GitHub issues data
                issues_data = get_github_issues_data(driver)
                
                if not issues_data:
                    print("No GitHub issues data found")
                    return 0
                
                # Create test-to-issue mapping using shared utilities
                print("Creating test-to-issue mapping...")
                test_to_issue = create_test_issue_mapping(issues_data)
                print(f"Created mapping for {len(test_to_issue)} unique test names")
                
                # Convert mapping to table data format
                mapping_data = convert_mapping_to_table_data(test_to_issue)
                print(f"Converted to {len(mapping_data)} table records")
                
                # Create mapping table
                def create_table_wrapper(session):
                    create_test_issue_mapping_table(session, table_path)
                    return True
                
                pool.retry_operation_sync(create_table_wrapper)
                
                # Bulk upsert mapping data
                if mapping_data:
                    bulk_upsert_mapping_data(driver.table_client, full_table_path, mapping_data)
                else:
                    print("No mapping data to insert")
        
        script_elapsed = time.time() - script_start_time
        print(f"Script completed successfully, total time: {script_elapsed:.2f}s")
        
    except Exception as e:
        print(f"Error during execution: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())