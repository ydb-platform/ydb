#!/usr/bin/env python3

import os
import ydb
import configparser
import time
import json
from datetime import datetime, timezone, timedelta
import sys
import re

# Import shared GitHub issue utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from github_issue_utils import parse_body, create_test_issue_mapping

# Load YDB configuration
dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../../config/ydb_qa_db.ini"
config.read(config_file_path)

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

def get_muted_test_data_from_existing_mart(driver):
    """Get muted test data from the existing test_muted_monitor_mart table"""
    query = """
    SELECT *
    FROM `test_results/analytics/test_muted_monitor_mart`
    """
    
    print("Fetching muted test data from existing data mart...")
    start_time = time.time()
    
    results = []
    column_types = None
    
    try:
        scan_query = ydb.ScanQuery(query, {})
        it = driver.table_client.scan_query(scan_query)
        
        while True:
            try:
                result = next(it)
                if column_types is None:
                    column_types = [(col.name, col.type) for col in result.result_set.columns]
                results.extend(result.result_set.rows)
            except StopIteration:
                break
    except Exception as e:
        print(f"Warning: Could not fetch muted test data from existing mart: {e}")
        print("Falling back to direct query from tests_monitor table...")
        return get_muted_test_data_fallback(driver)

    end_time = time.time()
    print(f'Fetched {len(results)} muted test records from existing mart, duration: {end_time - start_time}s')
    return results, column_types

def get_muted_test_data_fallback(driver):
    """Fallback: Get muted test data directly from tests_monitor table"""
    query = """
    SELECT 
        state_filtered, 
        test_name, 
        suite_folder, 
        full_name, 
        date_window, 
        build_type, 
        branch, 
        days_ago_window, 
        pass_count, 
        mute_count, 
        fail_count, 
        skip_count, 
        owner, 
        is_muted, 
        is_test_chunk, 
        state, 
        previous_state, 
        state_change_date, 
        days_in_state, 
        previous_mute_state, 
        mute_state_change_date, 
        days_in_mute_state, 
        previous_state_filtered, 
        state_change_date_filtered, 
        days_in_state_filtered,
        CASE 
            WHEN (state = 'Skipped' AND days_in_state > 14) THEN 'Skipped'
            WHEN days_in_mute_state >= 30 THEN 'MUTED: delete candidate'
            ELSE 'MUTED: in sla'
        END as resolution,
        String::ReplaceAll(owner, 'TEAM:@ydb-platform/', '') as owner_team,
        CASE 
            WHEN is_muted = 1 OR (state = 'Skipped' AND days_in_state > 14) THEN TRUE
            ELSE FALSE
        END as is_muted_or_skipped
    FROM `test_results/analytics/tests_monitor`
    WHERE date_window >= CurrentUtcDate() - 30 * Interval("P1D")
    and ( branch = 'main' or branch like 'stable-%')
    and is_test_chunk = 0
    and (CASE 
            WHEN is_muted = 1 OR (state = 'Skipped' AND days_in_state > 14) THEN TRUE
            ELSE FALSE
        END ) = TRUE
    """
    
    print("Fetching muted test data directly from tests_monitor...")
    start_time = time.time()
    
    results = []
    column_types = None
    
    try:
        scan_query = ydb.ScanQuery(query, {})
        it = driver.table_client.scan_query(scan_query)
        
        while True:
            try:
                result = next(it)
                if column_types is None:
                    column_types = [(col.name, col.type) for col in result.result_set.columns]
                results.extend(result.result_set.rows)
            except StopIteration:
                break
    except Exception as e:
        print(f"Warning: Could not fetch muted test data from tests_monitor: {e}")
        print("This might be because the tests_monitor table doesn't exist yet.")
        return [], []

    end_time = time.time()
    print(f'Fetched {len(results)} muted test records from tests_monitor, duration: {end_time - start_time}s')
    return results, column_types

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
    WHERE state = 'OPEN'
    AND body IS NOT NULL
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

def create_test_issue_mapping_from_db_data(issues_data):
    """Create a mapping from test names to GitHub issue URLs using shared utilities"""
    return create_test_issue_mapping(issues_data)

def enhance_muted_tests_with_issues(muted_tests, test_to_issue):
    """Enhance muted test data with GitHub issue information"""
    print("Enhancing muted test data with GitHub issue links...")
    
    enhanced_tests = []
    
    for test in muted_tests:
        full_name = test.get('full_name', '')
        branch = test.get('branch', '')
        
        # Create enhanced record with all original fields
        enhanced_test = dict(test)
        
        # Initialize issue-related fields
        enhanced_test['github_issue_url'] = None
        enhanced_test['github_issue_title'] = None
        enhanced_test['github_issue_number'] = None
        
        # Look for matching GitHub issues
        if full_name in test_to_issue:
            # Find the most relevant issue (preferably matching branch)
            issues = test_to_issue[full_name]
            best_issue = None
            
            # First try to find an issue that matches the branch
            for issue in issues:
                if branch in issue.get('branches', []):
                    best_issue = issue
                    break
            
            # If no branch match, take the first issue
            if not best_issue and issues:
                best_issue = issues[0]
            
            if best_issue:
                enhanced_test['github_issue_url'] = best_issue['url']
                enhanced_test['github_issue_title'] = best_issue['title']
                enhanced_test['github_issue_number'] = best_issue['issue_number']
        
        enhanced_tests.append(enhanced_test)
    
    # Count how many tests have associated issues
    tests_with_issues = sum(1 for test in enhanced_tests if test['github_issue_url'] is not None)
    print(f"Enhanced {len(enhanced_tests)} tests, {tests_with_issues} have associated GitHub issues")
    
    return enhanced_tests

def get_enhanced_column_types(original_column_types):
    """Add GitHub issue columns to the original column types"""
    enhanced_types = list(original_column_types)
    
    # Add new columns for GitHub issue information
    enhanced_types.extend([
        ('github_issue_url', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
        ('github_issue_title', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
        ('github_issue_number', ydb.OptionalType(ydb.PrimitiveType.Uint64))
    ])
    
    return enhanced_types

def create_enhanced_table(session, table_path, column_types):
    """Create the enhanced table with GitHub issue columns"""
    print(f"Creating enhanced table: {table_path}")
    
    columns_sql = []
    for column_name, column_type in column_types:
        if hasattr(column_type, 'proto'):
            # Handle YDB type from query result
            if column_type.proto.HasField('optional_type'):
                base_type = column_type.proto.optional_type.item
                for ptype in ydb.PrimitiveType:
                    if ptype.proto.type_id == base_type.type_id:
                        type_str = ydb.OptionalType(ptype)._repr
                        break
                else:
                    type_str = "Utf8?"  # fallback
            else:
                for ptype in ydb.PrimitiveType:
                    if ptype.proto.type_id == column_type.proto.type_id:
                        type_str = ptype.name
                        break
                else:
                    type_str = "Utf8"  # fallback
        else:
            # Handle explicitly defined YDB type
            if hasattr(column_type, '_repr'):
                type_str = column_type._repr
            elif hasattr(column_type, 'name'):
                type_str = column_type.name
            else:
                type_str = str(column_type)
        
        columns_sql.append(f"`{column_name}` {type_str.replace('?', '')}")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        {', '.join(columns_sql)},
        PRIMARY KEY (date_window, owner_team, branch, build_type, suite_folder, full_name)
    )
    PARTITION BY HASH(date_window, branch, build_type, owner_team, suite_folder)
    WITH (
        STORE = COLUMN,
        TTL = Interval("PT43200M") ON date_window
    )
    """

    print(f"Creating table with query: {create_table_sql}")
    session.execute_scheme(create_table_sql)

def bulk_upsert_enhanced_data(table_client, table_path, enhanced_data, column_types):
    """Bulk upsert enhanced data into the table"""
    print(f"Bulk upserting {len(enhanced_data)} enhanced records to {table_path}")
    start_time = time.time()
    
    column_types_map = ydb.BulkUpsertColumns()
    for column_name, column_type in column_types:
        if hasattr(column_type, 'proto'):
            # Handle YDB type from query result
            if column_type.proto.HasField('optional_type'):
                base_type = column_type.proto.optional_type.item
                for ptype in ydb.PrimitiveType:
                    if ptype.proto.type_id == base_type.type_id:
                        column_types_map.add_column(column_name, ydb.OptionalType(ptype))
                        break
                else:
                    column_types_map.add_column(column_name, ydb.OptionalType(ydb.PrimitiveType.Utf8))
            else:
                for ptype in ydb.PrimitiveType:
                    if ptype.proto.type_id == column_type.proto.type_id:
                        column_types_map.add_column(column_name, ptype)
                        break
                else:
                    column_types_map.add_column(column_name, ydb.PrimitiveType.Utf8)
        else:
            # Handle explicitly defined YDB type
            column_types_map.add_column(column_name, column_type)

    table_client.bulk_upsert(table_path, enhanced_data, column_types_map)
    
    end_time = time.time()
    print(f"Bulk upsert completed, duration: {end_time - start_time}s")

def main():
    """Main function to create enhanced muted test data mart with GitHub issue links"""
    print("Starting enhanced muted test data mart creation")
    script_start_time = time.time()
    
    # Check environment variables
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
        return 1
    
    os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
        "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
    ]
    
    table_path = "test_results/analytics/test_muted_monitor_mart_with_issue"
    full_table_path = f"{DATABASE_PATH}/{table_path}"
    
    try:
        with ydb.Driver(
            endpoint=DATABASE_ENDPOINT,
            database=DATABASE_PATH,
            credentials=ydb.credentials_from_env_variables()
        ) as driver:
            driver.wait(timeout=10, fail_fast=True)
            
            with ydb.SessionPool(driver) as pool:
                # Get muted test data (try existing mart first, fallback to direct query)
                muted_tests, original_column_types = get_muted_test_data_from_existing_mart(driver)
                
                if not muted_tests:
                    print("No muted test data found")
                    return 0
                
                if not original_column_types:
                    print("No column types found from muted test data")
                    return 0
                
                # Get GitHub issues data
                issues_data = get_github_issues_data(driver)
                
                # Create test-to-issue mapping using shared utilities
                print("Creating test-to-issue mapping...")
                test_to_issue = create_test_issue_mapping_from_db_data(issues_data)
                print(f"Created mapping for {len(test_to_issue)} unique test names")
                
                # Enhance muted tests with issue information
                enhanced_tests = enhance_muted_tests_with_issues(muted_tests, test_to_issue)
                
                # Get enhanced column types
                enhanced_column_types = get_enhanced_column_types(original_column_types)
                
                # Create enhanced table
                def create_table_wrapper(session):
                    create_enhanced_table(session, table_path, enhanced_column_types)
                    return True
                
                pool.retry_operation_sync(create_table_wrapper)
                
                # Bulk upsert enhanced data
                bulk_upsert_enhanced_data(driver.table_client, full_table_path, enhanced_tests, enhanced_column_types)
        
        script_elapsed = time.time() - script_start_time
        print(f"Script completed successfully, total time: {script_elapsed:.2f}s")
        
    except Exception as e:
        print(f"Error during execution: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())