#!/usr/bin/env python3

import argparse
import json
import os
import sys
import ydb

from codeowners import CodeOwners
from decimal import Decimal
from ydb_wrapper import YDBWrapper

max_characters_for_status_description = int(7340032/3)  #workaround for error "cannot split batch in according to limits: there is row with size more then limit (7340032)"

def get_column_types():
    """Get column types for bulk upsert (base columns, error_type and metrics added conditionally)"""
    return (
        ydb.BulkUpsertColumns()
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("commit", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("duration", ydb.OptionalType(ydb.PrimitiveType.Double))
        .add_column("job_id", ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column("job_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("log", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("logsdir", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("owners", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("pull", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("run_timestamp", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column("status", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("status_description", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("stderr", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("stdout", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("test_id", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    )

def get_table_schema(table_path):
    """Get SQL schema for table creation"""
    return f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            -- Primary key columns (in PRIMARY KEY order)
            run_timestamp Timestamp NOT NULL,
            build_type Utf8 NOT NULL,
            branch Utf8 NOT NULL,
            full_name Utf8 NOT NULL,
            test_name Utf8 NOT NULL,
            suite_folder Utf8 NOT NULL,
            status Utf8 NOT NULL,
            
            -- Other required columns
            test_id Utf8 NOT NULL,
            
            -- Optional columns
            job_name Utf8,
            job_id Uint64,
            commit Utf8,
            pull Utf8,
            duration Double,
            status_description Utf8,
            owners Utf8,
            log Utf8,
            logsdir Utf8,
            stderr Utf8,
            stdout Utf8,
            error_type Utf8,
            metadata Json,
            metrics Json,
            
            -- Primary key definition
            PRIMARY KEY (run_timestamp, build_type, branch, full_name, test_name, suite_folder, status)
        )
        PARTITION BY HASH(run_timestamp, build_type, branch, suite_folder)
        WITH (
            STORE = COLUMN
        );
        """


def parse_build_results_report(test_results_file, build_type, job_name, job_id, commit, branch, pull, run_timestamp):
    with open(test_results_file, 'r') as f:
        report = json.load(f)

    results = []
    for result in report.get("results", []):
        if result.get("type") != "test":
            continue
        
        suite_folder = result.get("path", "")
        name_part = result.get("name", "")
        subtest_name = result.get("subtest_name", "")
        
        # Format test_name: name.subtest_name (same as generate-summary.py)
        # This matches the format used in generate-summary.py and other scripts
        if subtest_name:
            if name_part:
                test_name = f"{name_part}.{subtest_name}"
            else:
                test_name = subtest_name
        else:
            test_name = name_part
        
        # Get duration from result
        duration = result.get("duration", 0)
        
        # Determine status
        status_str = result.get("status", "OK")
        error_type = result.get("error_type", "")
        status_description = result.get("rich-snippet", "")  # Already cleaned by transform_build_results.py
        
        if result.get("muted", False):
            status = "mute"
        elif status_str == "FAILED":
            status = "failure"
        elif status_str == "ERROR":
            status = "error"
        elif status_str == "SKIPPED":
            status = "skipped"
        else:
            # OK, PASSED, or any other status -> "passed"
            status = "passed"
        
        # Extract log URLs from links (updated by transform_build_results.py with URLs)
        # Links format: {"log": ["https://..."], "stdout": ["https://..."], "logsdir": ["https://..."]}
        links = result.get("links", {})
        
        def get_link_url(link_type):
            if link_type in links and isinstance(links[link_type], list) and len(links[link_type]) > 0:
                return links[link_type][0]  # Take first URL from array
            return ""
        
        log_url = get_link_url("log")
        logsdir_url = get_link_url("logsdir")
        stderr_url = get_link_url("stderr")
        stdout_url = get_link_url("stdout")

        # Determine entity_type: suite, chunk, or test
        if result.get("suite"):
            entity_type = "suite"
        elif result.get("chunk"):
            entity_type = "chunk"
        else:
            entity_type = "test"

        # Build metadata JSON from available fields
        metadata = {
            "chunk_hid": result.get("chunk_hid"),
            "suite_hid": result.get("suite_hid"),
            "uid": result.get("uid"),
            "toolchain": result.get("toolchain"),
            "size": result.get("size"),
            "name": result.get("name"),
            "id": result.get("id"),
            "hid": result.get("hid"),
            "entity_type": entity_type,
        }
        # Remove None values from metadata
        metadata = {k: v for k, v in metadata.items() if v is not None}
        # Convert to JSON string, or None if empty
        metadata_json = json.dumps(metadata) if metadata else None
        
        # Always add metrics as separate field (default to empty dict if not available)
        metrics = result.get("metrics", {})
        metrics_json = json.dumps(metrics) if metrics is not None else None

        results.append(
            {
                "build_type": build_type,
                "commit": commit,
                "branch": branch,
                "pull": pull,
                "run_timestamp": int(run_timestamp)*1000000,
                "job_name": job_name,
                "job_id": int(job_id),
                "suite_folder": suite_folder,
                "test_name": test_name,
                "duration": Decimal(duration),
                "status": status,
                "status_description": status_description[:max_characters_for_status_description],
                "log": log_url,
                "logsdir": logsdir_url,
                "stderr": stderr_url,
                "stdout": stdout_url,
                "error_type": error_type if error_type else None,
                "metadata": metadata_json,
                "metrics": metrics_json,
            }
        )
    return results


def sort_codeowners_lines(codeowners_lines):
    def path_specificity(line):
        # removing comments
        trimmed_line = line.strip()
        if not trimmed_line or trimmed_line.startswith('#'):
            return -1, -1
        path = trimmed_line.split()[0]
        return len(path.split('/')), len(path)

    sorted_lines = sorted(codeowners_lines, key=path_specificity)
    return sorted_lines

def get_codeowners_for_tests(codeowners_file_path, tests_data):
    with open(codeowners_file_path, 'r') as file:
        data = file.readlines()
        owners_obj = CodeOwners(''.join(sort_codeowners_lines(data)))
        tests_data_with_owners = []
        for test in tests_data:
            target_path = test["suite_folder"]
            owners = owners_obj.of(target_path)
            test["owners"] = ";;".join(
                [(":".join(x)) for x in owners])
            tests_data_with_owners.append(test)
        return tests_data_with_owners


def check_table_schema(wrapper, table_path):
    """Check table schema and return which columns exist (single table path)."""
    schema_check_query = f"SELECT * FROM `{table_path}` LIMIT 1"
    try:
        _, column_metadata = wrapper.execute_scan_query_with_metadata(schema_check_query)
        existing_columns = {col_name for col_name, _ in (column_metadata or [])}
        has_full_name = 'full_name' in existing_columns
        has_metadata = 'metadata' in existing_columns
        has_error_type = 'error_type' in existing_columns
        has_metrics = 'metrics' in existing_columns
        return has_full_name, has_metadata, has_error_type, has_metrics
    except Exception as e:
        # Table doesn't exist or error - assume old schema
        print(f'Table schema check failed (assuming old schema): {e}')
        return False, False, False, False


def prepare_rows_for_upload(results_with_owners):
    """Prepare rows for upload - always include full_name, metadata and metrics"""
    prepared_rows = []
    for index, row in enumerate(results_with_owners):
        upload_row = {
            'branch': row['branch'],
            'build_type': row['build_type'],
            'commit': row['commit'],
            'duration': row['duration'],
            'job_id': row['job_id'],
            'job_name': row['job_name'],
            'log': row['log'],
            'logsdir': row['logsdir'],
            'owners': row['owners'],
            'pull': row['pull'],
            'run_timestamp': row['run_timestamp'],
            'status_description': row['status_description'],
            'status': row['status'],
            'stderr': row['stderr'],
            'stdout': row['stdout'],
            'suite_folder': row['suite_folder'],
            'test_id': f"{row['pull']}_{row['run_timestamp']}_{index}",
            'test_name': row['test_name'],
            'error_type': row.get('error_type'),
            # Format: path/name.subtest_name (same as generate-summary.py)
            'full_name': f"{row['suite_folder']}/{row['test_name']}",
            'metadata': row.get('metadata'),  # JSON string from parse_build_results_report
            'metrics': row.get('metrics'),  # JSON string from parse_build_results_report
        }
        
        prepared_rows.append(upload_row)
    
    return prepared_rows


def filter_rows_for_schema(rows, has_full_name, has_metadata, has_error_type, has_metrics):
    """Filter rows to match table schema - remove fields that don't exist in table"""
    filtered_rows = []
    for row in rows:
        filtered_row = row.copy()
        
        # Remove full_name if table doesn't have it
        if not has_full_name:
            filtered_row.pop('full_name', None)
        
        # Ensure error_type is present if table has it, otherwise remove it
        if has_error_type:
            # Make sure error_type key exists in row (set to None if missing)
            if 'error_type' not in filtered_row:
                filtered_row['error_type'] = None
        else:
            filtered_row.pop('error_type', None)
        
        # Handle metadata: remove if table doesn't have it, or convert empty JSON to None
        if not has_metadata:
            filtered_row.pop('metadata', None)
        elif 'metadata' in filtered_row:
            # If metadata is empty JSON string "{}", convert to None (like export_issues_to_ydb.py)
            if filtered_row['metadata'] == "{}":
                filtered_row['metadata'] = None
        
        # Ensure metrics is present if table has it, otherwise remove it
        if has_metrics:
            # Make sure metrics key exists in row (set to None if missing)
            if 'metrics' not in filtered_row:
                filtered_row['metrics'] = None
        else:
            filtered_row.pop('metrics', None)
        
        filtered_rows.append(filtered_row)
    
    return filtered_rows


def prepare_column_types(has_full_name, has_metadata, has_error_type, has_metrics):
    """Build column types based on schema"""
    column_types = get_column_types()
    if has_full_name:
        column_types = column_types.add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    if has_metadata:
        column_types = column_types.add_column("metadata", ydb.OptionalType(ydb.PrimitiveType.Json))
    if has_error_type:
        column_types = column_types.add_column("error_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    if has_metrics:
        column_types = column_types.add_column("metrics", ydb.OptionalType(ydb.PrimitiveType.Json))
    return column_types


def upload_to_table(wrapper, table_path, rows, column_types, table_name="table"):
    """Upload rows to specified table"""
    if not rows:
        print(f'No rows to upload to {table_name}')
        return
    
    print(f'Uploading {len(rows)} test results to {table_name}')
    wrapper.bulk_upsert_batches(
        table_path,
        rows,
        column_types,
        batch_size=1000
    )
    print(f'Successfully uploaded to {table_name}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-results-file', action='store',
                        required=True, help='build-results-report JSON with results of tests')
    parser.add_argument('--build-type', action='store',
                        required=True, help='build type')
    parser.add_argument('--commit', default='store',
                        dest="commit", required=True, help='commit sha')
    parser.add_argument('--branch', default='store',
                        dest="branch", required=True, help='branch name ')
    parser.add_argument('--pull', action='store', dest="pull",
                        required=True, help='pull number')
    parser.add_argument('--run-timestamp', action='store',
                        dest="run_timestamp", required=True, help='time of test run start')
    parser.add_argument('--job-name', action='store', dest="job_name",
                        required=True, help='job name where launched')
    parser.add_argument('--job-id', action='store', dest="job_id",
                        required=True, help='job id of workflow')

    args = parser.parse_args()

    dir_path = os.path.dirname(__file__)
    git_root = f"{dir_path}/../../.."
    codeowners = f"{git_root}/.github/TESTOWNERS"

    try:
        with YDBWrapper() as wrapper:
            # Check credentials
            if not wrapper.check_credentials():
                print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
                return 1
            
            # Parse build-results-report JSON with test results
            results = parse_build_results_report(
                args.test_results_file, args.build_type, args.job_name, args.job_id,
                args.commit, args.branch, args.pull, args.run_timestamp
            )

            # Add owner information
            result_with_owners = get_codeowners_for_tests(codeowners, results)
            
            # Get table paths
            test_table_path = wrapper.get_table_path("test_results")
            
            # Create table if it doesn't exist (IF NOT EXISTS - won't affect existing table)
            wrapper.create_table(test_table_path, get_table_schema(test_table_path))
            
            # Prepare rows with full data (always includes full_name, metadata and metrics)
            prepared_rows = prepare_rows_for_upload(result_with_owners)
            
            # Check main table schema
            has_full_name, has_metadata, has_error_type, has_metrics = check_table_schema(wrapper, test_table_path)
            print(f'Main table schema: full_name={has_full_name}, metadata={has_metadata}, error_type={has_error_type}, metrics={has_metrics}')
            
            # Filter rows and prepare column types for main table
            main_rows = filter_rows_for_schema(prepared_rows, has_full_name, has_metadata, has_error_type, has_metrics)
            main_column_types = prepare_column_types(has_full_name, has_metadata, has_error_type, has_metrics)
            
            # Upload to main table
            upload_to_table(wrapper, test_table_path, main_rows, main_column_types, "main table")
                
    except Exception as e:
        print(f"Warning: Failed to upload test results to YDB: {e}")
        print("This is not a critical error, continuing with CI process...")
        return 0
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
