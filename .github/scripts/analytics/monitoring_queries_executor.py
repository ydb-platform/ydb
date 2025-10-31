#!/usr/bin/env python3

import os
import glob
from ydb_wrapper import YDBWrapper


def get_monitoring_queries(queries_dir):
    """Get list of all SQL files from monitoring queries directory"""
    # Get absolute path to the script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    queries_path = os.path.join(script_dir, queries_dir)
    
    # Find all .sql files
    sql_files = glob.glob(os.path.join(queries_path, "*.sql"))
    
    if not sql_files:
        print(f"Warning: No SQL files found in {queries_path}")
        return []
    
    # Sort for predictable execution order
    sql_files.sort()
    
    return sql_files


def execute_monitoring_query(ydb_wrapper, query_file_path):
    """Execute a single monitoring query"""
    # Get filename without extension for query_name
    query_name = os.path.splitext(os.path.basename(query_file_path))[0]
    
    print(f"\n{'='*80}")
    print(f"Executing monitoring query: {query_name}")
    print(f"File: {query_file_path}")
    print(f"{'='*80}\n")
    
    try:
        # Read SQL query from file
        with open(query_file_path, 'r', encoding='utf-8') as f:
            query = f.read()
        
        # Check that query is not empty
        if not query.strip():
            print(f"Warning: Query file {query_file_path} is empty, skipping")
            return False
        
        # Execute query with query_name parameter
        results = ydb_wrapper.execute_scan_query(query, query_name=query_name)
        
        # Log result
        row_count = len(results) if results else 0
        print(f"✅ Query '{query_name}' completed successfully: {row_count} rows returned")
        
        return True
        
    except Exception as e:
        print(f"❌ Error executing query '{query_name}': {e}")
        return False


def main():
    script_name = os.path.basename(__file__)
    queries_dir = "monitoring_queries"
    
    print(f"Starting monitoring queries executor...")
    print(f"Script: {script_name}")
    print(f"Queries directory: {queries_dir}\n")
    
    # Initialize YDB wrapper with context manager
    with YDBWrapper() as ydb_wrapper:
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return 1
        
        # Get list of monitoring queries
        query_files = get_monitoring_queries(queries_dir)
        
        if not query_files:
            print("No monitoring queries to execute")
            return 0
        
        print(f"Found {len(query_files)} monitoring queries to execute:\n")
        for i, query_file in enumerate(query_files, 1):
            query_name = os.path.splitext(os.path.basename(query_file))[0]
            print(f"  {i}. {query_name}")
        print()
        
        # Execute all queries
        success_count = 0
        failed_count = 0
        
        for query_file in query_files:
            if execute_monitoring_query(ydb_wrapper, query_file):
                success_count += 1
            else:
                failed_count += 1
        
        # Final statistics
        print(f"\n{'='*80}")
        print(f"Monitoring queries execution completed")
        print(f"Total: {len(query_files)}, Success: {success_count}, Failed: {failed_count}")
        print(f"{'='*80}\n")
        
        # Return exit code: 0 if all successful, 1 if there were errors
        return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    exit(main())

