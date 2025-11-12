#!/usr/bin/env python3

import argparse
import datetime
import os
import ydb
from ydb_wrapper import YDBWrapper


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--build_type', default='relwithdebinfo', type=str, help='build types')
    parser.add_argument('--branch', default='main', type=str, help='branch')
    parser.add_argument('--start-date', dest='start_date', type=str, help='Start date (YYYY-MM-DD), inclusive')
    parser.add_argument('--end-date', dest='end_date', type=str, help='End date (YYYY-MM-DD), inclusive')

    args, unknown = parser.parse_known_args()
    build_type = args.build_type
    branch = args.branch
    start_date_override = datetime.date.fromisoformat(args.start_date) if args.start_date else None
    end_date_override = datetime.date.fromisoformat(args.end_date) if args.end_date else None
    
    if start_date_override and end_date_override and start_date_override > end_date_override:
        raise ValueError("start-date must be earlier or equal to end-date")
    
    print(f'🚀 Starting flaky_tests_history.py')
    print(f'   📅 Days window: 1')
    print(f'   🔧 Build type: {build_type}')
    print(f'   🌿 Branch: {branch}')
    if start_date_override:
        print(f'   📍 Start date override: {start_date_override}')
    if end_date_override:
        print(f'   📍 End date override: {end_date_override}')
    
    with YDBWrapper() as ydb_wrapper:
        # Get table paths from config
        test_runs_table = ydb_wrapper.get_table_path("test_results")
        testowners_table = ydb_wrapper.get_table_path("testowners")
        flaky_tests_table = ydb_wrapper.get_table_path("flaky_tests_window")
      
        # Get last date from history
        table_path = flaky_tests_table
        last_date_query = f"""
            select max(date_window) as max_date_window from `{table_path}`
            where build_type = '{build_type}' and branch = '{branch}'
        """
        
        try:
            results = ydb_wrapper.execute_scan_query(last_date_query, query_name=f"get_last_date_from_history_{branch}")
            
            default_start_date = datetime.date(2024, 9, 1)
            base_date = datetime.date(1970, 1, 1)
            
            # YDB may return date_window as int (days since 1970-01-01) or datetime.date
            max_date_window = results[0].get('max_date_window') if results[0] else None
            if start_date_override:
                last_datetime = max(start_date_override, default_start_date)
            else:
                if max_date_window is not None:
                    # Convert int to date if needed
                    if isinstance(max_date_window, int):
                        max_date_window = base_date + datetime.timedelta(days=max_date_window)
                    # Now max_date_window is datetime.date, can compare
                    if max_date_window > default_start_date:
                        last_datetime = max_date_window
                    else:
                        last_datetime = default_start_date
                else:
                    last_datetime = default_start_date
            
            today = end_date_override if end_date_override else datetime.date.today()
            if last_datetime > today:
                raise ValueError("Start date is after end date/today; nothing to process")
            
            last_date = last_datetime.strftime('%Y-%m-%d')
            print(f'📅 Start history date: {last_date}')
            print(f'📅 End history date: {today.strftime("%Y-%m-%d")}')
            
            # Create table if it doesn't exist
            create_table_sql = f"""
            CREATE table IF NOT EXISTS `{table_path}` (
                `test_name` Utf8 NOT NULL,
                `suite_folder` Utf8 NOT NULL,
                `full_name` Utf8 NOT NULL,
                `date_window` Date NOT NULL,
                `build_type` Utf8 NOT NULL,
                `branch` Utf8 NOT NULL,
                `first_run` Timestamp,
                `last_run` Timestamp ,
                `owners` Utf8 ,
                `days_ago_window` Uint64 NOT NULL,
                `history` String,
                `history_class` String,
                `pass_count` Uint64,
                `mute_count` Uint64,
                `fail_count` Uint64,
                `skip_count` Uint64,
                PRIMARY KEY (`test_name`, `suite_folder`, `full_name`,date_window, build_type, branch)
            )
                PARTITION BY HASH(`full_name`,build_type,branch)
                WITH (STORE = COLUMN)
        """
            
            ydb_wrapper.create_table(table_path, create_table_sql)
            
            # Process each date
            date_list = [last_datetime + datetime.timedelta(days=x) for x in range((today - last_datetime).days + 1)]
            
            print(f'📊 Processing {len(date_list)} dates from {last_date} to {today}')
            
            # Collect all data for bulk upsert
            all_prepared_rows = []
            
            for i, date in enumerate(sorted(date_list), 1):
                print(f'📅 Processing date {i}/{len(date_list)}: {date}')
                
                query_get_history = f"""
                select
                    full_name,
                    date_base,
                    history_list,
                    if(dist_hist = '','no_runs',dist_hist) as dist_hist,
                    suite_folder,
                    test_name,
                    build_type,
                    branch,
                    owners,
                    first_run,
                    last_run

                from (
                    select
                        full_name,
                        date_base,
                        AGG_LIST(status) as history_list ,
                        String::JoinFromList( ListSort(AGG_LIST_DISTINCT(status)) ,',') as dist_hist,
                        suite_folder,
                        test_name,
                        owners,
                        build_type,
                        branch,
                        min(run_timestamp) as first_run,
                        max(run_timestamp) as last_run
                    from (
                        select * from (
       
                            select distinct
                                full_name,
                                suite_folder,
                                test_name,
                                owners,
                                Date('{date}') as date_base,
                                '{build_type}' as  build_type,
                                '{branch}' as  branch
                            from  `{testowners_table}` 
                        ) as test_and_date
                        left JOIN (
                            
                            select
                                suite_folder || '/' || test_name as full_name,
                                run_timestamp,
                                status
                            from  `{test_runs_table}`
                            where
                                run_timestamp >= Date('{date}')
                                and run_timestamp < Date('{date}') + Interval("P1D")

                                and job_name in (
                                    'Nightly-run',
                                    'Regression-run',
                                    'Regression-run_Large',
                                    'Regression-run_Small_and_Medium',
                                    'Regression-run_compatibility',
                                    'Regression-whitelist-run',
                                    'Postcommit_relwithdebinfo', 
                                    'Postcommit_asan'
                                ) 
                                and build_type = '{build_type}'
                                and branch = '{branch}'
                            order by full_name,run_timestamp desc
                            
                        ) as hist
                        ON test_and_date.full_name=hist.full_name
                    )
                    GROUP BY full_name,suite_folder,test_name,date_base,build_type,branch,owners
                )
            """
                
                results = ydb_wrapper.execute_scan_query(query_get_history, query_name=f"get_flaky_test_history_for_date_{branch}")
                print(f'📈 History data captured, {len(results)} rows')
                
                # Prepare data for upsert
                for row in results:
                    row['count'] = dict(zip(list(row['history_list']), [list(
                        row['history_list']).count(i) for i in list(row['history_list'])]))   
                    all_prepared_rows.append({
                        'suite_folder': row['suite_folder'],
                        'test_name': row['test_name'],
                        'full_name': row['full_name'],
                        'date_window': row['date_base'],
                        'days_ago_window': 1,
                        'build_type': row['build_type'],
                        'branch': row['branch'],
                        'first_run': row['first_run'],
                        'last_run': row['last_run'],
                        'history': ','.join(row['history_list']).encode('utf8'),
                        'history_class': row['dist_hist'],
                        'pass_count': row['count'].get('passed', 0),
                        'mute_count': row['count'].get('mute', 0),
                        'fail_count': row['count'].get('failure', 0),
                        'skip_count': row['count'].get('skipped', 0),
                    })
            
            # Insert all data in one batch
            if all_prepared_rows:
                print(f'💾 Upserting {len(all_prepared_rows)} rows of history data')
                
                # Prepare column types for bulk upsert
                column_types = (
                    ydb.BulkUpsertColumns()
                    .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("first_run", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                    .add_column("last_run", ydb.OptionalType(ydb.PrimitiveType.Timestamp))
                    .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
                    .add_column("date_window", ydb.OptionalType(ydb.PrimitiveType.Date))
                    .add_column("days_ago_window", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                    .add_column("history", ydb.OptionalType(ydb.PrimitiveType.String))
                    .add_column("history_class", ydb.OptionalType(ydb.PrimitiveType.String))
                    .add_column("pass_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                    .add_column("mute_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                    .add_column("fail_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                    .add_column("skip_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
                )
                
                ydb_wrapper.bulk_upsert_batches(table_path, all_prepared_rows, column_types, batch_size=1000)
                
                print('✅ History updated successfully')
            else:
                print('ℹ️  No data to upload')

        except Exception as e:
            print(f'❌ Script failed: {e}')
            raise


if __name__ == "__main__":
    main()