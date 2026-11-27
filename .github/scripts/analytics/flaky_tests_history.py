#!/usr/bin/env python3

import argparse
import datetime
import os
import ydb
from ydb_wrapper import YDBWrapper


def _dedupe_history_rows(rows):
    """One row per (full_name, date_base, branch, build_type); prefer deepest suite_folder."""
    if not rows:
        return rows
    best = {}
    for row in rows:
        key = (row['full_name'], row['date_base'], row['branch'], row['build_type'])
        suite_len = len(str(row.get('suite_folder') or ''))
        if key not in best or suite_len > len(str(best[key].get('suite_folder') or '')):
            best[key] = row
    return list(best.values())


BASE_DATE = datetime.date(1970, 1, 1)
DEFAULT_DAYS_BACK = 30  # cold start when branch has no flaky_tests_window history
MAX_RESUME_GAP_DAYS = 180  # safety cap on incremental resume if collection was paused a long time
INCREMENTAL_LOOKBACK_DAYS = 1  # reprocess N days before max(date_window); catches late Large/Nightly


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--build_type', default='relwithdebinfo', type=str, help='build types')
    parser.add_argument('--branch', default='main', type=str, help='branch')
    parser.add_argument('--start-date', dest='start_date', type=str, help='Start date (YYYY-MM-DD), inclusive')
    parser.add_argument('--end-date', dest='end_date', type=str, help='End date (YYYY-MM-DD), inclusive')

    args, unknown = parser.parse_known_args()
    
    start_date_override = datetime.date.fromisoformat(args.start_date) if args.start_date else None
    end_date_override = datetime.date.fromisoformat(args.end_date) if args.end_date else None
    
    if start_date_override and end_date_override and start_date_override > end_date_override:
        raise ValueError("start-date must be earlier or equal to end-date")
    
    return {
        'build_type': args.build_type,
        'branch': args.branch,
        'start_date_override': start_date_override,
        'end_date_override': end_date_override,
    }


def convert_ydb_date_to_python(date_value):
    """Convert YDB date (int or date) to Python date."""
    if date_value is None:
        return None
    if isinstance(date_value, int):
        return BASE_DATE + datetime.timedelta(days=date_value)
    return date_value


def get_max_date_from_history(ydb_wrapper, flaky_tests_table, build_type, branch):
    """Get maximum date from flaky_tests_window table."""
    query = f"""
        select max(date_window) as max_date_window from `{flaky_tests_table}`
            where build_type = '{build_type}' and branch = '{branch}'
        """
    results = ydb_wrapper.execute_scan_query(query, query_name=f"get_last_date_from_history_{branch}")
    max_date_window = results[0].get('max_date_window') if results[0] else None
    return convert_ydb_date_to_python(max_date_window)


def get_min_date_from_test_runs(ydb_wrapper, test_runs_table, build_type, branch):
    """Get minimum date from test_runs table."""
    query = f"""
        select min(cast(run_timestamp as Date)) as min_run_date from `{test_runs_table}`
        where build_type = '{build_type}' and branch = '{branch}'
    """
    try:
        results = ydb_wrapper.execute_scan_query(query, query_name=f"get_min_date_from_test_runs_{branch}")
        min_run_date = results[0].get('min_run_date') if results[0] else None
        return convert_ydb_date_to_python(min_run_date)
    except Exception as e:
        print(f'⚠️  Failed to get min date from test_runs_table: {e}')
        return None


def determine_start_date(ydb_wrapper, test_runs_table, flaky_tests_table, build_type, branch,
                         start_date_override, end_date_override):
    """Determine the start date for processing history.
    
    Logic:
    1. If start_date_override is provided, use it (must be <= end_date)
    2. If history exists, resume from max(date_window) - INCREMENTAL_LOOKBACK_DAYS
       (reprocess recent days for late-arriving runs), but never look back further
       than MAX_RESUME_GAP_DAYS even if collection was paused for a long time —
       avoids re-creating the unbounded-backfill problem this cold-start rework
       was meant to fix, at the cost of leaving an un-backfilled gap in the rare
       case collection was dormant that long.
    3. If no history, check test_runs_table for min_run_date, capped at DEFAULT_DAYS_BACK
    4. Default cold start to DEFAULT_DAYS_BACK ago from end_date
    """
    end_date = end_date_override if end_date_override else datetime.date.today()
    default_start_date = end_date - datetime.timedelta(days=DEFAULT_DAYS_BACK)
    max_resume_start_date = end_date - datetime.timedelta(days=MAX_RESUME_GAP_DAYS)
    
    # If user explicitly provided start_date_override, use it
    if start_date_override:
        if start_date_override > end_date:
            raise ValueError(f"start-date ({start_date_override}) must be earlier or equal to end-date ({end_date})")
        return start_date_override, end_date
    
    # Try to get max date from history
    max_date_window = get_max_date_from_history(ydb_wrapper, flaky_tests_table, build_type, branch)
    
    if max_date_window is not None:
        # Cap against max(date_window) itself, then apply lookback only when uncapped
        # (avoids disabling lookback at gap == MAX_RESUME_GAP_DAYS).
        capped_max = min(max_date_window, end_date)
        if capped_max < max_resume_start_date:
            print(
                f'⚠️  Last recorded date_window ({max_date_window}) is more than {MAX_RESUME_GAP_DAYS} days '
                f'old — history collection looks paused. Capping resume start to '
                f'{max_resume_start_date} instead of backfilling the full gap.'
            )
            start_date = max_resume_start_date
        else:
            start_date = capped_max - datetime.timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
        return start_date, end_date
    
    # No history exists, check test_runs_table for min date
    min_run_date = get_min_date_from_test_runs(ydb_wrapper, test_runs_table, build_type, branch)
    
    if min_run_date is not None:
        # Use the later of min_run_date and default_start_date
        start_date = max(min_run_date, default_start_date)
        # Ensure start_date doesn't exceed end_date
        start_date = min(start_date, end_date)
        print(f'📅 Found min date from test_runs_table: {min_run_date}, using: {start_date}')
    else:
        start_date = default_start_date
        print(f'📅 No data in test_runs_table, using default: {start_date}')
    
    return start_date, end_date


def build_history_query(date, test_runs_table, testowners_table, flaky_tests_table, build_type, branch):
    """Build SQL query to get test history for a specific date.

    owners preference for that calendar day:
    1. last non-empty owners from test_runs that day (owner may change mid-day);
    2. else owners already stored in flaky for this date (do not rewrite with today's
       testowners on lookback reprocess when the day still has no_runs);
    3. else current testowners snapshot.
    """
    return f"""
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
                j.full_name AS full_name,
                j.date_base AS date_base,
                AGG_LIST(j.status) AS history_list,
                String::JoinFromList(ListSort(AGG_LIST_DISTINCT(j.status)), ',') AS dist_hist,
                j.suite_folder AS suite_folder,
                j.test_name AS test_name,
                COALESCE(
                    MAX_BY(
                        j.run_owners,
                        IF(j.run_owners IS NOT NULL AND j.run_owners != '', j.run_timestamp, NULL)
                    ),
                    prev.owners,
                    j.testowners_owners
                ) AS owners,
                j.build_type AS build_type,
                j.branch AS branch,
                min(j.run_timestamp) AS first_run,
                max(j.run_timestamp) AS last_run
            from (
                select
                    test_and_date.full_name AS full_name,
                    test_and_date.suite_folder AS suite_folder,
                    test_and_date.test_name AS test_name,
                    test_and_date.owners AS testowners_owners,
                    test_and_date.date_base AS date_base,
                    test_and_date.build_type AS build_type,
                    test_and_date.branch AS branch,
                    hist.run_timestamp AS run_timestamp,
                    hist.status AS status,
                    hist.owners AS run_owners
                from (
                    select distinct
                        full_name,
                        suite_folder,
                        test_name,
                        owners,
                        Date('{date}') as date_base,
                        '{build_type}' as build_type,
                        '{branch}' as branch
                    from `{testowners_table}`
                ) as test_and_date
                left JOIN (
                    select
                        suite_folder || '/' || test_name as full_name,
                        run_timestamp,
                        status,
                        owners
                    from `{test_runs_table}`
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
                        and (pull IS NULL OR NOT String::Contains(pull, 'manual'))
                ) as hist
                ON test_and_date.full_name = hist.full_name
            ) AS j
            LEFT JOIN (
                SELECT full_name, suite_folder, test_name, owners
                FROM `{flaky_tests_table}`
                WHERE date_window = Date('{date}')
                    AND build_type = '{build_type}'
                    AND branch = '{branch}'
            ) AS prev
            ON j.full_name = prev.full_name
                AND j.suite_folder = prev.suite_folder
                AND j.test_name = prev.test_name
            GROUP BY
                j.full_name,
                j.suite_folder,
                j.test_name,
                j.date_base,
                j.build_type,
                j.branch,
                j.testowners_owners,
                prev.owners
        )
    """
                

def prepare_row_data(row):
    """Prepare a single row for database upsert."""
    # Count status occurrences
    history_list = list(row['history_list'])
    status_counts = {status: history_list.count(status) for status in history_list}
    
    return {
        'suite_folder': row['suite_folder'],
        'test_name': row['test_name'],
        'full_name': row['full_name'],
        'date_window': row['date_base'],
        'days_ago_window': 1,
        'build_type': row['build_type'],
        'branch': row['branch'],
        'owners': row.get('owners'),
        'first_run': row['first_run'],
        'last_run': row['last_run'],
        'history': ','.join(history_list).encode('utf8'),
        'history_class': row['dist_hist'],
        'pass_count': status_counts.get('passed', 0),
        'mute_count': status_counts.get('mute', 0),
        'fail_count': status_counts.get('failure', 0),
        'skip_count': status_counts.get('skipped', 0),
    }


def get_column_types():
    """Get column types for bulk upsert."""
    return (
        ydb.BulkUpsertColumns()
        .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("owners", ydb.OptionalType(ydb.PrimitiveType.Utf8))
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
                

def process_date_range(ydb_wrapper, test_runs_table, testowners_table, flaky_tests_table,
                       build_type, branch, start_date, end_date):
    """Process all dates in the range and collect data for bulk upsert."""
    date_list = [start_date + datetime.timedelta(days=x) 
                 for x in range((end_date - start_date).days + 1)]
    
    print(f'📊 Processing {len(date_list)} dates from {start_date} to {end_date}')
    
    all_prepared_rows = []
    
    for i, date in enumerate(sorted(date_list), 1):
        print(f'📅 Processing date {i}/{len(date_list)}: {date}')
        
        query = build_history_query(
            date, test_runs_table, testowners_table, flaky_tests_table, build_type, branch,
        )
        results = ydb_wrapper.execute_scan_query(
            query, 
            query_name=f"get_flaky_test_history_for_date_{branch}"
        )
        results = _dedupe_history_rows(results)
        print(f'📈 History data captured, {len(results)} rows')
        
        for row in results:
            all_prepared_rows.append(prepare_row_data(row))
    
    return all_prepared_rows


def main():
    """Main function."""
    args = parse_arguments()
    build_type = args['build_type']
    branch = args['branch']
    start_date_override = args['start_date_override']
    end_date_override = args['end_date_override']
    
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
        
        try:
            # Determine start and end dates
            start_date, end_date = determine_start_date(
                ydb_wrapper, test_runs_table, flaky_tests_table,
                build_type, branch, start_date_override, end_date_override
            )
            
            # Final validation
            if start_date > end_date:
                raise ValueError(f"Start date ({start_date}) is after end date ({end_date}); nothing to process")
            
            print(f'📅 Start history date: {start_date}')
            print(f'📅 End history date: {end_date}')
            
            # Create table if it doesn't exist
            create_table_sql = f"""
            CREATE table IF NOT EXISTS `{flaky_tests_table}` (
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
            ydb_wrapper.create_table(flaky_tests_table, create_table_sql)
            
            # Process all dates and collect data
            all_prepared_rows = process_date_range(
                ydb_wrapper, test_runs_table, testowners_table, flaky_tests_table,
                build_type, branch, start_date, end_date
            )
            
            # Insert all data in one batch
            if all_prepared_rows:
                print(f'💾 Upserting {len(all_prepared_rows)} rows of history data')
                column_types = get_column_types()
                ydb_wrapper.bulk_upsert_batches(
                    flaky_tests_table, 
                    all_prepared_rows, 
                    column_types, 
                    batch_size=1000
                )
                print('✅ History updated successfully')
            else:
                print('ℹ️  No data to upload')

        except Exception as e:
            print(f'❌ Script failed: {e}')
            raise


if __name__ == "__main__":
    main()
