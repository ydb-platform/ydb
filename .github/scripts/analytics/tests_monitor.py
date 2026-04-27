#!/usr/bin/env python3

import argparse
import datetime
import os
import sys
import time
import ydb
import numpy as np
import pandas as pd
from ydb_wrapper import YDBWrapper

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from github_issue_utils import (
    area_to_owner_map_from_rows,
    compute_effective_analytics_row,
    min_area_by_owner_team_from_rows,
)
from testowners_utils import normalize_github_team_owners_string


def create_tables(ydb_wrapper, table_path):
    print(f"> create table if not exists:'{table_path}'")

    create_sql = f"""
        CREATE table IF NOT EXISTS `{table_path}` (
            `test_name` Utf8 NOT NULL,
            `suite_folder` Utf8 NOT NULL,
            `full_name` Utf8 NOT NULL,
            `date_window` Date NOT NULL,
            `build_type` Utf8 NOT NULL,
            `branch` Utf8 NOT NULL,
            `days_ago_window` Uint64 NOT NULL,
            `history` Utf8,
            `history_class` Utf8,
            `pass_count` Uint64,
            `mute_count` Uint64,
            `fail_count` Uint64,
            `skip_count` Uint64,
            `success_rate` Uint64,
            `summary` Utf8,
            `owner` Utf8,
            `is_muted` Uint32,
            `is_test_chunk` Uint32,
            `state` Utf8,
            `previous_state` Utf8,
            `state_change_date` Date,
            `days_in_state` Uint64,
            `previous_mute_state` Uint32,
            `mute_state_change_date` Date,
            `days_in_mute_state` Uint64,
            `previous_state_filtered` Utf8,
            `state_change_date_filtered` Date,
            `days_in_state_filtered` Uint64,
            `state_filtered` Utf8,
            `effective_area` Utf8,
            `effective_owner_team` Utf8,
            `previous_effective_owner_team` Utf8,
            `effective_owner_team_changed_date` Date,
            PRIMARY KEY (`test_name`, `suite_folder`, `full_name`,date_window, build_type, branch)
        )
            PARTITION BY HASH(build_type,branch)
            WITH (STORE = COLUMN)
        """
    
    ydb_wrapper.create_table(table_path, create_sql)


def process_test_group(name, group, last_day_lookup, default_start_date):
    """Processes data for a single test group (by full_name).

    Used for multi-day backfill where sequential state tracking is needed.
    ``last_day_lookup`` is a dict {full_name: {col: val, …}} for O(1) access.
    """
    state_list_for_filter = ['Muted', 'Muted Flaky', 'Muted Stable', 'Flaky', 'Passed']

    previous_state_list = []
    state_change_date_list = []
    days_in_state_list = []

    previous_mute_state_list = []
    mute_state_change_date_list = []
    days_in_mute_state_list = []

    previous_state_filtered_list = []
    state_change_date_filtered_list = []
    days_in_state_filtered_list = []
    state_filtered_list = []

    prev = last_day_lookup.get(name)
    if prev is not None:
        prev_state = prev['state']
        prev_date = prev['state_change_date']
        current_days_in_state = prev['days_in_state']

        prev_mute_state = prev['is_muted']
        prev_mute_date = prev['mute_state_change_date']
        current_days_in_mute_state = prev['days_in_mute_state']

        prev_state_filtered = prev['state_filtered']
        prev_date_filtered = prev['state_change_date_filtered']
        current_days_in_state_filtered = prev['days_in_state_filtered']

        saved_prev_state = prev['previous_state']
        saved_prev_mute_state = prev['previous_mute_state']
        saved_prev_state_filtered = prev['previous_state_filtered']
    else:
        prev_state = 'no_runs'
        prev_date = datetime.datetime(default_start_date.year, default_start_date.month, default_start_date.day)
        current_days_in_state = 0
        
        prev_mute_state = 0
        prev_mute_date = datetime.datetime(default_start_date.year, default_start_date.month, default_start_date.day)
        current_days_in_mute_state = 0

        state_filtered = ''
        prev_state_filtered = 'no_runs'
        prev_date_filtered = datetime.datetime(
            default_start_date.year, default_start_date.month, default_start_date.day
        )
        current_days_in_state_filtered = 0
        
        saved_prev_state = prev_state
        saved_prev_mute_state = prev_mute_state
        saved_prev_state_filtered = prev_state_filtered

    for index, row in group.iterrows():
        # Process prev state
        current_days_in_state += 1
        if row['state'] != prev_state:
            saved_prev_state = prev_state
            prev_state = row['state']
            prev_date = row['date_window']
            current_days_in_state = 1
        previous_state_list.append(saved_prev_state)
        state_change_date_list.append(prev_date)
        days_in_state_list.append(current_days_in_state)
        
        # Process prev mute state

        current_days_in_mute_state += 1
        if row['is_muted'] != prev_mute_state:
            saved_prev_mute_state = prev_mute_state
            prev_mute_state = row['is_muted']
            prev_mute_date = row['date_window']
            current_days_in_mute_state = 1

        previous_mute_state_list.append(saved_prev_mute_state)
        mute_state_change_date_list.append(prev_mute_date)
        days_in_mute_state_list.append(current_days_in_mute_state)

        # Process filtered states

        if row['state'] not in state_list_for_filter:
            state_filtered = prev_state_filtered
        else:
            state_filtered = row['state']

        current_days_in_state_filtered += 1
        if state_filtered != prev_state_filtered:
            saved_prev_state_filtered = prev_state_filtered
            prev_state_filtered = state_filtered
            prev_date_filtered = row['date_window']
            current_days_in_state_filtered = 1

        state_filtered_list.append(state_filtered)
        previous_state_filtered_list.append(saved_prev_state_filtered)
        state_change_date_filtered_list.append(prev_date_filtered)
        days_in_state_filtered_list.append(current_days_in_state_filtered)

    return {
        'previous_state': previous_state_list,
        'state_change_date': state_change_date_list,
        'days_in_state': days_in_state_list,
        'previous_mute_state': previous_mute_state_list,
        'mute_state_change_date': mute_state_change_date_list,
        'days_in_mute_state': days_in_mute_state_list,
        'previous_state_filtered': previous_state_filtered_list,
        'state_change_date_filtered': state_change_date_filtered_list,
        'days_in_state_filtered': days_in_state_filtered_list,
        'state_filtered': state_filtered_list,
    }


def _utf8_cell(val):
    if val is None:
        return None
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return val


def _load_latest_github_issue_mapping_index(ydb_wrapper, mapping_table: str) -> dict:
    query = f"""
    SELECT full_name, branch, build_type, area_override, area_override_since FROM (
        SELECT
            full_name,
            branch,
            build_type,
            area_override,
            area_override_since,
            ROW_NUMBER() OVER (
                PARTITION BY full_name, branch, build_type
                ORDER BY github_issue_created_at DESC, github_issue_number DESC
            ) AS rn
        FROM `{mapping_table}`
        WHERE github_issue_state IS NOT NULL
          AND Unicode::ToLower(CAST(github_issue_state AS Utf8)) = 'open'
    ) AS ranked
    WHERE rn = 1
    """
    rows = ydb_wrapper.execute_scan_query(query, query_name="tests_monitor_github_issue_index")
    out = {}
    for r in rows:
        key = (
            str(_utf8_cell(r["full_name"])),
            str(_utf8_cell(r["branch"])),
            str(_utf8_cell(r["build_type"])),
        )
        out[key] = {
            "area_override": r.get("area_override"),
            "area_override_since": r.get("area_override_since"),
        }
    return out


def _attach_effective_analytics_columns(df, ydb_wrapper):
    """Fill effective_area / effective_owner_team (same rules as former datamart SQL)."""
    gim_path = ydb_wrapper.get_table_path("github_issue_mapping")
    a2o_path = ydb_wrapper.get_table_path("area_to_owner_mapping")
    gim_by_key = {}
    try:
        gim_by_key = _load_latest_github_issue_mapping_index(ydb_wrapper, gim_path)
    except Exception as exc:
        print(f"Warning: github_issue_mapping unavailable ({exc}); effective_* use owner-only fallback.")
    area_rows = []
    try:
        area_rows = ydb_wrapper.execute_scan_query(
            f"SELECT area, owner_team FROM `{a2o_path}`",
            query_name="tests_monitor_area_to_owner",
        )
    except Exception as exc:
        print(f"Warning: area_to_owner_mapping unavailable ({exc}); effective_* use owner-only fallback.")
    area_to_owner = area_to_owner_map_from_rows(area_rows)
    min_by_owner = min_area_by_owner_team_from_rows(area_rows)
    eff_a, eff_o = [], []
    for _, row in df.iterrows():
        ea, eo = compute_effective_analytics_row(
            row.to_dict(), gim_by_key, area_to_owner, min_by_owner
        )
        eff_a.append(ea)
        eff_o.append(eo)
    df["effective_area"] = eff_a
    df["effective_owner_team"] = eff_o


def _annotate_effective_owner_change_columns(df, last_exist_df):
    """Track analytics owner hand-offs: who we left and the date we switched to current effective_owner_team.

    Compared chronologically per (full_name, branch, build_type), using the previous calendar day's
    ``effective_owner_team`` from ``last_exist_df`` when present. Rows before any change keep NULLs.
    """
    prev_map = {}
    if last_exist_df is not None and len(last_exist_df) > 0 and "effective_owner_team" in last_exist_df.columns:
        for _, r in last_exist_df.iterrows():
            k = (str(r["full_name"]), str(r["branch"]), str(r["build_type"]))
            prev_map[k] = str(r["effective_owner_team"])

    df["previous_effective_owner_team"] = None
    df["effective_owner_team_changed_date"] = None

    for key, group in df.groupby(["full_name", "branch", "build_type"], sort=False):
        g = group.sort_values("date_window")
        immediate = prev_map.get((str(key[0]), str(key[1]), str(key[2])))
        sprev, scd = None, None
        for idx, row in g.iterrows():
            curr = str(row["effective_owner_team"])
            if immediate is not None and immediate != curr:
                sprev = immediate
                dw = row["date_window"]
                scd = dw.date() if isinstance(dw, datetime.datetime) else dw
            df.at[idx, "previous_effective_owner_team"] = sprev
            df.at[idx, "effective_owner_team_changed_date"] = scd
            immediate = curr


def compute_owner(owner):
    if not owner or owner == '':
        return 'unknown'
    elif ';;' in owner:
        parts = owner.split(';;', 1)
        if 'TEAM' in parts[0]:
            return normalize_github_team_owners_string(parts[0])
        else:
            return parts[1]
    else:
        return owner


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--build_type',
        choices=['relwithdebinfo', 'release-asan', 'release-tsan', 'release-msan'],
        default='relwithdebinfo',
        type=str,
        help='build type',
    )
    parser.add_argument('--branch', default='main', type=str, help='branch')
    parser.add_argument('--start-date', dest='start_date', type=str, help='Start date (YYYY-MM-DD), inclusive')
    parser.add_argument('--end-date', dest='end_date', type=str, help='End date (YYYY-MM-DD), inclusive')
    parser.add_argument('--table-suffix', dest='table_suffix', type=str, default=None,
                        help='Append suffix to target table name (e.g. "_temp" → tests_monitor_temp)')

    args, unknown = parser.parse_known_args()
    build_type = args.build_type
    branch = args.branch
    start_date_override = datetime.date.fromisoformat(args.start_date) if args.start_date else None
    end_date_override = datetime.date.fromisoformat(args.end_date) if args.end_date else None
    if start_date_override and end_date_override and start_date_override > end_date_override:
        raise ValueError("start-date must be earlier or equal to end-date")
    
    if start_date_override:
        print(f"➡️  Start date override: {start_date_override}")
    if end_date_override:
        print(f"➡️  End date override: {end_date_override}")

    with YDBWrapper() as ydb_wrapper:        
        if not ydb_wrapper.check_credentials():
            return 1
        
        # Get table paths from config
        test_runs_table = ydb_wrapper.get_table_path("test_results")
        tests_monitor_table = ydb_wrapper.get_table_path("tests_monitor")
        all_tests_table = ydb_wrapper.get_table_path("all_tests_with_owner_and_mute")
        flaky_tests_table = ydb_wrapper.get_table_path("flaky_tests_window")
        
        base_date = datetime.datetime(1970, 1, 1)
        default_start_date = datetime.date(2025, 2, 1)
        actual_today = datetime.date.today()
        today = min(end_date_override, actual_today) if end_date_override else actual_today
        read_table_path = tests_monitor_table
        write_table_path = tests_monitor_table + (args.table_suffix or '')

        def load_monitor_data_for_date(target_date):
            if target_date is None:
                return None
            date_str = target_date.strftime('%Y-%m-%d')
            query = f"""
                SELECT *
                FROM `{read_table_path}`
                WHERE build_type = '{build_type}'
                AND branch = '{branch}'
                AND date_window = Date('{date_str}')
            """
            try:
                results = ydb_wrapper.execute_scan_query(query, query_name=f"get_monitor_data_for_date_{branch}")
            except Exception as e:
                print(f"Error fetching monitor data for {date_str}: {e}")
                return None

            if not results:
                return None

            rows = []
            for row in results:

                def _cell_utf8(col):
                    v = row.get(col)
                    if v is None:
                        return None
                    return v.decode('utf-8') if isinstance(v, bytes) else v

                rec = {
                    'test_name': row['test_name'],
                    'suite_folder': row['suite_folder'],
                    'full_name': row['full_name'],
                    'date_window': base_date + datetime.timedelta(days=row['date_window']),
                    'build_type': row['build_type'],
                    'branch': row['branch'],
                    'days_ago_window': row['days_ago_window'],
                    'history': row['history'],
                    'history_class': row['history_class'],
                    'pass_count': row['pass_count'],
                    'mute_count': row['mute_count'],
                    'fail_count': row['fail_count'],
                    'skip_count': row['skip_count'],
                    'success_rate': row['success_rate'],
                    'summary': row['summary'],
                    'owners': row['owner'],
                    'is_muted': row['is_muted'],
                    'is_test_chunk': row['is_test_chunk'],
                    'state': row['state'],
                    'previous_state': row['previous_state'],
                    'state_change_date': base_date + datetime.timedelta(days=row['state_change_date']),
                    'days_in_state': row['days_in_state'],
                    'previous_mute_state': row['previous_mute_state'],
                    'mute_state_change_date': base_date + datetime.timedelta(days=row['mute_state_change_date']),
                    'days_in_mute_state': row['days_in_mute_state'],
                    'previous_state_filtered': row['previous_state_filtered'],
                    'state_change_date_filtered': base_date + datetime.timedelta(days=row['state_change_date_filtered']),
                    'days_in_state_filtered': row['days_in_state_filtered'],
                    'state_filtered': row['state_filtered'],
                }
                if row.get('effective_area') is not None:
                    rec['effective_area'] = _cell_utf8('effective_area')
                if row.get('effective_owner_team') is not None:
                    rec['effective_owner_team'] = _cell_utf8('effective_owner_team')
                if row.get('previous_effective_owner_team') is not None:
                    rec['previous_effective_owner_team'] = _cell_utf8('previous_effective_owner_team')
                if row.get('effective_owner_team_changed_date') is not None:
                    rec['effective_owner_team_changed_date'] = base_date + datetime.timedelta(
                        days=row['effective_owner_team_changed_date']
                    )
                rows.append(rec)

            return pd.DataFrame(rows)

        # Get last existing day
        print("Getting date of last collected monitor data")
        query_last_exist_day = f"""
            SELECT MAX(date_window) AS last_exist_day
            FROM `{read_table_path}`
            WHERE build_type = '{build_type}'
            AND branch = '{branch}'
        """
        
        try:
            results = ydb_wrapper.execute_scan_query(query_last_exist_day, query_name=f"get_max_monitor_date_{branch}")
            last_exist_day = results[0]['last_exist_day'] if results else None
        except Exception as e:
            print(f"Error during fetching last existing day: {e}")
            last_exist_day = None

        last_exist_df = None

        if start_date_override:
            process_start_date = max(start_date_override, default_start_date)
            if process_start_date != start_date_override:
                print(f"Requested start date {start_date_override} is earlier than supported minimum {default_start_date}. Using {process_start_date} instead.")
            if process_start_date > today:
                print(f"Requested start date {process_start_date} is after end date {today}. Nothing to process.")
                return 0

            prev_day = process_start_date - datetime.timedelta(days=1)
            if prev_day >= default_start_date:
                last_exist_df = load_monitor_data_for_date(prev_day)
            date_list = [process_start_date + datetime.timedelta(days=x) for x in range((today - process_start_date).days + 1)]
            print(f"Recalculating monitor data for custom range {process_start_date} - {today}")
        elif last_exist_day is None:
            print(f"Monitor data do not exist for branch '{branch}' - checking when branch was created")

            query_branch_creation = f"""
                SELECT MIN(run_timestamp) as earliest_run
                FROM `{test_runs_table}`
                WHERE branch = '{branch}' AND build_type = '{build_type}'
            """

            try:
                results = ydb_wrapper.execute_scan_query(query_branch_creation, query_name=f"get_branch_creation_date_{branch}")
                branch_creation_date = None

                if results and results[0]['earliest_run']:
                    earliest_run = results[0]['earliest_run']
                    try:
                        if earliest_run > 1000000000000000:
                            timestamp_seconds = earliest_run / 1000000
                            branch_creation_date = datetime.datetime.fromtimestamp(timestamp_seconds).date()
                            print(f"Converted from microseconds: {branch_creation_date}")
                        elif earliest_run > 1000000000000:
                            timestamp_seconds = earliest_run / 1000
                            branch_creation_date = datetime.datetime.fromtimestamp(timestamp_seconds).date()
                            print(f"Converted from milliseconds: {branch_creation_date}")
                        else:
                            branch_creation_date = datetime.datetime.fromtimestamp(earliest_run).date()
                            print(f"Converted from seconds: {branch_creation_date}")
                    except (OSError, OverflowError, ValueError) as e:
                        print(f"Error converting timestamp {earliest_run} to datetime: {e}")
                        branch_creation_date = None
            except Exception as e:
                print(f"Error fetching branch creation date: {e}")
                branch_creation_date = None

            if branch_creation_date:
                process_start_date = max(branch_creation_date, default_start_date)
                print(f"Found branch creation date: {branch_creation_date}")
            else:
                process_start_date = max(today - datetime.timedelta(days=7), default_start_date)
                print(f"No test runs found for branch, using 1 week ago: {process_start_date}")

            date_list = [process_start_date + datetime.timedelta(days=x) for x in range((today - process_start_date).days + 1)]
            print(f"Init new monitor collecting from date {process_start_date}")
        else:
            last_exist_day_date = (base_date + datetime.timedelta(days=last_exist_day)).date()
            if last_exist_day_date >= today:
                last_exist_day_date = last_exist_day_date - datetime.timedelta(days=1)

            # Reprocess last existing day to catch late-arriving test runs
            # (e.g. Nightly jobs that finish after midnight may upload results after tests_monitor already ran)
            process_start_date = max(last_exist_day_date, default_start_date)
            prev_day = process_start_date - datetime.timedelta(days=1)
            if prev_day >= default_start_date:
                last_exist_df = load_monitor_data_for_date(prev_day)
            print(f"Monitor data exist - reprocessing from {process_start_date} (last recorded: {last_exist_day_date})")

            if process_start_date > today:
                print("No new dates to process.")
                return 0
            date_list = [process_start_date + datetime.timedelta(days=x) for x in range((today - process_start_date).days + 1)]

        # Get data from flaky_tests_window table for requested dates
        data = {
            'test_name': [],
            'suite_folder': [],
            'full_name': [],
            'date_window': [],
            'build_type': [],
            'branch': [],
            'owners': [],
            'days_ago_window': [],
            'history': [],
            'history_class': [],
            'pass_count': [],
            'mute_count': [],
            'fail_count': [],
            'skip_count': [],
            'is_muted': [],
        }

        thirty_days_ago_ts = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)
        ).strftime('%Y-%m-%dT%H:%M:%SZ')

        print(f'Getting aggregated history for {len(date_list)} day(s): {date_list[0]} .. {date_list[-1]}')
        for date in sorted(date_list):
            query_get_history = f"""
                SELECT 
                    hist.branch AS branch,
                    hist.build_type AS build_type,
                    hist.date_window AS date_window,
                    hist.days_ago_window AS days_ago_window,
                    hist.fail_count AS fail_count,
                    hist.full_name AS full_name,
                    hist.history AS history,
                    hist.history_class AS history_class,
                    hist.mute_count AS mute_count,
                    owners_t.owners AS owners,
                    hist.pass_count AS pass_count,
                    owners_t.is_muted AS is_muted,
                    hist.skip_count AS skip_count,
                    hist.suite_folder AS suite_folder,
                    hist.test_name AS test_name
                FROM (
                    SELECT * FROM
                    `{flaky_tests_table}` 
                    WHERE 
                    date_window = Date('{date}')
                    AND build_type = '{build_type}' 
                    AND branch = '{branch}'
                ) AS hist 
                INNER JOIN (
                    SELECT 
                        test_name,
                        suite_folder,
                        owners,
                        is_muted,
                        date,
                        build_type
                    FROM 
                        `{all_tests_table}`
                    WHERE 
                        branch = '{branch}'
                        AND build_type = '{build_type}'
                        AND date = Date('{date}')
                        AND run_timestamp_last >= Timestamp('{thirty_days_ago_ts}')
                ) AS owners_t
                ON 
                    hist.test_name = owners_t.test_name
                    AND hist.suite_folder = owners_t.suite_folder
                    AND hist.date_window = owners_t.date
                    AND hist.build_type = owners_t.build_type;
            """
            results = ydb_wrapper.execute_scan_query(query_get_history, query_name=f"get_monitor_history_for_date_{branch}")

            if results:
                for row in results:
                    data['test_name'].append(row['test_name'])
                    data['suite_folder'].append(row['suite_folder'])
                    data['full_name'].append(row['full_name'])
                    data['date_window'].append(base_date + datetime.timedelta(days=row['date_window']))
                    data['build_type'].append(row['build_type'])
                    data['branch'].append(row['branch'])
                    data['owners'].append(row['owners'])
                    data['days_ago_window'].append(row['days_ago_window'])
                    data['history'].append(
                        row['history'].decode('utf-8') if isinstance(row['history'], bytes) else row['history']
                    )
                    data['history_class'].append(
                        row['history_class'].decode('utf-8')
                        if isinstance(row['history_class'], bytes)
                        else row['history_class']
                    )
                    data['pass_count'].append(row['pass_count'])
                    data['mute_count'].append(row['mute_count'])
                    data['fail_count'].append(row['fail_count'])
                    data['skip_count'].append(row['skip_count'])
                    data['is_muted'].append(row['is_muted'])

            else:
                print(
                    f"Warning: No data found in flaky_tests_window for date {date} build_type='{build_type}', branch='{branch}'"
                )

        start_time = time.time()
        df = pd.DataFrame(data)

        if df.empty:
            print(f"No test data found for branch='{branch}', build_type='{build_type}' in the date range. Nothing to process.")
            return 0

        # Build dict lookup from last day (O(1) per test instead of O(N) DataFrame scan)
        last_day_lookup = {}
        if last_exist_df is not None and last_exist_df.shape[0] > 0:
            prev_cols = [
                'full_name', 'state', 'previous_state', 'state_change_date', 'days_in_state',
                'is_muted', 'previous_mute_state', 'mute_state_change_date', 'days_in_mute_state',
                'state_filtered', 'previous_state_filtered', 'state_change_date_filtered',
                'days_in_state_filtered',
            ]
            last_day_lookup = last_exist_df[prev_cols].set_index('full_name').to_dict('index')

        end_time = time.time()
        print(f'Dataframe inited: {end_time - start_time}')
        start_time = time.time()

        df = df.sort_values(by=['full_name', 'date_window'])

        end_time = time.time()
        print(f'Dataframe sorted: {end_time - start_time}')
        start_time = time.time()

        # Vectorized base params (replaces per-row apply)
        total = df['pass_count'] + df['mute_count'] + df['fail_count']
        df['success_rate'] = np.where(total > 0, df['pass_count'] / total * 100, 0).astype(int)

        df['summary'] = (
            'Pass:' + df['pass_count'].astype(str)
            + ' Fail:' + df['fail_count'].astype(str)
            + ' Mute:' + df['mute_count'].astype(str)
            + ' Skip:' + df['skip_count'].astype(str)
        )

        df['owner'] = df['owners'].apply(compute_owner)

        df['is_test_chunk'] = df['full_name'].str.contains(
            ']? chunk|sole chunk|chunk chunk|chunk\\+chunk', regex=True, na=False,
        ).astype(int)
        df['is_muted'] = df['is_muted'].fillna(0).astype(int)

        # Vectorized state: (is_muted, history_class) -> state
        hc = df['history_class'].fillna('')
        im = df['is_muted']
        has_mute    = hc.str.contains('mute', na=False)
        has_failure = hc.str.contains('failure', na=False)
        has_pass    = hc.str.contains('pass', na=False)
        has_skipped = hc.str.contains('skipped', na=False)
        muted       = (im == 1)
        not_muted   = ~muted

        #                    condition                          -> state
        state_conditions = [
            (muted     & (has_mute | has_failure),               'Muted Flaky'),
            (muted     & has_pass & ~has_failure & ~has_mute,    'Muted Stable'),
            (muted     & has_skipped,                            'Skipped'),
            (muted,                                              'no_runs'),
            (not_muted & has_failure & ~has_mute,                'Flaky'),
            (not_muted & has_mute,                               'Muted'),
            (not_muted & has_pass,                               'Passed'),
            (not_muted & has_skipped,                            'Skipped'),
        ]
        df['state'] = np.select(
            [cond for cond, _ in state_conditions],
            [name for _, name in state_conditions],
            default='no_runs',
        )

        end_time = time.time()
        print(f'Computed base params: {end_time - start_time}')
        start_time = time.time()

        # State tracking (days_in_state, transitions, etc.)
        default_dt = datetime.datetime(
            default_start_date.year, default_start_date.month, default_start_date.day,
        )
        num_dates = len(date_list)

        if num_dates == 1:
            # Fast path: single day → one row per test, fully vectorized via merge
            _STATE_FILTER_SET = {'Muted', 'Muted Flaky', 'Muted Stable', 'Flaky', 'Passed'}

            defaults = {
                'prev_state': 'no_runs',
                'prev_previous_state': 'no_runs',
                'prev_state_change_date': default_dt,
                'prev_days_in_state': 0,
                'prev_is_muted': 0,
                'prev_previous_mute_state': 0,
                'prev_mute_state_change_date': default_dt,
                'prev_days_in_mute_state': 0,
                'prev_state_filtered': 'no_runs',
                'prev_previous_state_filtered': 'no_runs',
                'prev_state_change_date_filtered': default_dt,
                'prev_days_in_state_filtered': 0,
            }

            if last_day_lookup:
                prev_df = pd.DataFrame.from_dict(last_day_lookup, orient='index')
                prev_df.index.name = 'full_name'
                prev_df = prev_df.add_prefix('prev_').reset_index()
                df = df.merge(prev_df, on='full_name', how='left')
                for col, val in defaults.items():
                    df[col] = df[col].fillna(val)
            else:
                for col, val in defaults.items():
                    df[col] = val

            int_cols = [c for c, v in defaults.items() if isinstance(v, int)]
            for col in int_cols:
                df[col] = df[col].astype(int)

            # State transitions
            state_changed = df['state'] != df['prev_state']
            df['previous_state'] = df['prev_state'].where(state_changed, df['prev_previous_state'])
            df['state_change_date'] = df['date_window'].where(state_changed, df['prev_state_change_date'])
            df['days_in_state'] = np.where(state_changed, 1, df['prev_days_in_state'] + 1)

            # Mute state transitions
            mute_changed = df['is_muted'] != df['prev_is_muted']
            df['previous_mute_state'] = df['prev_is_muted'].where(mute_changed, df['prev_previous_mute_state'])
            df['mute_state_change_date'] = df['date_window'].where(mute_changed, df['prev_mute_state_change_date'])
            df['days_in_mute_state'] = np.where(mute_changed, 1, df['prev_days_in_mute_state'] + 1)

            # Filtered state transitions
            in_filter = df['state'].isin(_STATE_FILTER_SET)
            df['state_filtered'] = df['state'].where(in_filter, df['prev_state_filtered'])
            filtered_changed = df['state_filtered'] != df['prev_state_filtered']
            df['previous_state_filtered'] = df['prev_state_filtered'].where(
                filtered_changed, df['prev_previous_state_filtered'],
            )
            df['state_change_date_filtered'] = df['date_window'].where(
                filtered_changed, df['prev_state_change_date_filtered'],
            )
            df['days_in_state_filtered'] = np.where(
                filtered_changed, 1, df['prev_days_in_state_filtered'] + 1,
            )

            df.drop(columns=[c for c in df.columns if c.startswith('prev_')], inplace=True)

        else:
            # Multi-day backfill: sequential per-group processing with dict lookup
            previous_state_list = []
            state_change_date_list = []
            days_in_state_list = []
            previous_mute_state_list = []
            mute_state_change_date_list = []
            days_in_mute_state_list = []
            previous_state_filtered_list = []
            state_change_date_filtered_list = []
            days_in_state_filtered_list = []
            state_filtered_list = []
            for name, group in df.groupby('full_name'):
                result = process_test_group(name, group, last_day_lookup, default_start_date)
                previous_state_list.extend(result['previous_state'])
                state_change_date_list.extend(result['state_change_date'])
                days_in_state_list.extend(result['days_in_state'])
                previous_mute_state_list.extend(result['previous_mute_state'])
                mute_state_change_date_list.extend(result['mute_state_change_date'])
                days_in_mute_state_list.extend(result['days_in_mute_state'])
                previous_state_filtered_list.extend(result['previous_state_filtered'])
                state_change_date_filtered_list.extend(result['state_change_date_filtered'])
                days_in_state_filtered_list.extend(result['days_in_state_filtered'])
                state_filtered_list.extend(result['state_filtered'])

            df['previous_state'] = previous_state_list
            df['state_change_date'] = state_change_date_list
            df['days_in_state'] = days_in_state_list
            df['previous_mute_state'] = previous_mute_state_list
            df['mute_state_change_date'] = mute_state_change_date_list
            df['days_in_mute_state'] = days_in_mute_state_list
            df['previous_state_filtered'] = previous_state_filtered_list
            df['state_change_date_filtered'] = state_change_date_filtered_list
            df['days_in_state_filtered'] = days_in_state_filtered_list
            df['state_filtered'] = state_filtered_list

        end_time = time.time()
        print(f'Computed days_in_state, state_change_date, previous_state and other params: {end_time - start_time}')
        start_time = time.time()

        df['date_window'] = df['date_window'].dt.date
        df['state_change_date'] = df['state_change_date'].dt.date
        df['days_in_state'] = df['days_in_state'].astype(int)
        df['previous_mute_state'] = df['previous_mute_state'].astype(int)
        df['mute_state_change_date'] = df['mute_state_change_date'].dt.date
        df['days_in_mute_state'] = df['days_in_mute_state'].astype(int)
        df['state_change_date_filtered'] = df['state_change_date_filtered'].dt.date
        df['days_in_state_filtered'] = df['days_in_state_filtered'].astype(int)

        end_time = time.time()
        print(f'Converting types of columns: {end_time - start_time}')
        start_time = time.time()

        _attach_effective_analytics_columns(df, ydb_wrapper)
        _annotate_effective_owner_change_columns(df, last_exist_df)

        end_time = time.time()
        print(f'Effective analytics columns: {end_time - start_time}')
        start_time = time.time()

        result = df[
            [
                'full_name',
                'date_window',
                'suite_folder',
                'test_name',
                'days_ago_window',
                'build_type',
                'branch',
                'history',
                'history_class',
                'pass_count',
                'mute_count',
                'fail_count',
                'skip_count',
                'summary',
                'owner',
                'is_test_chunk',
                'is_muted',
                'state',
                'previous_state',
                'state_change_date',
                'days_in_state',
                'previous_mute_state',
                'mute_state_change_date',
                'days_in_mute_state',
                'previous_state_filtered',
                'state_change_date_filtered',
                'days_in_state_filtered',
                'state_filtered',
                'success_rate',
                'effective_area',
                'effective_owner_team',
                'previous_effective_owner_team',
                'effective_owner_team_changed_date',
            ]
        ]

        end_time = time.time()
        print(f'Dataframe prepared {end_time - start_time}')
        print(f'Data collected, {len(result)} rows')


        start_time = time.time()
        prepared_for_update_rows = result.to_dict('records')
        end_time = time.time()
        print(f'Data converted to dict for upsert: {end_time - start_time}')

        start_upsert_time = time.time()

        # Create table and bulk upsert using ydb_wrapper
        create_tables(ydb_wrapper, write_table_path)

        chunk_size = 1000

        # Prepare column_types once
        column_types = (
            ydb.BulkUpsertColumns()
            .add_column("test_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("suite_folder", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("build_type", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("branch", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("full_name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("date_window", ydb.OptionalType(ydb.PrimitiveType.Date))
            .add_column("days_ago_window", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            .add_column("history", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("history_class", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("pass_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            .add_column("mute_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            .add_column("fail_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            .add_column("skip_count", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            .add_column("success_rate", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            .add_column("summary", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("owner", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("is_muted", ydb.OptionalType(ydb.PrimitiveType.Uint32))
            .add_column("is_test_chunk", ydb.OptionalType(ydb.PrimitiveType.Uint32))
            .add_column("state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("previous_state", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("state_change_date", ydb.OptionalType(ydb.PrimitiveType.Date))
            .add_column("days_in_state", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            .add_column("previous_mute_state", ydb.OptionalType(ydb.PrimitiveType.Uint32))
            .add_column("days_in_mute_state", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            .add_column("mute_state_change_date", ydb.OptionalType(ydb.PrimitiveType.Date))
            .add_column("previous_state_filtered", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("state_change_date_filtered", ydb.OptionalType(ydb.PrimitiveType.Date))
            .add_column("days_in_state_filtered", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            .add_column("state_filtered", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("effective_area", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("effective_owner_team", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("previous_effective_owner_team", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("effective_owner_team_changed_date", ydb.OptionalType(ydb.PrimitiveType.Date))
        )
        
        ydb_wrapper.bulk_upsert_batches(
            write_table_path, prepared_for_update_rows, column_types, chunk_size,
            query_name=f"tests_monitor_{branch}_{build_type}"
        )

        end_time = time.time()
        print(f'monitor data upserted: {end_time - start_upsert_time}')


if __name__ == "__main__":
    main()