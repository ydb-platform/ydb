#!/usr/bin/env python3
"""
Evaluate pattern rules (scope: pr_check, regression) from test_results/test_runs_column.

Patterns:
  - floating_across_days: timeout failures floating across different tests in a suite over days
  - retry_recovered: fail on first attempt, pass on retry (same job)
  - duration_increased: test duration grew significantly (baseline vs recent median)

Usage:
  evaluate_pr_check_rules.py --branch main --build_type relwithdebinfo [--days 7]
"""

import argparse
import datetime
import os
import sys
from collections import defaultdict

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
from ydb_wrapper import YDBWrapper
from mute_decisions import write_pattern_matches

sys.path.append(os.path.dirname(__file__))
from pattern_rules_loader import load_rules, get_rules_for_build, get_rule_params
from behavior_start import find_behavior_start
from regression_jobs import regression_job_names_sql, EXCLUDE_MANUAL_RUNS_SQL
from pr_check_patterns import (
    pattern_floating_across_days,
    pattern_retry_recovered,
    pattern_muted_test_different_error,
    pattern_duration_increased,
)


def fetch_pr_check_runs(ydb_wrapper, branch, build_type, days=7):
    """Fetch PR-check runs from test_results."""
    table = ydb_wrapper.get_table_path("test_results")
    start_date = datetime.date.today() - datetime.timedelta(days=days)
    query = f"""
        SELECT
            run_timestamp,
            full_name,
            suite_folder,
            test_name,
            status,
            error_type,
            job_id,
            job_name,
            commit,
            pull
        FROM `{table}`
        WHERE branch = '{branch}'
          AND build_type = '{build_type}'
          AND run_timestamp >= Date('{start_date}')
          AND (
              job_name = 'PR-check'
              OR job_name LIKE '%PR-check%'
              OR job_name LIKE '%PR_check%'
          )
          {EXCLUDE_MANUAL_RUNS_SQL}
    """
    try:
        return list(ydb_wrapper.execute_scan_query(query, query_name="pr_check_runs"))
    except Exception as e:
        print(f"Error fetching PR-check data: {e}")
        return []


def fetch_regression_runs(ydb_wrapper, branch, build_type, days=7):
    """Fetch all regression runs (for muted_test_different_error etc)."""
    table = ydb_wrapper.get_table_path("test_results")
    start_date = datetime.date.today() - datetime.timedelta(days=days)
    jobs = regression_job_names_sql()
    query = f"""
        SELECT
            run_timestamp,
            full_name,
            suite_folder,
            test_name,
            status,
            error_type,
            commit,
            pull
        FROM `{table}`
        WHERE branch = '{branch}'
          AND build_type = '{build_type}'
          AND run_timestamp >= Date('{start_date}')
          AND job_name IN ({jobs})
          {EXCLUDE_MANUAL_RUNS_SQL}
    """
    try:
        return list(ydb_wrapper.execute_scan_query(query, query_name="regression_runs"))
    except Exception as e:
        print(f"Error fetching regression data: {e}")
        return []


def fetch_regression_runs_with_duration(ydb_wrapper, branch, build_type, days=7):
    """Fetch regression runs with duration from test_results."""
    table = ydb_wrapper.get_table_path("test_results")
    start_date = datetime.date.today() - datetime.timedelta(days=days)
    jobs = regression_job_names_sql()
    query = f"""
        SELECT
            run_timestamp,
            full_name,
            suite_folder,
            test_name,
            status,
            duration,
            commit,
            pull
        FROM `{table}`
        WHERE branch = '{branch}'
          AND build_type = '{build_type}'
          AND run_timestamp >= Date('{start_date}')
          AND duration IS NOT NULL
          AND duration > 0
          AND job_name IN ({jobs})
          {EXCLUDE_MANUAL_RUNS_SQL}
    """
    try:
        return list(ydb_wrapper.execute_scan_query(query, query_name="regression_runs_with_duration"))
    except Exception as e:
        print(f"Error fetching regression duration data: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Evaluate pattern rules (PR-check, regression)")
    parser.add_argument('--branch', default='main')
    parser.add_argument('--build_type', default='relwithdebinfo')
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--rules_file', help='Path to pattern_rules.yaml')
    parser.add_argument('--muted_ya_file', help='Path to muted_ya.txt for muted_test_different_error')
    args = parser.parse_args()

    rules = load_rules(args.rules_file)
    build_rules = get_rules_for_build(rules, args.build_type)
    pr_rules = [r for r in build_rules if r.get('scope') == 'pr_check']
    regression_rules = [r for r in build_rules if r.get('scope') == 'regression']

    if not pr_rules and not regression_rules:
        print("No rules found for this build_type")
        return 0

    muted = set()
    if args.muted_ya_file and os.path.exists(args.muted_ya_file):
        with open(args.muted_ya_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split(' ', 1)
                    if len(parts) == 2:
                        muted.add((parts[0], parts[1]))

    all_matches = []

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("YDB credentials not available")
            return 1

        pr_runs = []
        if pr_rules:
            pr_runs = fetch_pr_check_runs(ydb_wrapper, args.branch, args.build_type, args.days)
            if not pr_runs and pr_rules:
                print("No PR-check runs found")

        regression_runs = []
        regression_runs_all = []
        duration_rules = [r for r in regression_rules if r.get('pattern') == 'duration_increased']
        if duration_rules:
            regression_runs = fetch_regression_runs_with_duration(
                ydb_wrapper, args.branch, args.build_type, args.days
            )
            if not regression_runs and duration_rules:
                print("No regression runs with duration found")
        if any(r.get('pattern') == 'muted_test_different_error' for r in regression_rules):
            regression_runs_all = fetch_regression_runs(ydb_wrapper, args.branch, args.build_type, args.days)

        for rule in pr_rules:
            pid = rule.get('id', '')
            pattern = rule.get('pattern', '')
            params = get_rule_params(rule, {})
            reaction = rule.get('reaction', 'log')

            if pattern == 'floating_across_days':
                matches = pattern_floating_across_days(pr_runs, params)
            elif pattern == 'retry_recovered':
                matches = pattern_retry_recovered(pr_runs, params)
            else:
                matches = []

            for m in matches:
                m['rule_id'] = pid
                m['reaction'] = reaction
                if params.get('find_behavior_start'):
                    d, c, p = find_behavior_start(m, pattern, pr_runs, params)
                    m['behavior_start_date'] = d
                    m['behavior_start_commit'] = c
                    m['behavior_start_pr'] = p
                all_matches.append(m)

        for rule in duration_rules:
            pid = rule.get('id', '')
            params = get_rule_params(rule, {})
            reaction = rule.get('reaction', 'alert')
            matches = pattern_duration_increased(regression_runs, params)
            for m in matches:
                m['rule_id'] = pid
                m['reaction'] = reaction
                if params.get('find_behavior_start'):
                    d, c, p = find_behavior_start(m, 'duration_increased', regression_runs, params)
                    m['behavior_start_date'] = d
                    m['behavior_start_commit'] = c
                    m['behavior_start_pr'] = p
                all_matches.append(m)

        for rule in regression_rules:
            if rule.get('pattern') == 'muted_test_different_error':
                pid = rule.get('id', '')
                params = get_rule_params(rule, {})
                reaction = rule.get('reaction', 'alert')
                matches = pattern_muted_test_different_error(regression_runs_all, muted, params)
                for m in matches:
                    m['rule_id'] = pid
                    m['reaction'] = reaction
                    if params.get('find_behavior_start'):
                        d, c, p = find_behavior_start(m, 'muted_test_different_error', regression_runs_all, params)
                        m['behavior_start_date'] = d
                        m['behavior_start_commit'] = c
                        m['behavior_start_pr'] = p
                    all_matches.append(m)
                break

        for m in all_matches:
            print(f"[{m['reaction']}] {m['rule_id']}: {m}")

        if all_matches:
            try:
                write_pattern_matches(ydb_wrapper, args.branch, args.build_type, all_matches)
            except Exception as e:
                print(f"Failed to write pattern matches to YDB: {e}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
