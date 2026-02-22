#!/usr/bin/env python3
"""
Evaluate PR-check rules (scope: pr_check) from test_results/test_runs_column.

Patterns:
  - floating_across_days: timeout failures floating across different tests in a suite over days
  - retry_recovered: fail on first attempt, pass on retry (same job)

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

sys.path.append(os.path.dirname(__file__))
from pattern_rules_loader import load_rules, get_rules_for_build, get_rule_params
from pr_check_patterns import (
    _parse_date,
    pattern_floating_across_days,
    pattern_retry_recovered,
    pattern_muted_test_different_error,
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
            job_name
        FROM `{table}`
        WHERE branch = '{branch}'
          AND build_type = '{build_type}'
          AND run_timestamp >= Date('{start_date}')
          AND (
              job_name = 'PR-check'
              OR job_name LIKE '%PR-check%'
              OR job_name LIKE '%PR_check%'
          )
    """
    try:
        return list(ydb_wrapper.execute_scan_query(query, query_name="pr_check_runs"))
    except Exception as e:
        print(f"Error fetching PR-check data: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Evaluate PR-check rules")
    parser.add_argument('--branch', default='main')
    parser.add_argument('--build_type', default='relwithdebinfo')
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--rules_file', help='Path to pattern_rules.yaml')
    parser.add_argument('--muted_ya_file', help='Path to muted_ya.txt for muted_test_different_error')
    args = parser.parse_args()

    rules = load_rules(args.rules_file)
    pr_rules = [r for r in get_rules_for_build(rules, args.build_type) if r.get('scope') == 'pr_check']
    if not pr_rules:
        print("No PR-check rules found")
        return 0

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            print("YDB credentials not available")
            return 1
        runs = fetch_pr_check_runs(ydb_wrapper, args.branch, args.build_type, args.days)
        if not runs:
            print("No PR-check runs found")
            return 0

    all_matches = []
    for rule in pr_rules:
        pid = rule.get('id', '')
        pattern = rule.get('pattern', '')
        params = get_rule_params(rule, {})
        reaction = rule.get('reaction', 'log')

        if pattern == 'floating_across_days':
            matches = pattern_floating_across_days(runs, params)
        elif pattern == 'retry_recovered':
            matches = pattern_retry_recovered(runs, params)
        elif pattern == 'muted_test_different_error':
            muted = set()
            if args.muted_ya_file and os.path.exists(args.muted_ya_file):
                with open(args.muted_ya_file) as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            parts = line.split(' ', 1)
                            if len(parts) == 2:
                                muted.add((parts[0], parts[1]))
            matches = pattern_muted_test_different_error(runs, muted, params)
        else:
            matches = []

        for m in matches:
            m['rule_id'] = pid
            m['reaction'] = reaction
            all_matches.append(m)

    for m in all_matches:
        print(f"[{m['reaction']}] {m['rule_id']}: {m}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
