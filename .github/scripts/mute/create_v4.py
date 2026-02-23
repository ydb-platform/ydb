#!/usr/bin/env python3
"""
Mute v4 Direct: reads from test_results (test_runs_column) only.
No dependency on flaky_tests_window or tests_monitor.
"""

import argparse
import logging
import os
import sys

# Paths for imports from tests/ and analytics/
_script_dir = os.path.dirname(os.path.abspath(__file__))
_scripts_dir = os.path.dirname(_script_dir)
sys.path.insert(0, os.path.join(_scripts_dir, 'tests'))
sys.path.insert(0, os.path.join(_scripts_dir, 'analytics'))

from mute_check import YaMuteCheck
from ydb_wrapper import YDBWrapper
from .decisions import write_mute_decisions

from .logic import aggregate_test_data, get_quarantine_graduation
from .rules import (
    get_delete_rule,
    get_mute_rule,
    get_quarantine_graduation_rule,
    get_rule_params,
    get_unmute_rule,
    load_rules,
)
from .data import fetch_from_test_results
from .apply import apply_and_add_mutes
from .utils import parse_mute_file

DEFAULT_MUTE_DAYS = 4
DEFAULT_UNMUTE_DAYS = 7
DEFAULT_DELETE_DAYS = 7

MUTED_YA_PATH = '.github/config/muted_ya.txt'
QUARANTINE_PATH = '.github/config/quarantine.txt'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _aggregate_with_logging(all_data, period_days):
    logging.info(f"Starting aggregation for {period_days} days period...")
    result = aggregate_test_data(all_data, period_days)
    logging.info(f"Aggregation completed: {len(result)} unique tests")
    return result


def run_v4_direct(args):
    """Run mute logic using test_results directly."""
    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1

        logging.info("Starting mute v4 direct (test_results only)")
        logging.info(f"Branch: {args.branch}, build_type: {args.build_type}")

        input_muted_ya_path = getattr(args, 'muted_ya_file', MUTED_YA_PATH)
        mute_check = YaMuteCheck()
        mute_check.load(input_muted_ya_path)
        logging.info(f"Loaded muted_ya with {len(mute_check.regexps)} patterns")

        quarantine_check = None
        input_quarantine_path = getattr(args, 'quarantine_file', QUARANTINE_PATH)
        if os.path.exists(input_quarantine_path):
            quarantine_check = YaMuteCheck()
            quarantine_check.load(input_quarantine_path)
            logging.info(f"Loaded quarantine with {len(quarantine_check.regexps)} tests")
        else:
            logging.info(f"Quarantine file not found: {input_quarantine_path}")

        build_type = getattr(args, 'build_type', 'relwithdebinfo')
        rules_path = getattr(args, 'rules_file', None)
        rules = load_rules(rules_path)
        mute_rule = get_mute_rule(rules, build_type)
        unmute_rule = get_unmute_rule(rules, build_type)
        delete_rule = get_delete_rule(rules, build_type)
        graduation_rule = get_quarantine_graduation_rule(rules, build_type)

        mute_days = get_rule_params(mute_rule, {}).get('window_days', DEFAULT_MUTE_DAYS) if mute_rule else DEFAULT_MUTE_DAYS
        unmute_days = get_rule_params(unmute_rule, {}).get('window_days', DEFAULT_UNMUTE_DAYS) if unmute_rule else DEFAULT_UNMUTE_DAYS
        delete_days = get_rule_params(delete_rule, {}).get('window_days', DEFAULT_DELETE_DAYS) if delete_rule else DEFAULT_DELETE_DAYS
        graduation_params = get_rule_params(graduation_rule, {}) if graduation_rule else {}
        grad_window = graduation_params.get('window_days', 1)

        mute_rule_params = get_rule_params(mute_rule, {}) if mute_rule else {}
        unmute_rule_params = get_rule_params(unmute_rule, {}) if unmute_rule else {}
        delete_rule_params = get_rule_params(delete_rule, {}) if delete_rule else {}

        all_data = fetch_from_test_results(
            ydb_wrapper, args.branch, build_type, days_window=7, mute_check=mute_check
        )
        logging.info(f"Fetched {len(all_data)} test records from test_results")

        aggregated_for_mute = _aggregate_with_logging(all_data, mute_days)
        aggregated_for_unmute = _aggregate_with_logging(all_data, unmute_days)
        aggregated_for_delete = _aggregate_with_logging(all_data, delete_days)
        aggregated_1day = _aggregate_with_logging(all_data, grad_window)

        to_graduated = set()
        if quarantine_check and input_quarantine_path and os.path.exists(input_quarantine_path):
            quarantine_tests = parse_mute_file(input_quarantine_path)
            to_graduated = get_quarantine_graduation(quarantine_tests, aggregated_1day, graduation_params)
            if to_graduated:
                updated_quarantine = quarantine_tests - to_graduated
                with open(input_quarantine_path, 'w') as f:
                    f.write('\n'.join(sorted(updated_quarantine)) + '\n')
                logging.info(f"Quarantine graduation: removed {len(to_graduated)} tests")

        os.makedirs(args.output_folder, exist_ok=True)

        result = apply_and_add_mutes(
            all_data,
            args.output_folder,
            mute_check,
            aggregated_for_mute,
            aggregated_for_unmute,
            aggregated_for_delete,
            quarantine_check=quarantine_check,
            to_graduated=to_graduated,
            mute_rule_params=mute_rule_params,
            unmute_rule_params=unmute_rule_params,
            delete_rule_params=delete_rule_params,
            output_file=getattr(args, 'output_file', None),
        )
        to_mute, to_unmute, to_delete, to_mute_debug, to_unmute_debug, to_delete_debug = result

        try:
            mute_rule_id = mute_rule.get("id", "regression_flaky_mute") if mute_rule else "regression_flaky_mute"
            unmute_rule_id = unmute_rule.get("id", "regression_stable_unmute") if unmute_rule else "regression_stable_unmute"
            delete_rule_id = delete_rule.get("id", "regression_no_runs_delete") if delete_rule else "regression_no_runs_delete"
            grad_rule_id = graduation_rule.get("id", "quarantine_graduation") if graduation_rule else "quarantine_graduation"
            write_mute_decisions(
                ydb_wrapper,
                args.branch,
                build_type,
                to_mute, to_unmute, to_delete,
                mute_rule_id=mute_rule_id,
                unmute_rule_id=unmute_rule_id,
                delete_rule_id=delete_rule_id,
                graduation_rule_id=grad_rule_id,
                to_graduated=to_graduated,
                to_mute_debug=to_mute_debug,
                to_unmute_debug=to_unmute_debug,
                to_delete_debug=to_delete_debug,
                system_version=getattr(args, "system_version", "v4_direct"),
            )
        except Exception as e:
            logging.warning(f"Failed to write mute_decisions: {e}")

        return 0


def main():
    parser = argparse.ArgumentParser(description="Mute v4 Direct: test_results only")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--build_type", default="relwithdebinfo")
    parser.add_argument("--muted_ya_file", default=None)
    parser.add_argument("--quarantine_file", default=None)
    parser.add_argument("--output_folder", default="comparison/v4_direct")
    parser.add_argument("--rules_file", default=None)
    parser.add_argument("--output_file", default=None)
    parser.add_argument("--system-version", "--system_version", dest="system_version", default="v4_direct")
    args = parser.parse_args()

    repo_root = os.path.join(_script_dir, '..', '..', '..')
    if args.muted_ya_file is None:
        args.muted_ya_file = os.path.join(repo_root, '.github', 'config', 'muted_ya.txt')
    if args.quarantine_file is None:
        args.quarantine_file = os.path.join(repo_root, '.github', 'config', 'quarantine.txt')

    return run_v4_direct(args)


if __name__ == "__main__":
    sys.exit(main())
