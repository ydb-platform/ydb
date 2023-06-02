#!/usr/bin/env python3
import argparse
from typing import TextIO
import xml.etree.ElementTree as ET

from log_parser import ctest_log_parser, log_reader
from mute_utils import mute_target, remove_failure, update_suite_info, MutedShardCheck


def find_targets_to_remove(log_fp):
    return {target for target, reason, _ in ctest_log_parser(log_fp) if reason == "Failed"}


def postprocess_ctest(log_fp: TextIO, ctest_junit_report, is_mute_shard, dry_run):
    to_remove = find_targets_to_remove(log_fp)
    tree = ET.parse(ctest_junit_report)
    root = tree.getroot()
    n_remove_failures = n_skipped = 0

    for testcase in root.findall("testcase"):
        target = testcase.attrib["classname"]

        if is_mute_shard(target):
            if mute_target(testcase):
                print(f"mute {target}")
                testcase.set("status", "run")  # CTEST specific
                n_remove_failures += 1
                n_skipped += 1
        elif target in to_remove:
            print(f"set {target} as passed")
            n_remove_failures += 1
            remove_failure(testcase)

    if n_remove_failures:
        update_suite_info(root, n_remove_failures, n_skipped)
        print(f"{'(dry-run) ' if dry_run else ''}update {ctest_junit_report}")
        if not dry_run:
            tree.write(ctest_junit_report, xml_declaration=True, encoding="UTF-8")
    else:
        print("nothing to remove")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--filter-file", required=False)
    parser.add_argument("--decompress", action="store_true", default=False, help="decompress ctest log")
    parser.add_argument("ctest_log", type=str)
    parser.add_argument("ctest_junit_report")
    args = parser.parse_args()

    log = log_reader(args.ctest_log, args.decompress)
    is_mute_shard = MutedShardCheck(args.filter_file)
    postprocess_ctest(log, args.ctest_junit_report, is_mute_shard, args.dry_run)


if __name__ == "__main__":
    main()
