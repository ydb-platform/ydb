#!/usr/bin/env python3
import os
import glob
import argparse
import xml.etree.ElementTree as ET
from mute_utils import mute_target, update_suite_info, MutedTestCheck


def case_iterator(root):
    for case in root.findall("testcase"):
        cls, method = case.attrib["classname"], case.attrib["name"]
        yield case, cls, method


def mute_junit(is_mute_test, folder, dry_run):
    for fn in glob.glob(os.path.join(folder, "*.xml")):
        tree = ET.parse(fn)
        root = tree.getroot()
        total_muted = 0
        for suite in root.findall("testsuite"):
            muted_cnt = 0
            for case, cls, method in case_iterator(suite):
                if is_mute_test(cls, method):
                    if mute_target(case):
                        print(f"mute {cls}::{method}")
                        muted_cnt += 1

            if muted_cnt:
                update_suite_info(suite, n_skipped=muted_cnt, n_remove_failures=muted_cnt)
                total_muted += muted_cnt

        if total_muted:
            update_suite_info(root, n_skipped=total_muted, n_remove_failures=total_muted)

            print(f"{'(dry-run) ' if dry_run else ''}patch {fn}")

            if not dry_run:
                tree.write(fn, xml_declaration=True, encoding="UTF-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--filter-file", required=True)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("yunit_path")
    args = parser.parse_args()

    if not os.path.isdir(args.yunit_path):
        print(f"{args.yunit_path} is not a directory, exit")
        raise SystemExit(-1)

    # FIXME: add gtest filter file ?
    is_mute_test = MutedTestCheck(args.filter_file)

    if not is_mute_test.has_rules:
        print("nothing to mute")
        return

    mute_junit(is_mute_test, args.yunit_path, args.dry_run)


if __name__ == "__main__":
    main()
