#!/usr/bin/env python3
import argparse
import glob
import os
import re
import xml.etree.ElementTree as ET
from mute_utils import MuteTestCheck, mute_target, recalc_suite_info


shard_suffix_re = re.compile(r"-\d+$")


def update_testname(fn, testcase):
    shardname = os.path.splitext(os.path.basename(fn))[0]
    shardname = shard_suffix_re.sub("", shardname)

    clsname = testcase.get("classname")
    tstname = testcase.get("name")
    testcase.set("classname", shardname)

    testcase.set("name", f"{clsname}::{tstname}")
    testcase.set("id", f"{shardname}_{clsname}_{tstname}")

    return f"{shardname}/{clsname}::{tstname}"


def postprocess_yunit(fn, mute_check: MuteTestCheck, dry_run):
    try:
        tree = ET.parse(fn)
    except ET.ParseError as e:
        print(f"Unable to parse {fn}: {e}")
        return

    root = tree.getroot()

    for testsuite in root.findall("testsuite"):
        need_recalc = False
        for testcase in testsuite.findall("testcase"):
            new_name = update_testname(fn, testcase)

            if mute_check(new_name) and mute_target(testcase):
                print(f"mute {new_name}")
                need_recalc = True

        if need_recalc:
            recalc_suite_info(testsuite)

    print(f"{'(dry-run) ' if dry_run else ''}save {fn}")

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

    mute_check = MuteTestCheck(args.filter_file)

    for fn in glob.glob(os.path.join(args.yunit_path, "*.xml")):
        postprocess_yunit(fn, mute_check, args.dry_run)


if __name__ == "__main__":
    main()
