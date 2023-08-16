#!/usr/bin/env python3
import argparse
import glob
import os
import xml.etree.ElementTree as ET
from mute_utils import MuteTestCheck, mute_target, recalc_suite_info
from junit_utils import get_property_value


def update_testname(testcase):
    filename = get_property_value(testcase, "filename")

    clsname = testcase.get("classname")
    tstname = testcase.get("name")

    if filename is None:
        return f"{clsname}::{tstname}"

    filename = filename.split("/")
    test_fn = filename[-1]
    folder = "/".join(filename[:-1])

    testcase.set("classname", folder)

    clsname = clsname.split(".")[-1]

    test_name = f"{test_fn}::{clsname}::{tstname}"

    testcase.set("name", test_name)
    testcase.set("id", f"{folder}_{test_fn}_{clsname}_{tstname}")

    return f"{folder}/{test_name}"


def postprocess_pytest(fn, mute_check, dry_run):
    tree = ET.parse(fn)
    root = tree.getroot()

    for testsuite in root.findall("testsuite"):
        need_recalc = False
        for testcase in testsuite.findall("testcase"):
            new_name = update_testname(testcase)
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
    parser.add_argument("pytest_xml_path")

    args = parser.parse_args()

    if not os.path.isdir(args.pytest_xml_path):
        print(f"{args.pytest_xml_path} is not a directory, exit")
        raise SystemExit(-1)

    mute_check = MuteTestCheck(args.filter_file)

    for fn in glob.glob(os.path.join(args.pytest_xml_path, "*.xml")):
        postprocess_pytest(fn, mute_check, args.dry_run)


if __name__ == "__main__":
    main()
