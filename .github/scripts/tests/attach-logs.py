#!/usr/bin/env python3
import argparse
import io
import os
import glob
import re
from xml.etree import ElementTree as ET
from pathlib import Path
from typing import List
from log_parser import ctest_log_parser, parse_yunit_fails, parse_gtest_fails, log_reader
from junit_utils import add_junit_log_property, create_error_testcase, create_error_testsuite, suite_case_iterator
from ctest_utils import CTestLog

fn_shard_part_re = re.compile(r"-\d+$")


def make_filename(n, *parts):
    fn = f'{"-".join(parts)}'

    if n > 0:
        fn = f"{fn}-{n}"

    return f"{fn}.log"


def save_log(err_lines: List[str], out_path: Path, *parts):
    for x in range(128):
        fn = make_filename(x, *parts)
        print(f"save {fn} for {'::'.join(parts)}")
        path = out_path.joinpath(fn)
        try:
            with open(path, "xt") as fp:
                for line in err_lines:
                    fp.write(f"{line}\n")
        except FileExistsError:
            pass
        else:
            return fn, path

    raise Exception("Unable to create file")


def extract_logs(log_fp: io.StringIO, out_path: Path, url_prefix):
    # FIXME: memory inefficient because new buffer created every time

    ctestlog = CTestLog()

    for target, reason, ctest_buf in ctest_log_parser(log_fp):
        fn, _ = save_log(ctest_buf, out_path, target)
        log_url = f"{url_prefix}{fn}"

        shard = ctestlog.add_shard(target, reason, log_url)

        if not ctest_buf:
            continue

        first_line = ctest_buf[0]

        if first_line.startswith("[==========]"):
            for classname, method, err_lines in parse_gtest_fails(ctest_buf):
                fn, path = save_log(err_lines, out_path, classname, method)
                log_url = f"{url_prefix}{fn}"
                shard.add_testcase(classname, method, path, log_url)
        elif first_line.startswith("<-----"):
            for classname, method, err_lines in parse_yunit_fails(ctest_buf):
                fn, path = save_log(err_lines, out_path, classname, method)
                log_url = f"{url_prefix}{fn}"
                shard.add_testcase(classname, method, path, log_url)
        else:
            pass

    return ctestlog


def attach_to_ctest(ctest_log: CTestLog, ctest_path):
    tree = ET.parse(ctest_path)
    root = tree.getroot()
    changed = False
    for testcase in root.findall("testcase"):
        name = testcase.attrib["classname"]
        if ctest_log.has_error_shard(name):
            add_junit_log_property(testcase, ctest_log.get_shard(name).log_url)
            changed = True

    if changed:
        print(f"patch {ctest_path}")
        tree.write(ctest_path, xml_declaration=True, encoding="UTF-8")


def attach_to_unittests(ctest_log: CTestLog, unit_path):
    all_found_tests = {}

    for fn in glob.glob(os.path.join(unit_path, "*.xml")):
        log_name = os.path.splitext(os.path.basename(fn))[0]
        common_shard_name = fn_shard_part_re.sub("", log_name)
        found_tests = all_found_tests.setdefault(common_shard_name, [])
        tree = ET.parse(fn)
        root = tree.getroot()
        changed = False

        for tsuite, tcase, cls, name in suite_case_iterator(root):
            test_log = ctest_log.get_log(common_shard_name, cls, name)

            if test_log is None:
                continue

            found_tests.append((cls, name))
            add_junit_log_property(tcase, test_log.url)
            changed = True

        if changed:
            print(f"patch {fn}")
            tree.write(fn, xml_declaration=True, encoding="UTF-8")

    for shard, found_tests in all_found_tests.items():
        extra_logs = ctest_log.get_extra_tests(shard, found_tests)
        if not extra_logs:
            continue

        fn = f"_{shard}_not_found.xml"
        testcases = [create_error_testcase(t.shard.name, t.classname, t.method, t.fn, t.url) for t in extra_logs]

        testsuite = create_error_testsuite(testcases)
        testsuite.write(os.path.join(unit_path, fn), xml_declaration=True, encoding="UTF-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-prefix", default="./")
    parser.add_argument("--decompress", action="store_true", default=False, help="decompress ctest log")
    parser.add_argument("--filter-test-file", required=False)
    parser.add_argument("--filter-shard-file", required=False)
    parser.add_argument("--ctest-report")
    parser.add_argument("--junit-reports-path")
    parser.add_argument("ctest_log")
    parser.add_argument("out_log_dir")

    args = parser.parse_args()

    ctest_log = extract_logs(log_reader(args.ctest_log, args.decompress), Path(args.out_log_dir), args.url_prefix)

    if ctest_log.has_logs:
        attach_to_ctest(ctest_log, args.ctest_report)
        attach_to_unittests(ctest_log, args.junit_reports_path)


if __name__ == "__main__":
    main()
