#!/usr/bin/env python3
import argparse
import io
import os
import glob
from xml.etree import ElementTree as ET
from pathlib import Path
from typing import List
from log_parser import ctest_log_parser, parse_yunit_fails, parse_gtest_fails, log_reader
from mute_utils import MutedTestCheck, MutedShardCheck


def make_filename(*parts):
    return f'{"-".join(parts)}.log'


def save_log(err_lines: List[str], out_path: Path, *parts):
    fn = make_filename(*parts)
    print(f"write {fn} for {'::'.join(parts)}")
    with open(out_path.joinpath(fn), "wt") as fp:
        for line in err_lines:
            fp.write(f"{line}\n")

    return fn


def extract_logs(log_fp: io.StringIO, out_path: Path, url_prefix):
    # FIXME: memory inefficient because new buffer created every time

    log_urls = []
    for target, reason, ctest_buf in ctest_log_parser(log_fp):
        suite_summary = []

        fn = save_log(ctest_buf, out_path, target)
        log_url = f"{url_prefix}{fn}"

        log_urls.append((target, reason, log_url, suite_summary))

        if not ctest_buf:
            continue

        first_line = ctest_buf[0]
        if first_line.startswith("[==========]"):
            for classname, method, err in parse_gtest_fails(ctest_buf):
                fn = save_log(err, out_path, classname, method)
                log_url = f"{url_prefix}{fn}"
                suite_summary.append((classname, method, log_url))
        elif first_line.startswith("<-----"):
            for classname, method, err in parse_yunit_fails(ctest_buf):
                fn = save_log(err, out_path, classname, method)
                log_url = f"{url_prefix}{fn}"
                suite_summary.append((classname, method, log_url))
        else:
            pass

    return log_urls


def generate_summary(summary, is_mute_shard, is_mute_test):
    icon = ":floppy_disk:"
    mute_icon = ":white_check_mark:"
    text = [
        "| Test  | Status | Muted | Log |",
        "| ----: | :----: | :---: | --: |",
    ]

    for target, reason, target_log_url, cases in summary:
        mute_target = mute_icon if is_mute_shard(target) else ""
        display_reason = reason if reason != "Failed" else ""
        text.append(f"| **{target}** | {display_reason} | {mute_target} | [{icon}]({target_log_url}) |")
        for classname, method, log_url in cases:
            mute_class = mute_icon if is_mute_test(classname, method) else ""
            text.append(f"| _{ classname }::{ method }_ | Failed | {mute_class} | [{icon}]({log_url}) |")
    return text


def write_summary(summary, is_mute_shard, is_mute_test):
    fail_count = sum([len(s[3]) for s in summary])
    text = generate_summary(summary, is_mute_shard, is_mute_test)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "at") as fp:
        fp.write(f"Failed tests log files ({fail_count}):\n")
        for line in text:
            fp.write(f"{line}\n")


def patch_jsuite(log_urls, ctest_path, unit_paths):
    def add_link_property(tc, url):
        props = tc.find("properties")
        if props is None:
            props = ET.Element("properties")
            tc.append(props)
        props.append(ET.Element("property", dict(name="url:Log", value=url)))

    suite_logs = {}
    test_logs = {}

    for shard_name, _, log_url, cases in log_urls:
        suite_logs[shard_name] = log_url
        for classname, method, test_log_url in cases:
            test_logs[(classname, method)] = test_log_url

    if ctest_path:
        tree = ET.parse(ctest_path)
        root = tree.getroot()
        changed = False
        for testcase in root.findall("testcase"):
            log_url = suite_logs.get(testcase.attrib["classname"])
            if log_url:
                add_link_property(testcase, log_url)
                changed = True

        if changed:
            print(f"patch {ctest_path}")
            tree.write(ctest_path, xml_declaration=True, encoding="UTF-8")

    for path in unit_paths:
        for fn in glob.glob(os.path.join(path, "*.xml")):
            tree = ET.parse(fn)
            root = tree.getroot()
            changed = False
            for testsuite in root.findall("testsuite"):
                for testcase in testsuite.findall("testcase"):
                    cls, method = testcase.attrib["classname"], testcase.attrib["name"]
                    log_url = test_logs.get((cls, method))
                    if log_url:
                        add_link_property(testcase, log_url)
                        changed = True
            if changed:
                print(f"patch {fn}")
                tree.write(fn, xml_declaration=True, encoding="UTF-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-prefix", default="./")
    parser.add_argument("--decompress", action="store_true", default=False, help="decompress ctest log")
    parser.add_argument("--write-summary", action="store_true", default=False, help="update github summary")
    parser.add_argument("--filter-test-file", required=False)
    parser.add_argument("--filter-shard-file", required=False)
    parser.add_argument("--patch-jsuite", default=False, action="store_true")
    parser.add_argument("--ctest-report")
    parser.add_argument("--junit-reports-path", nargs="*")
    parser.add_argument("ctest_log")
    parser.add_argument("out_log_dir")

    args = parser.parse_args()

    log_urls = extract_logs(log_reader(args.ctest_log, args.decompress), Path(args.out_log_dir), args.url_prefix)

    if args.patch_jsuite and log_urls:
        patch_jsuite(log_urls, args.ctest_report, args.junit_reports_path)

    is_mute_shard = MutedShardCheck(args.filter_shard_file)
    is_mute_test = MutedTestCheck(args.filter_test_file)

    if args.write_summary:
        if log_urls:
            write_summary(log_urls, is_mute_shard, is_mute_test)
    else:
        print("\n".join(generate_summary(log_urls, is_mute_shard, is_mute_test)))


if __name__ == "__main__":
    main()
