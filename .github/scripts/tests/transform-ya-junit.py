#!/usr/bin/env python3

# Tool used to transform junit report. Performs the following:
# - adds classname with relative path to test in 'testcase' node
# - add 'url:logsdir' and other links with in 'testcase' node
# - mutes tests

import argparse
import re
import json
import os
import sys
import urllib.parse
import zipfile
from typing import Set
from xml.etree import ElementTree as ET
from mute_utils import mute_target, pattern_to_re
from junit_utils import add_junit_link_property, is_faulty_testcase


def log_print(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class YaMuteCheck:
    def __init__(self):
        self.regexps = set()
        self.regexps = []

    def load(self, fn):
        with open(fn, "r") as fp:
            for line in fp:
                line = line.strip()
                try:
                    testsuite, testcase = line.split(" ", maxsplit=1)
                except ValueError:
                    log_print(f"SKIP INVALID MUTE CONFIG LINE: {line!r}")
                    continue
                self.populate(testsuite, testcase)

    def populate(self, testsuite, testcase):
        check = []

        for p in (pattern_to_re(testsuite), pattern_to_re(testcase)):
            try:
                check.append(re.compile(p))
            except re.error:
                log_print(f"Unable to compile regex {p!r}")
                return

        self.regexps.append(tuple(check))

    def __call__(self, suite_name, test_name):
        for ps, pt in self.regexps:
            if ps.match(suite_name) and pt.match(test_name):
                return True
        return False


class YTestReportTrace:
    def __init__(self, out_root):
        self.out_root = out_root
        self.traces = {}
        self.logs_dir = set()

    def abs_path(self, path):
        return path.replace("$(BUILD_ROOT)", self.out_root)

    def load(self, subdir):
        test_results_dir = os.path.join(self.out_root, f"{subdir}/test-results/")

        if not os.path.isdir(test_results_dir):
            log_print(f"Directory {test_results_dir} doesn't exist")
            return

        # find the test result
        for folder in os.listdir(test_results_dir):
            fn = os.path.join(self.out_root, test_results_dir, folder, "ytest.report.trace")

            if not os.path.isfile(fn):
                continue

            with open(fn, "r") as fp:
                for line in fp:
                    event = json.loads(line.strip())
                    if event["name"] == "subtest-finished":
                        event = event["value"]
                        cls = event["class"]
                        subtest = event["subtest"]
                        cls = cls.replace("::", ".")
                        self.traces[(cls, subtest)] = event
                        logs_dir = self.abs_path(event['logs']['logsdir'])
                        self.logs_dir.add(logs_dir)

    def has(self, cls, name):
        return (cls, name) in self.traces

    def get_logs(self, cls, name):
        trace = self.traces.get((cls, name))

        if not trace:
            return {}

        logs = trace["logs"]

        result = {}
        for k, path in logs.items():
            if k == "logsdir":
                continue

            result[k] = self.abs_path(path)

        return result


def filter_empty_logs(logs):
    result = {}
    for k, v in logs.items():
        if not os.path.isfile(v) or os.stat(v).st_size == 0:
            continue
        result[k] = v
    return result


def save_log(build_root, fn, out_dir, log_url_prefix, trunc_size):
    fpath = os.path.relpath(fn, build_root)

    if out_dir is not None:
        out_fn = os.path.join(out_dir, fpath)
        fsize = os.stat(fn).st_size

        out_fn_dir = os.path.dirname(out_fn)

        if not os.path.isdir(out_fn_dir):
            os.makedirs(out_fn_dir, 0o700)

        if trunc_size and fsize > trunc_size:
            with open(fn, "rb") as in_fp:
                in_fp.seek(fsize - trunc_size)
                log_print(f"truncate {out_fn} to {trunc_size}")
                with open(out_fn, "wb") as out_fp:
                    while 1:
                        buf = in_fp.read(8192)
                        if not buf:
                            break
                        out_fp.write(buf)
        else:
            os.symlink(fn, out_fn)
    quoted_fpath = urllib.parse.quote(fpath)
    return f"{log_url_prefix}{quoted_fpath}"


def save_zip(suite_name, out_dir, url_prefix, logs_dir: Set[str]):
    arc_name = f"{suite_name.replace('/', '-')}.zip"

    arc_fn = os.path.join(out_dir, arc_name)

    zf = zipfile.ZipFile(arc_fn, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9)

    for path in logs_dir:
        # path is .../test-results/black/testing_out_stuff
        log_print(f"put {path} into {arc_name}")
        test_type = os.path.basename(os.path.dirname(path))
        for root, dirs, files in os.walk(path):
            for f in files:
                filename = os.path.join(root, f)
                zf.write(filename, os.path.join(test_type, os.path.relpath(filename, path)))
    zf.close()

    quoted_fpath = urllib.parse.quote(arc_name)
    return f"{url_prefix}{quoted_fpath}"


def transform(fp, mute_check: YaMuteCheck, ya_out_dir, save_inplace, log_url_prefix, log_out_dir, log_truncate_size,
              test_stuff_out, test_stuff_prefix):
    tree = ET.parse(fp)
    root = tree.getroot()

    for suite in root.findall("testsuite"):
        suite_name = suite.get("name")
        traces = YTestReportTrace(ya_out_dir)
        traces.load(suite_name)

        has_fail_tests = False

        for case in suite.findall("testcase"):
            test_name = case.get("name")
            case.set("classname", suite_name)

            is_fail = is_faulty_testcase(case)
            has_fail_tests |= is_fail

            if mute_check(suite_name, test_name):
                log_print("mute", suite_name, test_name)
                mute_target(case)

            if is_fail and "." in test_name:
                test_cls, test_method = test_name.rsplit(".", maxsplit=1)
                logs = filter_empty_logs(traces.get_logs(test_cls, test_method))

                if logs:
                    log_print(f"add {list(logs.keys())!r} properties for {test_cls}.{test_method}")
                    for name, fn in logs.items():
                        url = save_log(ya_out_dir, fn, log_out_dir, log_url_prefix, log_truncate_size)
                        add_junit_link_property(case, name, url)

        if has_fail_tests:
            if not traces.logs_dir:
                log_print(f"no logsdir for {suite_name}")
                continue

            url = save_zip(suite_name, test_stuff_out, test_stuff_prefix, traces.logs_dir)

            for case in suite.findall("testcase"):
                add_junit_link_property(case, 'logsdir', url)

    if save_inplace:
        tree.write(fp.name)
    else:
        ET.indent(root)
        print(ET.tostring(root, encoding="unicode"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", action="store_true", dest="save_inplace", default=False, help="modify input file in-place"
    )
    parser.add_argument("-m", help="muted test list")
    parser.add_argument('--public_dir', help='root directory for publication')
    parser.add_argument("--public_dir_url", help="url prefix for root directory")

    parser.add_argument("--log_out_dir", help="out dir to store logs (symlinked), relative to public_dir")
    parser.add_argument(
        "--log_truncate_size",
        type=int,
        default=134217728,
        help="truncate log after specific size, 0 disables truncation",
    )
    parser.add_argument("--ya_out", help="ya make output dir (for searching logs and artifacts)")
    parser.add_argument('--test_stuff_out', help='output dir for archive testing_out_stuff, relative to public_dir"')
    parser.add_argument("in_file", type=argparse.FileType("r"))

    args = parser.parse_args()

    mute_check = YaMuteCheck()

    if args.m:
        mute_check.load(args.m)

    log_out_dir =  os.path.join(args.public_dir, args.log_out_dir)
    os.makedirs(log_out_dir, exist_ok=True)
    log_url_prefix = os.path.join(args.public_dir_url, args.log_out_dir)

    test_stuff_out = os.path.join(args.public_dir, args.test_stuff_out)
    os.makedirs(test_stuff_out, exist_ok=True)
    test_stuff_prefix = os.path.join(args.public_dir_url, args.test_stuff_out)

    transform(
        args.in_file,
        mute_check,
        args.ya_out,
        args.save_inplace,
        log_url_prefix,
        log_out_dir,
        args.log_truncate_size,
        test_stuff_out,
        test_stuff_prefix,
    )


if __name__ == "__main__":
    main()
