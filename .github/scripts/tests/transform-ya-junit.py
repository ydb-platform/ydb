#!/usr/bin/env python3
import argparse
import re
import json
import os
import sys
from xml.etree import ElementTree as ET
from mute_utils import mute_target, pattern_to_re
from junit_utils import add_junit_link_property, is_faulty_testcase


def log_print(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class YaMuteCheck:
    def __init__(self):
        self.regexps = set()

    def add_unittest(self, fn):
        with open(fn, "r") as fp:
            for line in fp:
                line = line.strip()
                path, rest = line.split("/")
                path = path.replace("-", "/")
                rest = rest.replace("::", ".")
                self.populate(f"{path}/{rest}")

    def add_functest(self, fn):
        with open(fn, "r") as fp:
            for line in fp:
                line = line.strip()
                line = line.replace("::", ".")
                self.populate(line)

    def populate(self, line):
        pattern = pattern_to_re(line)

        try:
            self.regexps.add(re.compile(pattern))
        except re.error:
            log_print(f"Unable to compile regex {pattern!r}")

    def __call__(self, suitename, testname):
        for r in self.regexps:
            if r.match(f"{suitename}/{testname}"):
                return True
        return False


class YTestReportTrace:
    def __init__(self, out_root):
        self.out_root = out_root
        self.traces = {}

    def load(self, subdir):
        test_results_dir = os.path.join(self.out_root, f"{subdir}/test-results/")

        if not os.path.isdir(test_results_dir):
            log_print(f"Directory {test_results_dir} doesn't exist")
            return

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

            result[k] = path.replace("$(BUILD_ROOT)", self.out_root)

        return result


def filter_empty_logs(logs):
    result = {}
    for k, v in logs.items():
        if os.stat(v).st_size == 0:
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

    return f"{log_url_prefix}{fpath}"


def transform(fp, mute_check: YaMuteCheck, ya_out_dir, save_inplace, log_url_prefix, log_out_dir, log_trunc_size):
    tree = ET.parse(fp)
    root = tree.getroot()

    for suite in root.findall("testsuite"):
        suite_name = suite.get("name")
        traces = YTestReportTrace(ya_out_dir)
        traces.load(suite_name)

        for case in suite.findall("testcase"):
            test_name = case.get("name")
            case.set("classname", suite_name)

            is_fail = is_faulty_testcase(case)

            if mute_check(suite_name, test_name):
                log_print("mute", suite_name, test_name)
                mute_target(case)

            if is_fail and "." in test_name:
                test_cls, test_method = test_name.rsplit(".", maxsplit=1)
                logs = filter_empty_logs(traces.get_logs(test_cls, test_method))

                if logs:
                    log_print(f"add {list(logs.keys())!r} properties for {test_cls}.{test_method}")
                    for name, fn in logs.items():
                        url = save_log(ya_out_dir, fn, log_out_dir, log_url_prefix, log_trunc_size)
                        add_junit_link_property(case, name, url)

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
    parser.add_argument("--mu", help="unittest mute config")
    parser.add_argument("--mf", help="functional test mute config")
    parser.add_argument("--log-url-prefix", default="./", help="url prefix for logs")
    parser.add_argument("--log-out-dir", help="symlink logs to specific directory")
    parser.add_argument(
        "--log-truncate-size",
        dest="log_trunc_size",
        type=int,
        default=134217728,
        help="truncate log after specific size, 0 disables truncation",
    )
    parser.add_argument("--ya-out", help="ya make output dir (for searching logs and artifacts)")
    parser.add_argument("in_file", type=argparse.FileType("r"))

    args = parser.parse_args()

    mute_check = YaMuteCheck()

    if args.mu:
        mute_check.add_unittest(args.mu)

    if args.mf:
        mute_check.add_functest(args.mf)

    transform(
        args.in_file,
        mute_check,
        args.ya_out,
        args.save_inplace,
        args.log_url_prefix,
        args.log_out_dir,
        args.log_trunc_size,
    )


if __name__ == "__main__":
    main()
