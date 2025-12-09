#!/usr/bin/env python3

# Tool used to transform build-results-report. Performs the following:
# - adds links to logs in test results
# - mutes tests
# - adds user properties from test_dir

import argparse
import json
import os
import shutil
import sys
import urllib.parse
import zipfile
from typing import Set
from mute_check import YaMuteCheck


def log_print(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


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
            shutil.copyfile(fn, out_fn)
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


def load_user_properties(test_dir):
    """Load user properties from test_dir JSON files"""
    all_properties = {}

    if not test_dir or not os.path.isdir(test_dir):
        return all_properties

    for dirpath, _, filenames in os.walk(test_dir):
        for filename in filenames:
            properties_file_path = os.path.abspath(os.path.join(dirpath, filename))

            if os.path.isfile(properties_file_path):
                try:
                    with open(properties_file_path, "r") as upf:
                        properties = json.load(upf)

                    # Merge properties into all_properties
                    for key, value in properties.items():
                        if key not in all_properties:
                            all_properties[key] = value
                        else:
                            all_properties[key].update(value)
                except (json.JSONDecodeError, IOError) as e:
                    log_print(f"Warning: Unable to load properties from {properties_file_path}: {e}")

    return all_properties


def strip_rich_markup(text):
    """Remove rich formatting tags like [[bad]], [[rst]], [[good]], etc. from text.
    
    Only removes known markup tags: imp, unimp, bad, warn, good, alt1, alt2, alt3, path, rst
    to avoid breaking error messages that might contain square brackets.
    """
    if not text:
        return text
    # List of known markup tags from app.display
    known_tags = ['imp', 'unimp', 'bad', 'warn', 'good', 'alt1', 'alt2', 'alt3', 'path', 'rst']
    # Replace only known tags, preserving any other square bracket content
    for tag in known_tags:
        text = text.replace(f'[[{tag}]]', '')
    return text


def mute_test_result(result):
    """Mute a test result - set muted flag and change status to SKIPPED"""
    if result.get("status") in ("FAILED", "ERROR"):
        result["muted"] = True
        result["status"] = "SKIPPED"
        # Preserve error information in rich-snippet if it exists
        return True
    return False


def transform(report_file, mute_check: YaMuteCheck, ya_out_dir, log_url_prefix, log_out_dir, log_truncate_size,
              test_stuff_out, test_stuff_prefix, test_dir):
    with open(report_file, 'r') as f:
        report = json.load(f)

    # Load user properties
    user_properties = load_user_properties(test_dir)

    # Group results by suite (path)
    suites = {}
    for result in report.get("results", []):
        if result.get("type") != "test":
            continue
        
        suite_name = result.get("path", "")
        if suite_name not in suites:
            suites[suite_name] = []
        suites[suite_name].append(result)

    # Process each suite
    for suite_name, results in suites.items():
        traces = YTestReportTrace(ya_out_dir)
        traces.load(suite_name)

        has_fail_tests = False

        for result in results:
            path_str = result.get("path", "")
            name = result.get("name", "")
            subtest_name = result.get("subtest_name", "")
            
            test_name = path_str
            if name:
                test_name = f"{path_str}/{name}"
            if subtest_name:
                if name:
                    test_name = f"{path_str}/{name}.{subtest_name}"
                else:
                    test_name = f"{path_str}/{subtest_name}"

            # Check if test should be muted
            if mute_check(suite_name, test_name):
                log_print("mute", suite_name, test_name)
                mute_test_result(result)

            # Check if test failed
            status = result.get("status", "")
            is_fail = status in ("FAILED", "ERROR")
            has_fail_tests |= is_fail

            # Add logs for failed tests
            # First, try to get logs from ytest.report.trace
            if is_fail and subtest_name and "." in subtest_name:
                test_cls, test_method = subtest_name.rsplit(".", maxsplit=1)
                logs = filter_empty_logs(traces.get_logs(test_cls, test_method))

                if logs:
                    log_print(f"add {list(logs.keys())!r} properties for {test_cls}.{test_method}")
                    if "properties" not in result:
                        result["properties"] = {}
                    
                    for name, fn in logs.items():
                        url = save_log(ya_out_dir, fn, log_out_dir, log_url_prefix, log_truncate_size)
                        result["properties"][f"url:{name}"] = url
            
            # Also process existing links from build-results-report (they are arrays with file paths)
            if is_fail:
                if "properties" not in result:
                    result["properties"] = {}
                original_links = result.get("links", {})
                # links is an object with arrays: {"stdout": ["/path"], "stderr": ["/path"]}
                for link_type in ["stdout", "stderr", "log"]:
                    if link_type in original_links and isinstance(original_links[link_type], list):
                        for file_path in original_links[link_type]:
                            if os.path.isfile(file_path):
                                url = save_log(ya_out_dir, file_path, log_out_dir, log_url_prefix, log_truncate_size)
                                result["properties"][f"url:{link_type}"] = url
                                break

            # Add user properties
            if test_dir and path_str in user_properties:
                if subtest_name and subtest_name in user_properties[path_str]:
                    if "properties" not in result:
                        result["properties"] = {}
                    result["properties"].update(user_properties[path_str][subtest_name])

        # Add logsdir for failed tests
        if has_fail_tests:
            if not traces.logs_dir:
                log_print(f"no logsdir for {suite_name}")
                continue

            url = save_zip(suite_name, test_stuff_out, test_stuff_prefix, traces.logs_dir)

            for result in results:
                if "properties" not in result:
                    result["properties"] = {}
                result["properties"]["url:logsdir"] = url
                # Also update links if it exists (for consistency)
                if "links" in result:
                    if "logsdir" not in result["links"]:
                        result["links"]["logsdir"] = []
                    # Add URL to links array (though properties is the primary source)
                    if url not in result["links"]["logsdir"]:
                        result["links"]["logsdir"].append(url)

    # Strip rich markup from all results (not just test results) to ensure cleanup
    for result in report.get("results", []):
        if "rich-snippet" in result and result["rich-snippet"]:
            result["rich-snippet"] = strip_rich_markup(result["rich-snippet"])

    # Save updated report
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--build-results-report",
        dest="build_results_report",
        required=True,
        help="path to build-results-report JSON file"
    )
    parser.add_argument("-m", help="muted test list")
    parser.add_argument('--public_dir', help='root directory for publication')
    parser.add_argument("--public_dir_url", help="url prefix for root directory")
    parser.add_argument("--test_dir", help="directory with user properties JSON files")
    parser.add_argument("--log_out_dir", help="out dir to store logs (symlinked), relative to public_dir")
    parser.add_argument(
        "--log_truncate_size",
        type=int,
        default=134217728,  # 128 kb
        help="truncate log after specific size, 0 disables truncation",
    )
    parser.add_argument("--ya_out", help="ya make output dir (for searching logs and artifacts)")
    parser.add_argument('--test_stuff_out', help='output dir for archive testing_out_stuff, relative to public_dir"')

    args = parser.parse_args()

    mute_check = YaMuteCheck()

    if args.m:
        mute_check.load(args.m)

    log_out_dir = os.path.join(args.public_dir, args.log_out_dir)
    os.makedirs(log_out_dir, exist_ok=True)
    log_url_prefix = os.path.join(args.public_dir_url, args.log_out_dir)

    test_stuff_out = os.path.join(args.public_dir, args.test_stuff_out)
    os.makedirs(test_stuff_out, exist_ok=True)
    test_stuff_prefix = os.path.join(args.public_dir_url, args.test_stuff_out)

    transform(
        args.build_results_report,
        mute_check,
        args.ya_out,
        log_url_prefix,
        log_out_dir,
        args.log_truncate_size,
        test_stuff_out,
        test_stuff_prefix,
        args.test_dir,
    )


if __name__ == "__main__":
    main()

