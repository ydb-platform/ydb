#!/usr/bin/env python3
import argparse
import dataclasses
import os, sys
from enum import Enum
from itertools import groupby
from operator import attrgetter
from typing import List
from jinja2 import Environment, FileSystemLoader
from junit_utils import get_property_value, iter_xml_files


class TestStatus(Enum):
    PASS = 0
    FAIL = 1
    ERROR = 2
    SKIP = 3
    MUTE = 4

    def __lt__(self, other):
        return self.value < other.value


@dataclasses.dataclass
class TestResult:
    classname: str
    name: str
    status: TestStatus

    @property
    def status_display(self):
        return {
            TestStatus.PASS: "PASS",
            TestStatus.FAIL: "FAIL",
            TestStatus.ERROR: "ERROR",
            TestStatus.SKIP: "SKIP",
            TestStatus.MUTE: "MUTE",
        }[self.status]

    def __str__(self):
        return f"{self.full_name:<138} {self.status_display}"

    @property
    def full_name(self):
        return f"{self.classname}/{self.name}"


def render_pm(value, url, diff=None):
    if value:
        text = f"[{value}]({url})"
    else:
        text = str(value)

    if diff is not None and diff != 0:
        if diff == 0:
            sign = "±"
        elif diff < 0:
            sign = "-"
        else:
            sign = "+"

        text = f"{text} {sign}{abs(diff)}"

    return text


def render_testlist_html(rows, fn):
    TEMPLATES_PATH = os.path.join(os.path.dirname(__file__), "templates")

    env = Environment(loader=FileSystemLoader(TEMPLATES_PATH))

    rows.sort(key=attrgetter('full_name'))
    rows.sort(key=attrgetter('status'), reverse=True)

    rows = groupby(rows, key=attrgetter('status'))
    content = env.get_template("summary.html").render(test_results=rows)

    with open(fn, 'w') as fp:
        fp.write(content)


def write_summary(lines: List[str]):
    summary_fn = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_fn:
        fp = open(summary_fn, "at")
    else:
        fp = sys.stdout

    for line in lines:
        fp.write(f"{line}\n")
    fp.write("\n")

    if summary_fn:
        fp.close()


def gen_summary(summary_url_prefix, summary_out_folder, paths, ):
    summary = [
        "|      | TESTS | PASSED | ERRORS | FAILED | SKIPPED | MUTED[^1] |",
        "| :--- | ---:  | -----: | -----: | -----: | ------: | ----: |",
    ]
    for title, html_fn, path in paths:
        tests = failed = errors = muted = skipped = passed = 0

        test_results = []

        for fn, suite, case in iter_xml_files(path):
            tests += 1
            classname, name = case.get("classname"), case.get("name")
            if case.find("failure") is not None:
                failed += 1
                status = TestStatus.FAIL
            elif case.find("error") is not None:
                errors += 1
                status = TestStatus.ERROR
            elif get_property_value(case, "mute") is not None:
                muted += 1
                status = TestStatus.MUTE
            elif case.find("skipped") is not None:
                skipped += 1
                status = TestStatus.SKIP
            else:
                passed += 1
                status = TestStatus.PASS

            test_result = TestResult(classname=classname, name=name, status=status)
            test_results.append(test_result)

        report_url = f'{summary_url_prefix}{html_fn}'

        render_testlist_html(test_results, os.path.join(summary_out_folder, html_fn))

        summary.append(
            " | ".join(
                [
                    title,
                    render_pm(tests, f'{report_url}', 0),
                    render_pm(passed, f'{report_url}#PASS', 0),
                    render_pm(errors, f'{report_url}#ERROR', 0),
                    render_pm(failed, f'{report_url}#FAIL', 0),
                    render_pm(skipped, f'{report_url}#SKIP', 0),
                    render_pm(muted, f'{report_url}#MUTE', 0),
                ]
            )
        )

    github_srv = os.environ.get('GITHUB_SERVER_URL', 'https://github.com')
    repo = os.environ.get('GITHUB_REPOSITORY', 'ydb-platform/ydb')

    summary.append("\n")
    summary.append(f"[^1]: All mute rules are defined [here]({github_srv}/{repo}/tree/main/.github/config).")

    write_summary(lines=summary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary-out-path", required=True)
    parser.add_argument("--summary-url-prefix", required=True)
    parser.add_argument("args", nargs="+", metavar="TITLE html_out path")
    args = parser.parse_args()

    if len(args.args) % 3 != 0:
        print("Invalid argument count")
        raise SystemExit(-1)

    paths = iter(args.args)
    title_path = list(zip(paths, paths, paths))

    gen_summary(args.summary_url_prefix, args.summary_out_path, title_path)


if __name__ == "__main__":
    main()
