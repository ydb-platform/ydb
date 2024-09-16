#!/usr/bin/env python3
import argparse
import dataclasses
import os
import sys
import traceback
from codeowners import CodeOwners
from enum import Enum
from operator import attrgetter
from typing import List, Dict
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from junit_utils import get_property_value, iter_xml_files
from get_test_history import get_test_history


class TestStatus(Enum):
    PASS = 0
    FAIL = 1
    ERROR = 2
    SKIP = 3
    MUTE = 4

    @property
    def is_error(self):
        return self in (TestStatus.FAIL, TestStatus.ERROR, TestStatus.MUTE)

    def __lt__(self, other):
        return self.value < other.value


@dataclasses.dataclass
class TestResult:
    classname: str
    name: str
    status: TestStatus
    log_urls: Dict[str, str]
    elapsed: float
    count_of_passed: int
    owners: str

    @property
    def status_display(self):
        return {
            TestStatus.PASS: "PASS",
            TestStatus.FAIL: "FAIL",
            TestStatus.ERROR: "ERROR",
            TestStatus.SKIP: "SKIP",
            TestStatus.MUTE: "MUTE",
        }[self.status]

    @property
    def elapsed_display(self):
        m, s = divmod(self.elapsed, 60)
        parts = []
        if m > 0:
            parts.append(f'{int(m)}m')
        parts.append(f"{s:.3f}s")
        return ' '.join(parts)

    def __str__(self):
        return f"{self.full_name:<138} {self.status_display}"

    @property
    def full_name(self):
        return f"{self.classname}/{self.name}"

    @classmethod
    def from_junit(cls, testcase):
        classname, name = testcase.get("classname"), testcase.get("name")

        if testcase.find("failure") is not None:
            status = TestStatus.FAIL
        elif testcase.find("error") is not None:
            status = TestStatus.ERROR
        elif get_property_value(testcase, "mute") is not None:
            status = TestStatus.MUTE
        elif testcase.find("skipped") is not None:
            status = TestStatus.SKIP
        else:
            status = TestStatus.PASS

        log_urls = {
            'Log': get_property_value(testcase, "url:Log"),
            'log': get_property_value(testcase, "url:log"),
            'logsdir': get_property_value(testcase, "url:logsdir"),
            'stdout': get_property_value(testcase, "url:stdout"),
            'stderr': get_property_value(testcase, "url:stderr"),
        }
        log_urls = {k: v for k, v in log_urls.items() if v}

        elapsed = testcase.get("time")

        try:
            elapsed = float(elapsed)
        except (TypeError, ValueError):
            elapsed = 0
            print(f"Unable to cast elapsed time for {classname}::{name}  value={elapsed!r}")

        return cls(classname, name, status, log_urls, elapsed, 0,'')


class TestSummaryLine:
    def __init__(self, title):
        self.title = title
        self.tests = []
        self.is_failed = False
        self.report_fn = self.report_url = None
        self.counter = {s: 0 for s in TestStatus}

    def add(self, test: TestResult):
        self.is_failed |= test.status in (TestStatus.ERROR, TestStatus.FAIL)
        self.counter[test.status] += 1
        self.tests.append(test)

    def add_report(self, fn, url):
        self.report_fn = fn
        self.report_url = url

    @property
    def test_count(self):
        return len(self.tests)

    @property
    def passed(self):
        return self.counter[TestStatus.PASS]

    @property
    def errors(self):
        return self.counter[TestStatus.ERROR]

    @property
    def failed(self):
        return self.counter[TestStatus.FAIL]

    @property
    def skipped(self):
        return self.counter[TestStatus.SKIP]

    @property
    def muted(self):
        return self.counter[TestStatus.MUTE]


class TestSummary:
    def __init__(self, is_retry: bool):
        self.lines: List[TestSummaryLine] = []
        self.is_failed = False
        self.is_retry = is_retry

    def add_line(self, line: TestSummaryLine):
        self.is_failed |= line.is_failed
        self.lines.append(line)

    def render_line(self, items):
        return f"| {' | '.join(items)} |"

    def render(self, add_footnote=False, is_retry=False):
        github_srv = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
        repo = os.environ.get("GITHUB_REPOSITORY", "ydb-platform/ydb")

        footnote_url = f"{github_srv}/{repo}/tree/main/.github/config/muted_ya.txt"

        footnote = "[^1]" if add_footnote else f'<sup>[?]({footnote_url} "All mute rules are defined here")</sup>'

        columns = [
            "TESTS", "PASSED", "ERRORS", "FAILED", "SKIPPED", f"MUTED{footnote}"
        ]

        need_first_column = len(self.lines) > 1

        if need_first_column:
            columns.insert(0, "")

        result = []

        result.append(self.render_line(columns))

        if need_first_column:
            result.append(self.render_line([':---'] + ['---:'] * (len(columns) - 1)))
        else:
            result.append(self.render_line(['---:'] * len(columns)))

        for line in self.lines:
            report_url = line.report_url
            row = []
            if need_first_column:
                row.append(line.title)
            row.extend([
                render_pm(f"{line.test_count}" + (" (only retried tests)" if self.is_retry else ""), f"{report_url}", 0),
                render_pm(line.passed, f"{report_url}#PASS", 0),
                render_pm(line.errors, f"{report_url}#ERROR", 0),
                render_pm(line.failed, f"{report_url}#FAIL", 0),
                render_pm(line.skipped, f"{report_url}#SKIP", 0),
                render_pm(line.muted, f"{report_url}#MUTE", 0),
            ])
            result.append(self.render_line(row))

        if add_footnote:
            result.append("")
            result.append(f"[^1]: All mute rules are defined [here]({footnote_url}).")
        
        return result


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


def render_testlist_html(rows, fn, build_preset):
    TEMPLATES_PATH = os.path.join(os.path.dirname(__file__), "templates")

    env = Environment(loader=FileSystemLoader(TEMPLATES_PATH), undefined=StrictUndefined)

    status_test = {}
    last_n_runs = 5
    has_any_log = set()

    for t in rows:
        status_test.setdefault(t.status, []).append(t)
        if any(t.log_urls.values()):
            has_any_log.add(t.status)

    for status in status_test.keys():
        status_test[status].sort(key=attrgetter("full_name"))

    status_order = [TestStatus.ERROR, TestStatus.FAIL, TestStatus.SKIP, TestStatus.MUTE, TestStatus.PASS]

    # remove status group without tests
    status_order = [s for s in status_order if s in status_test]

    # get testowners
    all_tests = [test for status in status_order for test in status_test.get(status)]
        
    dir = os.path.dirname(__file__)
    git_root = f"{dir}/../../.."
    codeowners = f"{git_root}/.github/TESTOWNERS"
    get_codeowners_for_tests(codeowners, all_tests)
    
    # statuses for history
    status_for_history = [TestStatus.FAIL, TestStatus.MUTE]
    status_for_history = [s for s in status_for_history if s in status_test]
    
    tests_names_for_history = []
    history= {}
    tests_in_statuses = [test for status in status_for_history for test in status_test.get(status)]
    
    # get tests for history
    for test in tests_in_statuses:
        tests_names_for_history.append(test.full_name)

    try:
        history = get_test_history(tests_names_for_history, last_n_runs, build_preset)
    except Exception:
        print(traceback.format_exc())
   
    #geting count of passed tests in history for sorting
    for test in tests_in_statuses:
        if test.full_name in history:
            test.count_of_passed = len(
                [
                    history[test.full_name][x]
                    for x in history[test.full_name]
                    if history[test.full_name][x]["status"] == "passed"
                ]
            )
    # sorting, 
    # at first - show tests with passed resuts in history
    # at second - sorted by test name
    for current_status in status_for_history:
        status_test.get(current_status,[]).sort(key=lambda val: (-val.count_of_passed, val.full_name))

    content = env.get_template("summary.html").render(
        status_order=status_order,
        tests=status_test,
        has_any_log=has_any_log,
        history=history,
    )

    with open(fn, "w") as fp:
        fp.write(content)


def write_summary(summary: TestSummary):
    summary_fn = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_fn:
        fp = open(summary_fn, "at")
    else:
        fp = sys.stdout

    for line in summary.render(add_footnote=True):
        fp.write(f"{line}\n")

    fp.write("\n")

    if summary_fn:
        fp.close()


def get_codeowners_for_tests(codeowners_file_path, tests_data):
    with open(codeowners_file_path, 'r') as file:
        data = file.read()
        owners_odj = CodeOwners(data)

        tests_data_with_owners = []
        for test in tests_data:
            target_path = test.classname
            owners = owners_odj.of(target_path)
            test.owners = joined_owners = ";;".join(
                [(":".join(x)) for x in owners])
            tests_data_with_owners.append(test)


def gen_summary(public_dir, public_dir_url, paths, is_retry: bool, build_preset):
    summary = TestSummary(is_retry=is_retry)

    for title, html_fn, path in paths:
        summary_line = TestSummaryLine(title)

        for fn, suite, case in iter_xml_files(path):
            test_result = TestResult.from_junit(case)
            summary_line.add(test_result)
        
        if os.path.isabs(html_fn):
            html_fn = os.path.relpath(html_fn, public_dir)
        report_url = f"{public_dir_url}/{html_fn}"

        render_testlist_html(summary_line.tests, os.path.join(public_dir, html_fn),build_preset)
        summary_line.add_report(html_fn, report_url)
        summary.add_line(summary_line)

    return summary


def get_comment_text(summary: TestSummary, summary_links: str, is_last_retry: bool)->tuple[str, list[str]]:
    color = "red"
    if summary.is_failed:
        color = "red" if is_last_retry else "yellow"
        result = f"Some tests failed, follow the links below."
        if not is_last_retry:
            result += " Going to retry failed tests..."
    else:
        color = "green"
        result = f"Tests successful."

    body = []

    body.append(result)

    if not is_last_retry:
        body.append("")
        body.append("<details>")
        body.append("")

    with open(summary_links) as f:
        links = f.readlines()
    
    links.sort()
    links = [line.split(" ", 1)[1].strip() for line in links]

    if links:
        body.append("")
        body.append(" | ".join(links))
    
    body.extend(summary.render())

    if not is_last_retry:
        body.append("")
        body.append("</details>")
        body.append("")
    else:
        body.append("")

    return color, body


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--public_dir", required=True)
    parser.add_argument("--public_dir_url", required=True)
    parser.add_argument("--summary_links", required=True)
    parser.add_argument('--build_preset', default="default-linux-x86-64-relwithdebinfo", required=False)
    parser.add_argument('--status_report_file', required=False)
    parser.add_argument('--is_retry', required=True, type=int)
    parser.add_argument('--is_last_retry', required=True, type=int)
    parser.add_argument('--comment_color_file', required=True)
    parser.add_argument('--comment_text_file', required=True)
    parser.add_argument("args", nargs="+", metavar="TITLE html_out path")
    args = parser.parse_args()

    if len(args.args) % 3 != 0:
        print("Invalid argument count")
        raise SystemExit(-1)

    paths = iter(args.args)
    title_path = list(zip(paths, paths, paths))

    summary = gen_summary(args.public_dir, args.public_dir_url, title_path, is_retry=bool(args.is_retry),build_preset=args.build_preset)
    write_summary(summary)

    if summary.is_failed:
        overall_status = "failure"
    else:
        overall_status = "success"

    color, text = get_comment_text(summary, args.summary_links, is_last_retry=bool(args.is_last_retry))

    with open(args.comment_color_file, "w") as f:
        f.write(color)

    with open(args.comment_text_file, "w") as f:
        f.write('\n'.join(text))
        f.write('\n')

    with open(args.status_report_file, "w") as f:
        f.write(overall_status)


if __name__ == "__main__":
    main()
