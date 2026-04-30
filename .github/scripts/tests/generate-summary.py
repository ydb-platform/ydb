#!/usr/bin/env python3
import argparse
import dataclasses
import json
import math
import os
import sys
import traceback
from enum import Enum
from operator import attrgetter
from typing import List, Dict
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from get_test_history import get_test_history
from error_type_utils import (
    is_sanitizer_issue,
    is_timeout_issue,
    is_not_launched_issue,
    is_verify_classification,
    normalize_fetch_url,
    prefetch_texts_by_urls,
)

_ANALYTICS_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'analytics'))
if _ANALYTICS_DIR not in sys.path:
    sys.path.insert(0, _ANALYTICS_DIR)
from testowners_utils import get_testowners_for_tests  # noqa: E402


def load_owner_area_mapping():
    """Load owner to area label mapping from JSON config file."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_dir = os.path.join(script_dir, '..', '..', 'config')
        mapping_file = os.path.join(config_dir, 'owner_area_mapping.json')
        with open(mapping_file, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Warning: Could not load owner area mapping: {e}")
        return {}

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
    status_description: str
    stderr_url: str = ""
    error_type: str = ""
    is_sanitizer_issue: bool = False
    is_timeout_issue: bool = False
    is_verify_issue: bool = False
    is_not_launched: bool = False

    @property
    def status_display(self):
        base = {
            TestStatus.PASS: "PASS",
            TestStatus.FAIL: "FAIL",
            TestStatus.ERROR: "ERROR",
            TestStatus.SKIP: "SKIP",
            TestStatus.MUTE: "MUTE",
        }[self.status]
        return base

    @property
    def elapsed_display(self):
        # Round up to 1 decimal place: 10.545s -> 10.6s (ceiling to 0.1)
        elapsed_rounded = math.ceil(self.elapsed * 10) / 10
        m, s = divmod(elapsed_rounded, 60)
        parts = []
        if m > 0:
            parts.append(f'{int(m)}m')
        parts.append(f"{s:.1f}s")
        return ' '.join(parts)

    def __str__(self):
        return f"{self.full_name:<138} {self.status_display}"

    @property
    def full_name(self):
        if self.classname:
            return f"{self.classname}/{self.name}"
        return self.name

    @classmethod
    def from_build_results_report(cls, result):
        """
        Create TestResult from build-results-report JSON result.
        
        Required fields: path, status
        Optional fields: name, subtest_name, error_type, rich-snippet, properties, links, metrics
        """
        # Validate required fields
        path_str = result.get("path")
        if path_str is None:
            raise ValueError(f"Missing required field 'path' in result: {result}")
        
        status_str = result.get("status")
        if status_str is None:
            raise ValueError(f"Missing required field 'status' in result: {result}")
        
        # Extract fields
        name_part = result.get("name")
        subtest_name = result.get("subtest_name")
        error_type = result.get("error_type")
        status_description = result.get("rich-snippet")
        properties = result.get("properties")
        metrics = result.get("metrics")
        
        classname = path_str
        if subtest_name and subtest_name.strip():
            if name_part:
                name = f"{name_part}.{subtest_name}"
            else:
                name = subtest_name
        else:
            name = name_part or ""
        
        # Status normalization (OK->PASSED, NOT_LAUNCHED->SKIPPED, mute->MUTE) is done by transform_build_results.py
        # Map status to TestStatus enum
        if status_str == "MUTE":
            status = TestStatus.MUTE
        elif status_str == "FAILED":
            status = TestStatus.FAIL
        elif status_str == "ERROR":
            status = TestStatus.ERROR
        elif status_str == "SKIPPED":
            status = TestStatus.SKIP
        else:
            status = TestStatus.PASS
        
        # Extract log URLs from links (updated by transform_build_results.py with URLs)
        # Links format: {"log": ["https://..."], "stdout": ["https://..."], "logsdir": ["https://..."]}
        links = result.get("links", {})
        
        def get_link_url(link_type):
            if link_type in links and isinstance(links[link_type], list) and len(links[link_type]) > 0:
                return links[link_type][0]  # Take first URL from array
            return None
        
        log_urls = {
            'Log': get_link_url("Log"),
            'log': get_link_url("log"),
            'logsdir': get_link_url("logsdir"),
            'stdout': get_link_url("stdout"),
            'stderr': get_link_url("stderr"),
        }
        log_urls = {k: v for k, v in log_urls.items() if v}
        
        # Get duration from result (same as upload_tests_results.py)
        duration = result.get("duration", 0)
        try:
            elapsed = float(duration)
        except (TypeError, ValueError):
            elapsed = 0.0
        
        return cls(
            classname=classname,
            name=name,
            status=status,
            log_urls=log_urls,
            elapsed=elapsed,
            count_of_passed=0,
            owners='',
            status_description=status_description or '',
            stderr_url=log_urls.get('stderr', ''),
            error_type=error_type or '',
            is_sanitizer_issue=is_sanitizer_issue(status_description or ''),
            is_timeout_issue=is_timeout_issue(error_type),
            is_verify_issue=False,
            # NOT_LAUNCHED can be in SKIPPED or MUTE status (if muted after being NOT_LAUNCHED)
            is_not_launched=is_not_launched_issue(error_type, status.name)
        )


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


def render_testlist_html(rows, fn, build_preset, branch, pr_number=None, workflow_run_id=None):
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
        
    get_testowners_for_tests(all_tests)
    
    # statuses for history
    status_for_history = [TestStatus.FAIL, TestStatus.MUTE, TestStatus.ERROR]
    status_for_history = [s for s in status_for_history if s in status_test]
    
    tests_names_for_history = []
    history= {}
    tests_in_statuses = [test for status in status_for_history for test in status_test.get(status)]
    
    # get tests for history
    for test in tests_in_statuses:
        tests_names_for_history.append(test.full_name)

    try:
        # Get history for last 4 days instead of last N runs
        history = get_test_history(tests_names_for_history, 4, build_preset, branch)
    except Exception:
        print(traceback.format_exc())
   
    # Calculate success rate for each test
    def calculate_success_rate(test_history):
        """Calculate success rate separately for pr-check and other runs"""
        pr_check_runs = []
        other_runs = []
        
        for timestamp, run_data in test_history.items():
            job_name = run_data.get("job_name", "").lower()
            # Check if it's a pr-check run (common patterns: pr-check, pr_check, pr/check, etc.)
            is_pr_check = "pr-check" in job_name or "pr_check" in job_name or "pr/check" in job_name
            
            if is_pr_check:
                pr_check_runs.append(run_data)
            else:
                other_runs.append(run_data)
        
        def calc_rate(runs):
            if not runs:
                return None
            passed = sum(1 for r in runs if r.get("status") == "passed")
            total = len(runs)
            return {
                "rate": round(passed / total * 100, 1) if total > 0 else 0,
                "passed": passed,
                "total": total
            }
        
        return {
            "pr_check": calc_rate(pr_check_runs),
            "other": calc_rate(other_runs)
        }
    
    # Calculate success rates for all tests with history
    test_success_rates = {}
    
    print(f"Processing history for {len(history)} tests with history data")
    print(f"Total tests in statuses: {len(tests_in_statuses)}")
    
    for test in tests_in_statuses:
        if test.full_name in history:
            test_history = history[test.full_name]
            if test_history:  # Check that history is not empty
                rates = calculate_success_rate(test_history)
                test_success_rates[test.full_name] = rates
                # Also update count_of_passed for backward compatibility
                test.count_of_passed = len(
                    [
                        history[test.full_name][x]
                        for x in history[test.full_name]
                        if history[test.full_name][x]["status"] == "passed"
                    ]
                )
    
    print(f"Calculated success rates for {len(test_success_rates)} tests")
    
    # sorted by test name
    for current_status in status_for_history:
        status_test.get(current_status,[]).sort(key=lambda val: (val.full_name, ))

    build_preset_params = '--build unknown_build_type'
    if build_preset == 'release-asan' :
        build_preset_params = '--build "release" --sanitize="address" -DDEBUGINFO_LINES_ONLY'
    elif build_preset == 'release-msan':
        build_preset_params = '--build "release" --sanitize="memory" -DDEBUGINFO_LINES_ONLY'
    elif build_preset == 'release-tsan':   
        build_preset_params = '--build "release" --sanitize="thread" -DDEBUGINFO_LINES_ONLY'
    elif build_preset == 'relwithdebinfo':
        build_preset_params = '--build "relwithdebinfo"'
    
    # Get GitHub server URL and repository from environment
    github_server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    github_repository = os.environ.get("GITHUB_REPOSITORY", "ydb-platform/ydb")
    
    # For commit SHA, prioritize the actual head commit over merge commit
    # In PR context, GITHUB_HEAD_SHA contains the actual commit being tested
    github_sha = os.environ.get("GITHUB_HEAD_SHA", "")
    
    # Construct PR and workflow URLs if the information is available
    pr_url = None
    workflow_url = None
    
    if pr_number:
        pr_url = f"{github_server_url}/{github_repository}/pull/{pr_number}"
    
    if workflow_run_id:
        workflow_url = f"{github_server_url}/{github_repository}/actions/runs/{workflow_run_id}"
    
    # Load owner to area mapping
    owner_area_mapping = load_owner_area_mapping()
    
    # Prepare history data for JavaScript (without status_description to reduce HTML size)
    # Store status_description separately - only for failed/error/mute entries to save space
    history_for_js = {}
    history_descriptions = {}  # Separate object for status descriptions (only non-empty, failed/error/mute)
    if history:
        for test_name, test_history in history.items():
            history_for_js[test_name] = {}
            history_descriptions[test_name] = {}
            for timestamp, hist_data in test_history.items():
                timestamp_str = str(timestamp)
                status = hist_data.get("status", "")
                history_for_js[test_name][timestamp_str] = {
                    "status": status,
                    "date": hist_data.get("datetime", ""),
                    "commit": hist_data.get("commit", ""),
                    "job_name": hist_data.get("job_name", ""),
                    "job_id": hist_data.get("job_id", ""),
                    "branch": hist_data.get("branch", "")
                    # status_description removed to reduce HTML size - stored separately
                }
                # Store description separately (only for failed/error/mute and if not empty to save space)
                desc = hist_data.get("status_description", "")
                if desc and desc.strip() and status in ("failure", "error", "mute"):
                    history_descriptions[test_name][timestamp_str] = desc
    
    # Prepare test descriptions for current tests (without embedding in HTML to reduce size)
    # Store only for tests with errors (FAIL, ERROR, MUTE, SKIP) and if not empty
    test_descriptions = {}
    for test in all_tests:
        if test.status in (TestStatus.FAIL, TestStatus.ERROR, TestStatus.MUTE, TestStatus.SKIP):
            desc = test.status_description
            if desc and desc.strip():
                test_descriptions[test.full_name] = desc
    
    # Save data to separate JSON file to reduce HTML size
    # fn is the full path to HTML file (e.g., /path/to/public_dir/try_1/ya-test.html)
    data_file = fn.replace('.html', '_data.json')
    data_to_save = {
        'history_for_js': history_for_js,
        'history_descriptions': history_descriptions,
        'test_descriptions': test_descriptions,
        'test_success_rates': test_success_rates
    }
    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(data_to_save, f, separators=(',', ':'))  # Minified JSON
    
    # Calculate data file URL (relative to HTML file location)
    # JSON file is in the same directory as HTML file, so we use just the filename
    # This way fetch() in browser will load JSON from the same directory as HTML
    # Example: if HTML is at /path/to/try_1/ya-test.html,
    #          JSON is at /path/to/try_1/ya-test_data.json,
    #          and URL should be "ya-test_data.json" (relative to HTML location)
    data_file_url = os.path.basename(data_file)
        
    content = env.get_template("summary.html").render(
        status_order=status_order,
        tests=status_test,
        has_any_log=has_any_log,
        history=history,  # Keep for template checks (test.full_name in history)
        history_for_js={},  # Empty - will be loaded from JSON
        history_descriptions={},  # Empty - will be loaded from JSON
        test_descriptions={},  # Empty - will be loaded from JSON
        test_success_rates={},  # Empty - will be loaded from JSON
        data_file_url=data_file_url,  # URL to JSON data file
        build_preset=build_preset,
        build_preset_params=build_preset_params,
        branch=branch,
        pr_number=pr_number,
        pr_url=pr_url,
        workflow_run_id=workflow_run_id,
        workflow_url=workflow_url,
        owner_area_mapping=owner_area_mapping,
        commit_sha=github_sha
    )

    with open(fn, "w", encoding="utf-8") as fp:
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


def iter_build_results_files(path):
    """Iterate over build-results-report JSON files"""
    import glob
    
    if os.path.isfile(path):
        files = [path]
    else:
        # If it's a directory, look for report.json files
        files = glob.glob(os.path.join(path, "**/report.json"), recursive=True)
        if not files:
            files = glob.glob(os.path.join(path, "report.json"))
    
    for fn in files:
        try:
            with open(fn, 'r') as f:
                report = json.load(f)
            
            for result in report.get("results") or []:
                # Only include results that have a status field (indicates it's a test/check)
                # Filtering (suite, build, configure) is done by transform_build_results.py
                status = result.get("status")
                if not status:
                    continue
                
                yield fn, result
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Unable to parse {fn}: {e}", file=sys.stderr)
            continue


def _collect_stderr_urls_for_verify_badges(tests):
    """HTML summary: stderr URLs for failing tests (VERIFY badge vs logs, independent of other badges)."""
    urls = []
    for test in tests:
        st = getattr(test, "status", None)
        if st is None or not getattr(st, "is_error", False):
            continue
        raw = getattr(test, "stderr_url", None) or ""
        if normalize_fetch_url(raw):
            urls.append(raw)
    return urls


def _apply_verify_badges(tests, stderr_fetch_cache):
    for test in tests:
        st = getattr(test, "status", None)
        if st is None or not getattr(st, "is_error", False):
            test.is_verify_issue = False
            continue
        su = normalize_fetch_url(getattr(test, "stderr_url", None) or "")
        stderr_text = stderr_fetch_cache.get(su, "") if su else None
        test.is_verify_issue = is_verify_classification(
            getattr(test, "error_type", None),
            getattr(test, "status_description", None),
            error_file_text=stderr_text,
        )


def gen_summary(public_dir, public_dir_url, paths, is_retry: bool, build_preset, branch, pr_number=None, workflow_run_id=None):
    summary = TestSummary(is_retry=is_retry)
    stderr_fetch_cache = {}

    for title, html_fn, path in paths:
        summary_line = TestSummaryLine(title)

        for fn, result in iter_build_results_files(path):
            test_result = TestResult.from_build_results_report(result)
            summary_line.add(test_result)

        urls_to_fetch = _collect_stderr_urls_for_verify_badges(summary_line.tests)
        stderr_fetch_cache = prefetch_texts_by_urls(urls_to_fetch, existing_cache=stderr_fetch_cache)
        _apply_verify_badges(summary_line.tests, stderr_fetch_cache)
        
        if os.path.isabs(html_fn):
            html_fn = os.path.relpath(html_fn, public_dir)
        report_url = f"{public_dir_url}/{html_fn}"

        render_testlist_html(summary_line.tests, os.path.join(public_dir, html_fn), build_preset, branch, pr_number, workflow_run_id)
        summary_line.add_report(html_fn, report_url)
        summary.add_line(summary_line)

    return summary


def get_comment_text(summary: TestSummary, summary_links: str, is_last_retry: bool, is_test_result_ignored: bool)->tuple[str, list[str]]:
    color = "red"
    if summary.is_failed:
        if is_test_result_ignored:
            color = "yellow"
            result = f"Some tests failed, follow the links below. This fail is not in blocking policy yet"
        else:
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
    parser.add_argument('--branch', default="main", required=False)
    parser.add_argument('--status_report_file', required=False)
    parser.add_argument('--is_retry', required=True, type=int)
    parser.add_argument('--is_last_retry', required=True, type=int)
    parser.add_argument('--is_test_result_ignored', required=True, type=int)
    parser.add_argument('--comment_color_file', required=True)
    parser.add_argument('--comment_text_file', required=True)
    parser.add_argument('--pr_number', required=False, type=int, help="Pull request number")
    parser.add_argument('--workflow_run_id', required=False, help="GitHub workflow run ID")
    parser.add_argument("args", nargs="+", metavar="TITLE html_out build-results-report-path")
    args = parser.parse_args()

    if len(args.args) % 3 != 0:
        print("Invalid argument count")
        raise SystemExit(-1)

    paths = iter(args.args)
    title_path = list(zip(paths, paths, paths))

    summary = gen_summary(args.public_dir,
                          args.public_dir_url,
                          title_path,
                          is_retry=bool(args.is_retry),
                          build_preset=args.build_preset,
                          branch=args.branch,
                          pr_number=args.pr_number,
                          workflow_run_id=args.workflow_run_id
                          )
    write_summary(summary)

    if summary.is_failed and not args.is_test_result_ignored:
        overall_status = "failure"
    else:
        overall_status = "success"

    color, text = get_comment_text(summary, args.summary_links, is_last_retry=bool(args.is_last_retry), is_test_result_ignored=args.is_test_result_ignored)

    with open(args.comment_color_file, "w") as f:
        f.write(color)

    with open(args.comment_text_file, "w") as f:
        f.write('\n'.join(text))
        f.write('\n')

    with open(args.status_report_file, "w") as f:
        f.write(overall_status)


if __name__ == "__main__":
    main()
