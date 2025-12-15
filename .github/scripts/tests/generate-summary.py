#!/usr/bin/env python3
import argparse
import dataclasses
import os
import sys
import traceback
import re
import json
from codeowners import CodeOwners
from enum import Enum
from operator import attrgetter
from typing import List, Dict
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from get_test_history import get_test_history


def load_owner_area_mapping():
    """Load owner to area label mapping from JSON config file."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_dir = os.path.join(script_dir, '..', '..', 'config')
        mapping_file = os.path.join(config_dir, 'owner_area_mapping.json')
        with open(mapping_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Warning: Could not load owner area mapping: {e}")
        return {}


def is_sanitizer_issue(error_text):
    """
    Detect if a test failure is caused by a sanitizer.
    Returns True if the error text contains sanitizer-specific patterns.
    """
    if not error_text:
        return False
    
    # Sanitizer error patterns for comprehensive coverage
    sanitizer_patterns = [
        # Main sanitizer patterns with severity levels (covers most cases)
        r'(ERROR|WARNING|SUMMARY): (AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)',
        
        # Process ID prefixed patterns (format: ==PID==SEVERITY: SANITIZER)
        r'==\d+==\s*(ERROR|WARNING|SUMMARY): (AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)',
        
        # UndefinedBehaviorSanitizer runtime errors
        r'runtime error:',
        r'==\d+==.*runtime error:',
        
        # Memory leak detection (specific LeakSanitizer output)
        r'detected memory leaks',
        r'==\d+==.*detected memory leaks',
    ]
    
    for pattern in sanitizer_patterns:
        if re.search(pattern, error_text, re.IGNORECASE | re.MULTILINE):
            return True
    
    return False


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
    error_type: str = ""
    is_sanitizer_issue: bool = False
    is_timeout_issue: bool = False

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
        is_muted = bool(result.get("muted"))
        
        classname = path_str
        if subtest_name and subtest_name.strip():
            if name_part:
                name = f"{name_part}.{subtest_name}"
            else:
                name = subtest_name
        else:
            name = name_part or ""
        
        if status_str == "OK":
            status_str = "PASSED"
        
        # Map status to TestStatus enum
        # All ERROR statuses are treated as FAIL
        if is_muted:
            status = TestStatus.MUTE
        elif status_str == "FAILED" or status_str == "ERROR":
            status = TestStatus.FAIL
        elif status_str == "SKIPPED":
            status = TestStatus.SKIP
        else:
            status = TestStatus.PASS
        
        # Extract log URLs from properties
        # Properties (added by transform_build_results.py) contain URLs in format: {"url:log": "...", "url:stdout": "..."}
        log_urls = {}
        if properties and isinstance(properties, dict):
            # Check both "url:key" (primary format) and "key" (fallback for backward compatibility)
            log_keys = ['log', 'logsdir', 'stdout', 'stderr']
            for key in log_keys:
                # Try "url:key" format first (primary format from transform_build_results.py)
                url = properties.get(f"url:{key}")
                if not url:
                    # Fallback to just "key" (for backward compatibility)
                    url = properties.get(key)
                if url:
                    if key not in log_urls:  # Don't overwrite if already found
                        log_urls[key] = url
        
        # Note: Links are processed later in gen_summary() to handle both URLs and file paths
        # (file paths need public_dir_url for conversion, which is only available in gen_summary)
        
        # Extract elapsed time from metrics
        elapsed = 0.0
        if metrics and isinstance(metrics, dict):
            elapsed_time = metrics.get("elapsed_time")
            if elapsed_time is not None:
                try:
                    elapsed = float(elapsed_time)
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
            error_type=error_type or '',
            is_sanitizer_issue=is_sanitizer_issue(status_description or ''),
            is_timeout_issue=(error_type or '').upper() == 'TIMEOUT'
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
                render_pm(line.passed, f"{report_url}?status=Passed", 0),
                render_pm(line.errors, f"{report_url}?status=Error", 0),
                render_pm(line.failed, f"{report_url}?status=Failed", 0),
                render_pm(line.skipped, f"{report_url}?status=Skipped", 0),
                render_pm(line.muted, f"{report_url}?status=Mute", 0),
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


def group_tests_by_suite(tests):
    """Group tests by suite (classname)"""
    suites = {}
    for test in tests:
        suite_name = test.classname
        if suite_name not in suites:
            suites[suite_name] = []
        suites[suite_name].append(test)
    
    # Sort tests within each suite
    for suite_name in suites:
        suites[suite_name].sort(key=attrgetter("full_name"))
    
    return suites


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
        history = get_test_history(tests_names_for_history, last_n_runs, build_preset, branch)
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
    # sorted by test name
    for current_status in status_for_history:
        status_test.get(current_status,[]).sort(key=lambda val: (val.full_name, ))

    buid_preset_params = '--build unknown_build_type'
    if build_preset == 'release-asan' :
        buid_preset_params = '--build "release" --sanitize="address" -DDEBUGINFO_LINES_ONLY'
    elif build_preset == 'release-msan':
        buid_preset_params = '--build "release" --sanitize="memory" -DDEBUGINFO_LINES_ONLY'
    elif build_preset == 'release-tsan':   
        buid_preset_params = '--build "release" --sanitize="thread" -DDEBUGINFO_LINES_ONLY'
    elif build_preset == 'relwithdebinfo':
        buid_preset_params = '--build "relwithdebinfo"'
    
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
        
    content = env.get_template("summary.html").render(
        status_order=status_order,
        tests=status_test,
        has_any_log=has_any_log,
        history=history,
        build_preset=build_preset,
        buid_preset_params=buid_preset_params,
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


def render_testlist_html_v2(rows, fn, build_preset, branch, pr_number=None, workflow_run_id=None):
    """New version: Group tests by suite with filters"""
    TEMPLATES_PATH = os.path.join(os.path.dirname(__file__), "templates")

    env = Environment(loader=FileSystemLoader(TEMPLATES_PATH), undefined=StrictUndefined)

    # Include all statuses including SKIP for display
    visible_rows = [t for t in rows if t.status in [TestStatus.PASS, TestStatus.FAIL, TestStatus.ERROR, TestStatus.MUTE, TestStatus.SKIP]]
    
    # Group tests by suite (only visible tests)
    suites_dict = group_tests_by_suite(visible_rows)
    
    # Also group by status for status-based view
    status_test = {}
    for t in rows:
        status_test.setdefault(t.status, []).append(t)
    
    # Get testowners for all tests
    dir = os.path.dirname(__file__)
    git_root = f"{dir}/../../.."
    codeowners = f"{git_root}/.github/TESTOWNERS"
    get_codeowners_for_tests(codeowners, rows)
    
    # Get history for failed and muted tests
    status_for_history = [TestStatus.FAIL, TestStatus.MUTE, TestStatus.ERROR]
    status_for_history = [s for s in status_for_history if s in status_test]
    tests_in_statuses = [test for status in status_for_history for test in status_test.get(status, [])]
    tests_names_for_history = [test.full_name for test in tests_in_statuses]
    
    history = {}
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
    
    print(f"Calculated success rates for {len(test_success_rates)} tests")
    
    # Group by status -> suite -> tests
    # Convert to dict with status enum as key for template
    status_suites = {}
    for status in [TestStatus.ERROR, TestStatus.FAIL, TestStatus.SKIP, TestStatus.MUTE, TestStatus.PASS]:
        if status in status_test:
            status_suites[status] = group_tests_by_suite(status_test[status])
    
    # Calculate test counts for filters
    # Include all statuses including SKIP
    visible_tests = [
        t for t in rows 
        if t.status in [TestStatus.PASS, TestStatus.FAIL, TestStatus.ERROR, TestStatus.MUTE, TestStatus.SKIP]
    ]
    
    # Count sanitizer per status (for badge display)
    sanitizer_failed = [t for t in status_test.get(TestStatus.FAIL, []) if t.is_sanitizer_issue]
    sanitizer_error = [t for t in status_test.get(TestStatus.ERROR, []) if t.is_sanitizer_issue]
    sanitizer_mute = [t for t in status_test.get(TestStatus.MUTE, []) if t.is_sanitizer_issue]
    sanitizer_passed = [t for t in status_test.get(TestStatus.PASS, []) if t.is_sanitizer_issue]
    sanitizer_skipped = [t for t in status_test.get(TestStatus.SKIP, []) if t.is_sanitizer_issue]
    
    # Count timeout per status (for badge display)
    timeout_failed = [t for t in status_test.get(TestStatus.FAIL, []) if t.is_timeout_issue]
    timeout_error = [t for t in status_test.get(TestStatus.ERROR, []) if t.is_timeout_issue]
    timeout_mute = [t for t in status_test.get(TestStatus.MUTE, []) if t.is_timeout_issue]
    timeout_passed = [t for t in status_test.get(TestStatus.PASS, []) if t.is_timeout_issue]
    timeout_skipped = [t for t in status_test.get(TestStatus.SKIP, []) if t.is_timeout_issue]
    
    # Count all tests
    test_counts = {
        'all': len(visible_tests),
        'passed': len(status_test.get(TestStatus.PASS, [])),
        'failed': len(status_test.get(TestStatus.FAIL, [])),
        'error': len(status_test.get(TestStatus.ERROR, [])),
        'mute': len(status_test.get(TestStatus.MUTE, [])),
        'skipped': len(status_test.get(TestStatus.SKIP, [])),
        'sanitizer': len([t for t in rows if t.is_sanitizer_issue]),  # Total sanitizer count
        'sanitizer_failed': len(sanitizer_failed),  # Count of sanitizer within failed
        'sanitizer_error': len(sanitizer_error),    # Count of sanitizer within error
        'sanitizer_mute': len(sanitizer_mute),     # Count of sanitizer within mute
        'sanitizer_passed': len(sanitizer_passed),  # Count of sanitizer within passed
        'sanitizer_skipped': len(sanitizer_skipped),  # Count of sanitizer within skipped
        'sanitizer_all': len([t for t in visible_tests if t.is_sanitizer_issue]),  # Count of sanitizer in visible tests
        'timeout': len([t for t in rows if t.is_timeout_issue]),  # Total timeout count
        'timeout_failed': len(timeout_failed),  # Count of timeout within failed
        'timeout_error': len(timeout_error),    # Count of timeout within error
        'timeout_mute': len(timeout_mute),     # Count of timeout within mute
        'timeout_passed': len(timeout_passed),  # Count of timeout within passed
        'timeout_skipped': len(timeout_skipped),  # Count of timeout within skipped
        'timeout_all': len([t for t in visible_tests if t.is_timeout_issue])  # Count of timeout in visible tests
    }
    
    # Group sanitizer tests by suite
    sanitizer_tests = [t for t in rows if t.is_sanitizer_issue]
    sanitizer_suites = group_tests_by_suite(sanitizer_tests) if sanitizer_tests else {}
    
    # Build preset params
    buid_preset_params = '--build unknown_build_type'
    if build_preset == 'release-asan':
        buid_preset_params = '--build "release" --sanitize="address" -DDEBUGINFO_LINES_ONLY'
    elif build_preset == 'release-msan':
        buid_preset_params = '--build "release" --sanitize="memory" -DDEBUGINFO_LINES_ONLY'
    elif build_preset == 'release-tsan':
        buid_preset_params = '--build "release" --sanitize="thread" -DDEBUGINFO_LINES_ONLY'
    elif build_preset == 'relwithdebinfo':
        buid_preset_params = '--build "relwithdebinfo"'
    
    # Get GitHub server URL and repository from environment
    github_server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    github_repository = os.environ.get("GITHUB_REPOSITORY", "ydb-platform/ydb")
    github_sha = os.environ.get("GITHUB_HEAD_SHA", "")
    
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
                    "date": hist_data["datetime"],
                    "commit": hist_data["commit"],
                    "job_name": hist_data["job_name"],
                    "job_id": hist_data["job_id"],
                    "branch": hist_data["branch"]
                    # status_description removed to reduce HTML size - stored separately
                }
                # Store description separately (only for failed/error/mute and if not empty to save space)
                desc = hist_data.get("status_description", "")
                if desc and desc.strip() and status in ("failure", "error", "mute"):
                    history_descriptions[test_name][timestamp_str] = desc
    
    # Prepare test descriptions for current tests (without embedding in HTML to reduce size)
    # Store only for tests with errors (FAIL, ERROR, MUTE, SKIP) and if not empty
    test_descriptions = {}
    for test in rows:
        if test.status in (TestStatus.FAIL, TestStatus.ERROR, TestStatus.MUTE, TestStatus.SKIP):
            desc = test.status_description
            if desc and desc.strip():
                test_descriptions[test.full_name] = desc
    
    # Prepare flat sorted list of all visible tests (for default "no grouping" view)
    all_tests_sorted = sorted(visible_rows, key=attrgetter("full_name"))
    
    # Save data to separate JSON file to reduce HTML size
    data_file = fn.replace('.html', '_data.json')
    data_to_save = {
        'history_for_js': history_for_js,
        'history_descriptions': history_descriptions,
        'test_descriptions': test_descriptions,
        'test_success_rates': test_success_rates
    }
    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(data_to_save, f, separators=(',', ':'))  # Minified JSON
    
    # Calculate data file URL (relative to HTML file)
    data_file_url = os.path.basename(data_file)
    
    content = env.get_template("summary_v2.html").render(
        suites=suites_dict,
        all_tests_sorted=all_tests_sorted,  # Flat sorted list for default view
        status_suites=status_suites,
        sanitizer_suites=sanitizer_suites,
        test_counts=test_counts,
        history=history,  # Keep for template checks (test.full_name in history)
        history_for_js={},  # Empty - will be loaded from JSON
        history_descriptions={},  # Empty - will be loaded from JSON
        test_descriptions={},  # Empty - will be loaded from JSON
        test_success_rates={},  # Empty - will be loaded from JSON
        data_file_url=data_file_url,  # URL to JSON data file
        build_preset=build_preset,
        buid_preset_params=buid_preset_params,
        branch=branch,
        pr_number=pr_number,
        pr_url=pr_url,
        workflow_run_id=workflow_run_id,
        workflow_url=workflow_url,
        owner_area_mapping=owner_area_mapping,
        commit_sha=github_sha,
        all_tests=rows
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


def get_codeowners_for_tests(codeowners_file_path, tests_data):
    with open(codeowners_file_path, 'r', encoding='utf-8') as file:
        data = file.read()
        owners_odj = CodeOwners(data)

        tests_data_with_owners = []
        for test in tests_data:
            target_path = test.classname
            owners = owners_odj.of(target_path)
            test.owners = joined_owners = ";;".join(
                [(":".join(x)) for x in owners])
            tests_data_with_owners.append(test)


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
            with open(fn, 'r', encoding='utf-8') as f:
                report = json.load(f)
            
            for result in report.get("results") or []:
                # Filter: only tests without suite=true and chunk=true
                if (result.get("type") == "test" and 
                    not result.get("suite", False) and 
                    not result.get("chunk", False)):
                    yield fn, result
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Unable to parse {fn}: {e}", file=sys.stderr)
            continue


def gen_summary(public_dir, public_dir_url, paths, is_retry: bool, build_preset, branch, pr_number=None, workflow_run_id=None):
    summary = TestSummary(is_retry=is_retry)

    for title, html_fn, path in paths:
        summary_line = TestSummaryLine(title)

        for fn, result in iter_build_results_files(path):
            test_result = TestResult.from_build_results_report(result)
            
            # Convert file paths in links to URLs if needed
            # Links may contain file paths that need to be converted to URLs
            links = result.get("links", {})
            if links and isinstance(links, dict):
                for link_type in ["stdout", "stderr", "log", "logsdir"]:
                    if link_type in links:
                        link_value = links[link_type]
                        # If it's a list, take the first element
                        if isinstance(link_value, list) and len(link_value) > 0:
                            link_path = link_value[0]
                        elif isinstance(link_value, str):
                            link_path = link_value
                        else:
                            continue
                        
                        # Skip if already found in properties or processed earlier
                        display_key = link_type  # Already lowercase from list
                        if display_key in test_result.log_urls:
                            continue
                        
                        # If it's already a URL, use it
                        if isinstance(link_path, str) and (link_path.startswith("http://") or link_path.startswith("https://")):
                            test_result.log_urls[display_key] = link_path
                        # If it's a file path, try to convert it to URL
                        elif isinstance(link_path, str) and os.path.isabs(link_path):
                            try:
                                # Common pattern: paths like /home/runner/.../tmp/out/...
                                # Extract relative path after tmp/out
                                if "tmp/out" in link_path:
                                    parts = link_path.split("tmp/out", 1)
                                    if len(parts) > 1:
                                        rel_path = parts[1].lstrip("/")
                                        # Construct URL - try common patterns
                                        # Usually logs are accessible via artifacts/logs/ or similar
                                        # Try multiple possible URL patterns
                                        possible_urls = [
                                            f"{public_dir_url}/artifacts/logs/{rel_path}",
                                            f"{public_dir_url}/logs/{rel_path}",
                                            f"{public_dir_url}/{rel_path}",
                                        ]
                                        # Use the first pattern (most common)
                                        test_result.log_urls[display_key] = possible_urls[0]
                            except Exception:
                                # If conversion fails, skip this link
                                pass
            
            summary_line.add(test_result)
        
        if os.path.isabs(html_fn):
            html_fn = os.path.relpath(html_fn, public_dir)
        report_url = f"{public_dir_url}/{html_fn}"

        # Use new version v2 for rendering
        render_testlist_html_v2(summary_line.tests, os.path.join(public_dir, html_fn), build_preset, branch, pr_number, workflow_run_id)
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

    with open(summary_links, encoding='utf-8') as f:
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

    with open(args.comment_color_file, "w", encoding="utf-8") as f:
        f.write(color)

    with open(args.comment_text_file, "w", encoding="utf-8") as f:
        f.write('\n'.join(text))
        f.write('\n')

    with open(args.status_report_file, "w", encoding="utf-8") as f:
        f.write(overall_status)


if __name__ == "__main__":
    main()
