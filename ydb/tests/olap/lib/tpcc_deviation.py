from __future__ import annotations

import allure
import html
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from math import exp, inf, log
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.lib.utils import get_external_param

LOGGER = logging.getLogger(__name__)

MAIN_BRANCH = 'origin/main'

# Baseline is the "UnixBench" aggregate of the previous runs: take the best
# BASELINE_BEST of BASELINE_RUNS values and return their geometric mean, the way
# TTestInfo does it in ydb/public/lib/ydb_cli/commands/benchmark_utils.cpp.
BASELINE_RUNS = 6
BASELINE_BEST = 4
# Below this the baseline is too noisy to say anything, so the check is skipped.
MIN_BASELINE_RUNS = 2
DEFAULT_MAX_DEVIATION_PERCENT = 5.0

_OK_COLOR = '#90EE90'
_FAILED_COLOR = '#FA8072'
_STABLE_BRANCH_RE = re.compile(r'^(?:origin/)?stable-(\d+)-(\d+)(?:-(\d+))?$')


class CheckMode(StrEnum):
    OFF = 'off'
    REPORT = 'report'
    FAIL = 'fail'

    @staticmethod
    def get() -> CheckMode:
        param = os.getenv('TPCC_CHECK_DEVIATION')
        if param is None:
            param = get_external_param('tpcc-check-deviation', '')
        mode = _MODE_ALIASES.get(str(param).strip().lower())
        if mode is None:
            raise ValueError(f'invalid TPCC_CHECK_DEVIATION {param}')
        return mode


_MODE_ALIASES = {
    '': CheckMode.OFF,
    '0': CheckMode.OFF,
    'f': CheckMode.OFF,
    'false': CheckMode.OFF,
    'no': CheckMode.OFF,
    'off': CheckMode.OFF,
    'none': CheckMode.OFF,
    '2': CheckMode.REPORT,
    'report': CheckMode.REPORT,
    'report_only': CheckMode.REPORT,
    'warn': CheckMode.REPORT,
    'warning': CheckMode.REPORT,
    'soft': CheckMode.REPORT,
    '1': CheckMode.FAIL,
    't': CheckMode.FAIL,
    'true': CheckMode.FAIL,
    'yes': CheckMode.FAIL,
    'da': CheckMode.FAIL,
    'on': CheckMode.FAIL,
    'fail': CheckMode.FAIL,
    'error': CheckMode.FAIL,
    'strict': CheckMode.FAIL,
}


@dataclass(frozen=True)
class Metric:
    column: str
    name: str
    higher_is_better: bool


METRICS = (
    Metric('tpmC', 'tpmC', True),
    Metric('newOrderLatency90', 'NewOrder p90', False),
)


@dataclass
class MetricCheck:
    metric: Metric
    current: float
    max_deviation: float
    baseline: float | None = None
    # Degradation as a fraction of the baseline: positive means worse than the baseline.
    deviation: float | None = None
    # Indexes of the previous runs whose values form the baseline.
    used_runs: list[int] = field(default_factory=list)
    skip_reason: str = ''

    @property
    def failed(self) -> bool:
        return self.deviation is not None and self.deviation > self.max_deviation

    @property
    def status(self) -> str:
        if self.skip_reason:
            return 'skipped'
        return 'FAILED' if self.failed else 'ok'

    @property
    def report_text(self) -> str:
        if self.skip_reason:
            return f'{self.metric.name}: skipped, {self.skip_reason}'
        return f'{self.metric.name}: {100 * self.deviation:+.2f}% ({self.status})'

    @property
    def error_message(self) -> str:
        return (
            f'TPC-C {self.metric.name} degraded by {100 * self.deviation:.2f}% '
            f'(allowed {100 * self.max_deviation:.2f}%): {self.current:.2f} '
            f'vs baseline {self.baseline:.2f}, unixbench of {len(self.used_runs)} best of the previous runs'
        )


@dataclass
class DeviationCheckResult:
    # Non-empty when the current run must be treated as failed.
    errors: list[str] = field(default_factory=list)
    # Short one-line status for the Allure table, empty when the check did not run.
    summary: str = ''


def get_max_deviation() -> float:
    """Allowed degradation as a fraction of the baseline."""
    raw = os.getenv('TPCC_MAX_DEVIATION_PERCENT') or get_external_param('tpcc-max-deviation-percent', '')
    if not raw:
        return DEFAULT_MAX_DEVIATION_PERCENT / 100.0
    return float(raw) / 100.0


def _stable_branch_key(branch: str):
    """Sort key of a stable branch, newest first, or None for a non-stable branch.

    The branch line itself is newer than any release cut from it, so the order is
    stable-26-2, stable-26-2-2, stable-26-2-1, stable-26-1, stable-26-1-1, ...
    """
    match = _STABLE_BRANCH_RE.match(branch or '')
    if match is None:
        return None
    patch = match.group(3)
    return (-int(match.group(1)), -int(match.group(2)), -inf if patch is None else -int(patch))


def branch_fallback_chain(current_branch: str, known_branches) -> list[str]:
    """Branches to take the baseline from: the current one, then older ones."""
    chain = [current_branch]
    current_key = _stable_branch_key(current_branch)
    if current_key is None and current_branch != MAIN_BRANCH and MAIN_BRANCH in known_branches:
        # A branch outside of the stable numbering (a feature branch, a fork):
        # main is the closest thing to its own history.
        chain.append(MAIN_BRANCH)
    stable = []
    for branch in set(known_branches):
        key = _stable_branch_key(branch)
        if key is not None and branch != current_branch:
            stable.append((key, branch))
    for key, branch in sorted(stable):
        # A smaller key means a newer branch, and only older ones may be a fallback.
        if current_key is None or key > current_key:
            chain.append(branch)
    return chain


def _unixbench(values: list[tuple[int, float]], higher_is_better: bool) -> tuple[float, list[int]]:
    """Geometric mean of the best BASELINE_BEST/BASELINE_RUNS of the (run index, value) pairs."""
    best_count = max(1, len(values) * BASELINE_BEST // BASELINE_RUNS)
    best = sorted(values, key=lambda item: item[1], reverse=higher_is_better)[:best_count]
    baseline = exp(sum(log(value) for _, value in best) / len(best))
    return baseline, sorted(index for index, _ in best)


def _collect_history(history: list, chain: list[str]) -> tuple[list, list]:
    """Baseline runs and everything the branches they came from have to show.

    Returns the last BASELINE_RUNS runs taken along the branch chain, and the rows
    of the visited branches as (branch, row, index in the baseline or None).
    """
    by_branch: dict[str, list] = {}
    for row in history:
        by_branch.setdefault(row['git_branch'], []).append(row)
    runs = []
    displayed = []
    for branch in chain:
        if len(runs) >= BASELINE_RUNS:
            break
        for row in sorted(by_branch.get(branch, []), key=lambda row: row['timestamp'], reverse=True):
            index = None
            if len(runs) < BASELINE_RUNS:
                index = len(runs)
                runs.append(row)
            displayed.append((branch, row, index))
    return runs, displayed


def _check_metric(metric: Metric, current: float, runs: list, max_deviation: float) -> MetricCheck:
    values = []
    for index, row in enumerate(runs):
        value = row[metric.column]
        if value and float(value) > 0:
            values.append((index, float(value)))
    if len(values) < MIN_BASELINE_RUNS:
        return MetricCheck(metric, current, max_deviation, skip_reason=f'only {len(values)} usable previous values')
    baseline, used_runs = _unixbench(values, metric.higher_is_better)
    deviation = (baseline - current) / baseline if metric.higher_is_better else (current - baseline) / baseline
    check = MetricCheck(metric, current, max_deviation, baseline, deviation, used_runs)
    LOGGER.info(
        f'TPC-C deviation {check.status}: {metric.name} current {current}, baseline {baseline}, '
        f'deviation {100 * deviation:+.2f}%, limit {100 * max_deviation:.2f}%, previous values {values}'
    )
    return check


def _format_timestamp(value) -> str:
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    try:
        return datetime.fromtimestamp(int(value) / 1000000).strftime('%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError, OSError, OverflowError):
        return str(value)


def _table(header: list[str], rows: list[list]) -> str:
    """Cells are either a text or a (text, background color) pair."""
    result = '<table border="1" cellpadding="2px" cellspacing="0" style="margin-bottom: 10px">'
    result += '<tr>' + ''.join(f'<th>{html.escape(str(cell))}</th>' for cell in header) + '</tr>'
    for row in rows:
        result += '<tr>'
        for cell in row:
            text, color = cell if isinstance(cell, tuple) else (cell, None)
            color = f' bgcolor="{color}"' if color else ''
            result += f'<td{color}>{html.escape(str(text))}</td>'
        result += '</tr>'
    return result + '</table>'


def _previous_runs_report(displayed: list, checks: list[MetricCheck]) -> str:
    used = {check.metric.column: set(check.used_runs) for check in checks}
    rows = []
    for branch, row, index in displayed:
        cells = ['-' if index is None else str(index + 1), branch, _format_timestamp(row['timestamp'])]
        for metric in METRICS:
            value = row[metric.column]
            in_baseline = index is not None and index in used.get(metric.column, set())
            cells.append(('' if value is None else value, _OK_COLOR if in_baseline else None))
        rows.append(cells)
    return (
        '<h4>Previous runs</h4>'
        '<div>The numbered ones are the baseline runs, the highlighted values are the ones it is built of.</div>'
        + _table(['#', 'Branch', 'Timestamp'] + [metric.name for metric in METRICS], rows)
    )


def _baseline_report(checks: list[MetricCheck]) -> str:
    rows = []
    for check in checks:
        color = _FAILED_COLOR if check.failed else _OK_COLOR if not check.skip_reason else None
        rows.append([
            check.metric.name,
            f'{check.current:.2f}',
            'n/a' if check.baseline is None else f'{check.baseline:.2f}',
            check.skip_reason if check.deviation is None else f'{100 * check.deviation:+.2f}%',
            f'{100 * check.max_deviation:.2f}%',
            (check.status, color),
        ])
    return (
        '<h4>Check result</h4>'
        + _table(['Metric', 'Current', 'Baseline (unixbench)', 'Degradation', 'Allowed', 'Status'], rows)
    )


def _check(results: dict, run_type: str, run_ts: float, mode: CheckMode, report: list[str]) -> DeviationCheckResult:
    run_context = ResultsProcessor.get_tpcc_run_context()
    metrics = ResultsProcessor.get_tpcc_metrics(results)
    warehouses = int(metrics['warehouses'] or 0)
    if warehouses <= 0:
        raise ValueError(f'no warehouses count in the TPC-C results, got {metrics["warehouses"]!r}')
    cluster = run_context['cluster']
    branch = run_context['branch']
    max_deviation = get_max_deviation()
    report.append(_table(['Parameter', 'Value'], [
        ['Cluster', cluster],
        ['Branch', branch],
        ['Warehouses', warehouses],
        ['Run type', run_type],
        ['Mode', str(mode)],
        ['Allowed degradation', f'{100 * max_deviation:.2f}%'],
        ['Baseline', f'unixbench (geometric mean) of {BASELINE_BEST} best of {BASELINE_RUNS} previous runs'],
    ]))

    history = ResultsProcessor.get_tpcc_history(
        cluster=cluster,
        warehouses=warehouses,
        run_type=run_type,
        before_ts=run_ts,
        per_branch_limit=BASELINE_RUNS,
    )
    chain = branch_fallback_chain(branch, {row['git_branch'] for row in history})
    runs, displayed = _collect_history(history, chain)
    report.append(f'<div>Branches to fall back on: {html.escape(", ".join(chain))}</div>')

    if len(runs) < MIN_BASELINE_RUNS:
        summary = f'skipped, only {len(runs)} previous runs found'
        report.append(_previous_runs_report(displayed, []))
        report.append(f'<h4>Check result</h4><div>{html.escape(summary)}</div>')
        LOGGER.warning(f'TPC-C deviation check {summary}')
        return DeviationCheckResult(summary=summary)

    checks = []
    for metric in METRICS:
        current = float(metrics[metric.column] or 0)
        if current <= 0:
            raise ValueError(f'no {metric.name} in the TPC-C results, got {metrics[metric.column]!r}')
        checks.append(_check_metric(metric, current, runs, max_deviation))
    report.append(_previous_runs_report(displayed, checks))
    report.append(_baseline_report(checks))
    return DeviationCheckResult(
        errors=[check.error_message for check in checks if check.failed],
        summary=', '.join(check.report_text for check in checks),
    )


def check_tpcc_deviation(results: dict, run_type: str, run_ts: float) -> DeviationCheckResult:
    """Compare the finished run against the baseline of the previous ones.

    Never raises: the caller must be able to store the current results even when
    the check itself has failed, so problems are reported as errors of the run.
    """
    try:
        mode = CheckMode.get()
    except ValueError as e:
        LOGGER.error(f'TPC-C deviation check is misconfigured: {e}')
        return DeviationCheckResult(errors=[str(e)], summary='failed')
    if mode == CheckMode.OFF:
        return DeviationCheckResult()
    report: list[str] = ['<h4>TPC-C deviation check</h4>']
    with allure.step('Check TPC-C results deviation'):
        try:
            result = _check(results, run_type, run_ts, mode, report)
        except BaseException as e:
            LOGGER.exception('TPC-C deviation check failed')
            message = f'TPC-C deviation check failed: {e}'
            report.append(f'<h4>Check result</h4><div bgcolor="{_FAILED_COLOR}">{html.escape(message)}</div>')
            result = DeviationCheckResult(errors=[message], summary='failed')
        if mode == CheckMode.REPORT and result.errors:
            LOGGER.warning(f'TPC-C deviation check is report only, the test is not failed by: {result.errors}')
            report.append('<div>Report only mode: this check does not fail the test.</div>')
            result = DeviationCheckResult(summary=f'{result.summary} (report only)')
        allure.attach('\n'.join(report), 'TPC-C deviation check', attachment_type=allure.attachment_type.HTML)
    return result
