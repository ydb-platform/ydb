from __future__ import annotations

import allure
import logging
import os
import re
from dataclasses import dataclass, field
from math import exp, inf, log
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.lib.utils import external_param_is_true, get_external_param

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

_STABLE_BRANCH_RE = re.compile(r'^(?:origin/)?stable-(\d+)-(\d+)(?:-(\d+))?$')


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
class MetricDeviation:
    metric: Metric
    current: float
    baseline: float
    # Degradation as a fraction of the baseline: positive means worse than the baseline.
    deviation: float
    used_values: list[float]

    @property
    def error_message(self) -> str:
        return (
            f'TPC-C {self.metric.name} deviates from the baseline by {100 * self.deviation:+.2f}%: '
            f'{self.current:.2f} vs baseline {self.baseline:.2f} '
            f'(unixbench of {len(self.used_values)} best of the previous runs)'
        )


@dataclass
class DeviationCheckResult:
    # Non-empty when the current run must be treated as failed.
    errors: list[str] = field(default_factory=list)
    # Short one-line status for the Allure table, empty when the check did not run.
    summary: str = ''


def is_enabled() -> bool:
    env = os.getenv('TPCC_CHECK_DEVIATION')
    if env is not None:
        return env.strip().lower() in ['t', 'true', 'yes', '1', 'da']
    return external_param_is_true('tpcc-check-deviation')


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


def _unixbench(values: list[float], higher_is_better: bool) -> tuple[float, list[float]]:
    """Geometric mean of the best BASELINE_BEST/BASELINE_RUNS of the values."""
    best_count = max(1, len(values) * BASELINE_BEST // BASELINE_RUNS)
    best = sorted(values, reverse=higher_is_better)[:best_count]
    return exp(sum(log(value) for value in best) / len(best)), best


def _collect_baseline_runs(history: list, chain: list[str]) -> tuple[list, list[str]]:
    """Last BASELINE_RUNS runs, taken along the branch chain until there are enough."""
    by_branch: dict[str, list] = {}
    for row in history:
        by_branch.setdefault(row['git_branch'], []).append(row)
    runs = []
    used_branches = []
    for branch in chain:
        rows = sorted(by_branch.get(branch, []), key=lambda row: row['timestamp'], reverse=True)
        rows = rows[:BASELINE_RUNS - len(runs)]
        if not rows:
            continue
        runs.extend(rows)
        used_branches.append(f'{branch}: {len(rows)}')
        if len(runs) >= BASELINE_RUNS:
            break
    return runs, used_branches


def _check_metric(metric: Metric, current: float, runs: list, max_deviation: float) -> tuple[MetricDeviation | None, str]:
    values = [float(row[metric.column]) for row in runs if row[metric.column] and float(row[metric.column]) > 0]
    if len(values) < MIN_BASELINE_RUNS:
        return None, f'{metric.name}: skipped, only {len(values)} usable previous values'
    baseline, used_values = _unixbench(values, metric.higher_is_better)
    deviation = (baseline - current) / baseline if metric.higher_is_better else (current - baseline) / baseline
    result = MetricDeviation(metric, current, baseline, deviation, used_values)
    status = 'FAILED' if deviation > max_deviation else 'ok'
    LOGGER.info(
        f'TPC-C deviation {status}: {metric.name} current {current}, baseline {baseline}, '
        f'deviation {100 * deviation:+.2f}%, limit {100 * max_deviation:.2f}%, values {values}, used {used_values}'
    )
    report = f'{metric.name}: {100 * deviation:+.2f}% ({status})'
    return (result if deviation > max_deviation else None), report


def _report(lines: list[str]) -> None:
    allure.attach('\n'.join(lines), 'TPC-C deviation check', attachment_type=allure.attachment_type.TEXT)


def _check(results: dict, run_type: str, run_ts: float) -> DeviationCheckResult:
    run_context = ResultsProcessor.get_tpcc_run_context()
    metrics = ResultsProcessor.get_tpcc_metrics(results)
    warehouses = int(metrics['warehouses'] or 0)
    if warehouses <= 0:
        raise ValueError(f'no warehouses count in the TPC-C results, got {metrics["warehouses"]!r}')
    cluster = run_context['cluster']
    branch = run_context['branch']
    max_deviation = get_max_deviation()
    lines = [
        f'cluster: {cluster}, branch: {branch}, warehouses: {warehouses}, run type: {run_type}',
        f'allowed degradation: {100 * max_deviation:.2f}%, baseline: unixbench of {BASELINE_BEST} best of {BASELINE_RUNS} previous runs',
    ]

    history = ResultsProcessor.get_tpcc_history(
        cluster=cluster,
        warehouses=warehouses,
        run_type=run_type,
        before_ts=run_ts,
        per_branch_limit=BASELINE_RUNS,
    )
    chain = branch_fallback_chain(branch, {row['git_branch'] for row in history})
    runs, used_branches = _collect_baseline_runs(history, chain)
    lines.append(f'branches to fall back on: {", ".join(chain)}')
    lines.append(f'previous runs taken: {", ".join(used_branches) if used_branches else "none"}')

    if len(runs) < MIN_BASELINE_RUNS:
        summary = f'skipped, only {len(runs)} previous runs found'
        lines.append(summary)
        _report(lines)
        LOGGER.warning(f'TPC-C deviation check {summary}')
        return DeviationCheckResult(summary=summary)

    errors = []
    reports = []
    for metric in METRICS:
        current = float(metrics[metric.column] or 0)
        if current <= 0:
            raise ValueError(f'no {metric.name} in the TPC-C results, got {metrics[metric.column]!r}')
        deviation, report = _check_metric(metric, current, runs, max_deviation)
        reports.append(report)
        if deviation is not None:
            errors.append(deviation.error_message)
    lines.extend(reports)
    _report(lines)
    return DeviationCheckResult(errors=errors, summary=', '.join(reports))


def check_tpcc_deviation(results: dict, run_type: str, run_ts: float) -> DeviationCheckResult:
    """Compare the finished run against the baseline of the previous ones.

    Never raises: the caller must be able to store the current results even when
    the check itself has failed, so problems are reported as errors of the run.
    """
    if not is_enabled():
        return DeviationCheckResult()
    with allure.step('Check TPC-C results deviation'):
        try:
            return _check(results, run_type, run_ts)
        except BaseException as e:
            LOGGER.exception('TPC-C deviation check failed')
            message = f'TPC-C deviation check failed: {e}'
            _report([message])
            return DeviationCheckResult(errors=[message], summary='failed')
