from __future__ import annotations

import allure
import html
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from math import exp, expm1, inf, log, sqrt
from statistics import NormalDist, fmean, median, stdev
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.lib.utils import get_external_param

LOGGER = logging.getLogger(__name__)

MAIN_BRANCH = 'origin/main'

# Two windows of previous runs, both filled along the branch fallback chain.
#
# The baseline is the "UnixBench" aggregate of the last BASELINE_RUNS runs: the
# geometric mean of the best two thirds, the way TTestInfo does it in
# ydb/public/lib/ydb_cli/commands/benchmark_utils.cpp. Dropping the worst third
# is what keeps a flaky run, or a run that has caught a regression, out of the
# baseline, and the more runs the window holds the less one of them can move it.
BASELINE_RUNS = 12
BASELINE_BEST_NUM, BASELINE_BEST_DEN = 2, 3
MIN_BASELINE_RUNS = 2
# The noise window is longer still. The threshold is derived from an estimate of
# the noise, and that estimate is itself a random value: over 30 runs the robust
# estimate wanders by some 20%, over 6 runs by some 50%. Runs beyond the
# baseline window are used for the noise only.
SIGMA_RUNS = 30
MIN_SIGMA_RUNS = 8

# Allowed degradation is sigmas of the noise, one-sided. Three of them keep the
# false alarm rate near a promille per metric under normality, and reproduce the
# 5% that used to be hardcoded when the noise is the ~1.2% we see today.
DEFAULT_SIGMAS = 3.0
# The estimate of the noise is never good enough to be trusted without bounds:
# below the floor the check would fire on a quiet cluster, above the ceiling a
# dirty history would switch it off altogether.
DEFAULT_MIN_LIMIT_PERCENT = 3.0
DEFAULT_MAX_LIMIT_PERCENT = 15.0
# Used only when the adaptive threshold cannot be computed at all.
DEFAULT_FALLBACK_LIMIT_PERCENT = 5.0

_OK_COLOR = '#90EE90'
_FAILED_COLOR = '#FA8072'
# Key measurements are colored by the bounds the threshold itself lives in.
_KM_IMPROVED_COLOR = '#ccffcc'
_KM_SAFE_COLOR = '#eeffcc'
_KM_WATCH_COLOR = '#ffffcc'
_KM_FAILED_COLOR = '#ffcccc'
_KM_INFO_COLOR = '#f5f5f5'

_STABLE_BRANCH_RE = re.compile(r'^(?:origin/)?stable-(\d+)-(\d+)(?:-(\d+))?$')
_NORMAL = NormalDist()


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
    slug: str

    @property
    def improvement_signal(self) -> str:
        return f'tpcc_improvement_{self.slug}'

    @property
    def ratio_signal(self) -> str:
        return f'tpcc_improvement_ratio_{self.slug}'

    @property
    def noise_signal(self) -> str:
        return f'tpcc_noise_{self.slug}'

    @property
    def limit_signal(self) -> str:
        return f'tpcc_improvement_limit_{self.slug}'


METRICS = (
    Metric('tpmC', 'tpmC', True, 'tpmc'),
    Metric('newOrderLatency90', 'NewOrder p90', False, 'neworder_p90'),
)


@dataclass(frozen=True)
class KeyMeasurementSpec:
    """A numeric key measurement and the bounds it is colored by.

    The suite turns these into LoadSuiteBase.KeyMeasurement; the policy lives
    here so that the colors always follow the thresholds actually in use.
    """
    name: str
    caption: str
    description: str
    # (color, min, max), inclusive bounds, first match wins.
    intervals: list[tuple[str, float | None, float | None]]


@dataclass
class Dispersion:
    """Relative spread of the previous runs, as a fraction."""
    runs: int
    mad: float | None = None
    sd: float | None = None
    reason: str = ''

    @property
    def value(self) -> float | None:
        # The robust estimate is the primary one: a single flaky run in the
        # window inflates the classic deviation twofold and the median-based one
        # by a third. It can be exactly zero when the window is full of
        # duplicates (latency is whole milliseconds), and then there is nothing
        # to be robust about and the classic estimate is all there is.
        if self.mad:
            return self.mad
        return self.sd or None


@dataclass
class Threshold:
    """Allowed degradation for one metric, as a fraction of the baseline."""
    limit: float
    adaptive: bool
    sigma: float | None = None
    # How optimistic a baseline built of the best runs is, in sigmas.
    bias: float = 0.0
    sigmas: float = 0.0
    # The value before clamping, when it was clamped.
    raw: float | None = None
    note: str = ''

    @property
    def source(self) -> str:
        return 'adaptive' if self.adaptive else 'fixed'


@dataclass
class MetricCheck:
    metric: Metric
    current: float
    baseline: float | None = None
    # Degradation as a fraction of the baseline: positive means worse than the baseline.
    deviation: float | None = None
    threshold: Threshold | None = None
    dispersion: Dispersion | None = None
    # Indexes of the previous runs whose values form the baseline.
    used_runs: list[int] = field(default_factory=list)
    skip_reason: str = ''

    @property
    def limit(self) -> float | None:
        return None if self.threshold is None else self.threshold.limit

    @property
    def improvement(self) -> float | None:
        """How much better than the baseline the run is: negative is a degradation.

        Everything the report shows is signed this way, so that a bigger number
        always means a better run, whichever direction the metric itself grows in.
        """
        return None if self.deviation is None else -self.deviation

    @property
    def allowed(self) -> float | None:
        """The lowest improvement that still passes."""
        return None if self.limit is None else -self.limit

    @property
    def ratio(self) -> float | None:
        """Improvement in units of the allowed degradation: below -1.0 the check fails."""
        if self.deviation is None or not self.limit:
            return None
        return self.improvement / self.limit

    @property
    def failed(self) -> bool:
        return self.deviation is not None and self.limit is not None and self.deviation > self.limit

    @property
    def status(self) -> str:
        if self.skip_reason:
            return 'skipped'
        return 'FAILED' if self.failed else 'ok'

    @property
    def report_text(self) -> str:
        if self.skip_reason:
            return f'{self.metric.name}: skipped, {self.skip_reason}'
        return (
            f'{self.metric.name}: {100 * self.improvement:+.2f}% '
            f'(allowed {100 * self.allowed:+.2f}% {self.threshold.source}, {self.status})'
        )

    @property
    def error_message(self) -> str:
        noise = '' if self.threshold.sigma is None else f', noise {100 * self.threshold.sigma:.2f}%'
        return (
            f'TPC-C {self.metric.name} degraded by {100 * self.deviation:.2f}% '
            f'(allowed {100 * self.limit:.2f}%, {self.threshold.source}{noise}): '
            f'{self.current:.2f} vs baseline {self.baseline:.2f}, unixbench of '
            f'{len(self.used_runs)} best of the previous runs'
        )


@dataclass
class DeviationCheckResult:
    # Non-empty when the current run must be treated as failed.
    errors: list[str] = field(default_factory=list)
    # Short one-line status for the Allure table, empty when the check did not run.
    summary: str = ''
    # Key measurements of the run: signal name -> numeric value.
    measurements: dict[str, float] = field(default_factory=dict)


def _param(env: str, param: str) -> str:
    raw = os.getenv(env)
    if raw is None:
        raw = get_external_param(param, '')
    return str(raw).strip()


def _percent_param(env: str, param: str) -> float | None:
    """A percent knob as a fraction, or None when it is not set."""
    raw = _param(env, param)
    return float(raw) / 100.0 if raw else None


def get_sigmas() -> float:
    raw = _param('TPCC_DEVIATION_SIGMAS', 'tpcc-deviation-sigmas')
    return float(raw) if raw else DEFAULT_SIGMAS


def get_limit_bounds() -> tuple[float, float]:
    """Bounds the adaptive threshold is clamped to, as fractions."""
    low = _percent_param('TPCC_DEVIATION_MIN_LIMIT_PERCENT', 'tpcc-deviation-min-limit-percent')
    high = _percent_param('TPCC_DEVIATION_MAX_LIMIT_PERCENT', 'tpcc-deviation-max-limit-percent')
    return (
        DEFAULT_MIN_LIMIT_PERCENT / 100.0 if low is None else low,
        DEFAULT_MAX_LIMIT_PERCENT / 100.0 if high is None else high,
    )


def get_forced_limit() -> float | None:
    """An explicitly configured fixed threshold, which wins over the adaptive one."""
    forced = _percent_param('TPCC_DEVIATION_LIMIT_PERCENT', 'tpcc-deviation-limit-percent')
    if forced is None:
        # The name this knob had while the threshold was always fixed.
        forced = _percent_param('TPCC_MAX_DEVIATION_PERCENT', 'tpcc-max-deviation-percent')
    return forced


def get_fallback_limit() -> float:
    """Threshold used when the noise cannot be estimated."""
    raw = _percent_param('TPCC_DEVIATION_FALLBACK_PERCENT', 'tpcc-deviation-fallback-percent')
    return DEFAULT_FALLBACK_LIMIT_PERCENT / 100.0 if raw is None else raw


def key_measurement_specs() -> list[KeyMeasurementSpec]:
    low, high = get_limit_bounds()
    specs = []
    for metric in METRICS:
        better = 'greater' if metric.higher_is_better else 'less'
        specs.append(KeyMeasurementSpec(
            metric.improvement_signal,
            f'TPC-C {metric.name} improvement, %',
            f'Improvement of {metric.name} over the unixbench baseline of the previous runs, '
            f'in percent: positive means the current run is {better} than the baseline, '
            f'negative is a degradation',
            [
                (_KM_IMPROVED_COLOR, 0.0, None),
                # Above the floor and below the ceiling the verdict does not
                # depend on the noise of this particular configuration.
                (_KM_SAFE_COLOR, -100 * low, 0.0),
                (_KM_WATCH_COLOR, -100 * high, -100 * low),
                (_KM_FAILED_COLOR, None, -100 * high),
            ],
        ))
        specs.append(KeyMeasurementSpec(
            metric.ratio_signal,
            f'TPC-C {metric.name} improvement / limit',
            f'Improvement of {metric.name} in units of the allowed degradation: -1.0 is exactly '
            f'at the threshold, below -1.0 the check fails',
            [
                (_KM_IMPROVED_COLOR, 0.0, None),
                (_KM_SAFE_COLOR, -0.5, 0.0),
                (_KM_WATCH_COLOR, -1.0, -0.5),
                (_KM_FAILED_COLOR, None, -1.0),
            ],
        ))
        specs.append(KeyMeasurementSpec(
            metric.noise_signal,
            f'TPC-C {metric.name} noise, %',
            f'Robust relative spread of {metric.name} over the previous runs, in percent: '
            f'the noise the threshold is derived from',
            [(_KM_INFO_COLOR, None, None)],
        ))
        specs.append(KeyMeasurementSpec(
            metric.limit_signal,
            f'TPC-C {metric.name} limit, %',
            f'The lowest improvement of {metric.name} that still passes, in percent: '
            f'the run fails below it',
            [(_KM_INFO_COLOR, None, None)],
        ))
    return specs


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
    """Branches to take the history from: the current one, then older ones."""
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
    """Geometric mean of the best two thirds of the (run index, value) pairs."""
    best_count = max(1, len(values) * BASELINE_BEST_NUM // BASELINE_BEST_DEN)
    best = sorted(values, key=lambda item: item[1], reverse=higher_is_better)[:best_count]
    baseline = exp(sum(log(value) for _, value in best) / len(best))
    return baseline, sorted(index for index, _ in best)


def _best_of_bias(runs: int, best: int) -> float:
    """How optimistic a baseline of the best `best` of `runs` values is, in sigmas.

    Even a perfectly reproducible run is worse than the best of its predecessors,
    so this much of the threshold pays for the way the baseline is built. Blom's
    approximation of the expected normal order statistics is within 0.003 of the
    exact value for these window sizes. It does not vanish as the window grows:
    for the best two thirds it converges to 0.55.
    """
    if best >= runs or best <= 0:
        return 0.0
    return fmean(
        _NORMAL.inv_cdf((i - 0.375) / (runs + 0.25))
        for i in range(runs - best + 1, runs + 1)
    )


def _dispersion(values: list[float]) -> Dispersion:
    """Relative spread of the previous values, robustly and classically."""
    runs = len(values)
    if runs < MIN_SIGMA_RUNS:
        return Dispersion(runs, reason=f'{runs} runs, {MIN_SIGMA_RUNS} needed to estimate the noise')
    # In logarithms the spread is already relative, which is both what the
    # geometric mean of the baseline implies and the units the deviation is
    # measured in. Both estimates are corrected for the finite window: 1.4826
    # makes the median deviation comparable to a standard one, n/(n-0.8) and c4
    # remove the small-sample bias of the two estimators.
    logs = [log(value) for value in values]
    center = median(logs)
    mad = 1.4826 * runs / (runs - 0.8) * median(abs(value - center) for value in logs)
    sd = stdev(logs) / (1 - 1 / (4 * runs) - 7 / (32 * runs * runs))
    return Dispersion(runs, expm1(mad), expm1(sd))


def _threshold(dispersion: Dispersion, runs: int, best: int) -> Threshold:
    """Allowed degradation: the noise of the history, plus the bias of the baseline."""
    forced = get_forced_limit()
    if forced is not None:
        return Threshold(forced, False, dispersion.value, note='forced by the parameter')
    sigma = dispersion.value
    if sigma is None or sigma <= 0:
        reason = dispersion.reason or 'the noise estimate is zero'
        LOGGER.warning(f'TPC-C deviation threshold falls back to a fixed one: {reason}')
        return Threshold(get_fallback_limit(), False, None, note=reason)
    sigmas = get_sigmas()
    bias = _best_of_bias(runs, best)
    # One new run against a baseline of `best` runs: the variance of both adds up.
    raw = sigma * (bias + sigmas * sqrt(1 + 1 / best))
    low, high = get_limit_bounds()
    limit = min(max(raw, low), high)
    note = ''
    if limit > raw:
        note = f'raised to the lower bound from {100 * raw:.2f}%'
    elif limit < raw:
        note = f'lowered to the upper bound from {100 * raw:.2f}%'
    return Threshold(limit, True, sigma, bias, sigmas, raw, note)


def _collect_history(history: list, chain: list[str]) -> tuple[list, list]:
    """Runs to build the baseline and the noise estimate of, newest first.

    Takes up to SIGMA_RUNS runs along the branch chain; the first BASELINE_RUNS
    of them are the baseline window, the rest are used for the noise only.
    Returns those runs and their (branch, row, index) for the report.
    """
    by_branch: dict[str, list] = {}
    for row in history:
        by_branch.setdefault(row['git_branch'], []).append(row)
    runs = []
    displayed = []
    for branch in chain:
        if len(runs) >= SIGMA_RUNS:
            break
        for row in sorted(by_branch.get(branch, []), key=lambda row: row['timestamp'], reverse=True):
            if len(runs) >= SIGMA_RUNS:
                break
            displayed.append((branch, row, len(runs)))
            runs.append(row)
    return runs, displayed


def _check_metric(metric: Metric, current: float, runs: list) -> MetricCheck:
    values = []
    for index, row in enumerate(runs):
        value = row[metric.column]
        if value and float(value) > 0:
            values.append((index, float(value)))
    if len(values) < MIN_BASELINE_RUNS:
        return MetricCheck(metric, current, skip_reason=f'only {len(values)} usable previous values')
    base = values[:BASELINE_RUNS]
    baseline, used_runs = _unixbench(base, metric.higher_is_better)
    deviation = (baseline - current) / baseline if metric.higher_is_better else (current - baseline) / baseline
    dispersion = _dispersion([value for _, value in values])
    threshold = _threshold(dispersion, len(base), len(used_runs))
    check = MetricCheck(metric, current, baseline, deviation, threshold, dispersion, used_runs)
    LOGGER.info(
        f'TPC-C deviation {check.status}: {metric.name} current {current}, baseline {baseline} '
        f'of {len(used_runs)} best of {len(base)}, improvement {100 * check.improvement:+.2f}%, '
        f'allowed {100 * check.allowed:+.2f}% ({threshold.source}), noise {threshold.sigma}, '
        f'previous values {values}'
    )
    return check


def _measurements(checks: list[MetricCheck]) -> dict[str, float]:
    result = {}
    for check in checks:
        if check.deviation is None:
            continue
        result[check.metric.improvement_signal] = 100 * check.improvement
        if check.ratio is not None:
            result[check.metric.ratio_signal] = check.ratio
        if check.allowed is not None:
            result[check.metric.limit_signal] = 100 * check.allowed
        if check.threshold is not None and check.threshold.sigma is not None:
            result[check.metric.noise_signal] = 100 * check.threshold.sigma
    return result


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
        cells = [
            str(index + 1),
            'baseline' if index < BASELINE_RUNS else 'noise only',
            branch,
            _format_timestamp(row['timestamp']),
        ]
        for metric in METRICS:
            value = row[metric.column]
            cells.append(('' if value is None else value, _OK_COLOR if index in used.get(metric.column, ()) else None))
        rows.append(cells)
    return (
        '<h4>Previous runs</h4>'
        f'<div>The first {BASELINE_RUNS} runs are the baseline window, the rest estimate the noise only. '
        'Highlighted values are the ones the baseline is built of.</div>'
        + _table(['#', 'Role', 'Branch', 'Timestamp'] + [metric.name for metric in METRICS], rows)
    )


def _threshold_report(checks: list[MetricCheck]) -> str:
    rows = []
    for check in checks:
        threshold, dispersion = check.threshold, check.dispersion
        if threshold is None:
            rows.append([check.metric.name] + ['n/a'] * 7 + [check.skip_reason])
            continue
        rows.append([
            check.metric.name,
            'n/a' if dispersion is None else dispersion.runs,
            'n/a' if dispersion is None or dispersion.mad is None else f'{100 * dispersion.mad:.2f}%',
            'n/a' if dispersion is None or dispersion.sd is None else f'{100 * dispersion.sd:.2f}%',
            'n/a' if threshold.sigma is None else f'{100 * threshold.sigma:.2f}%',
            f'{threshold.bias:.2f}σ + {threshold.sigmas:.2f}σ·√(1+1/{len(check.used_runs)})' if threshold.adaptive else 'n/a',
            'n/a' if threshold.raw is None else f'{100 * threshold.raw:.2f}%',
            f'{100 * threshold.limit:.2f}%',
            f'{threshold.source}{", " + threshold.note if threshold.note else ""}',
        ])
    return (
        '<h4>Noise and threshold</h4>'
        '<div>The threshold pays for the optimism of a baseline built of the best runs, plus the '
        'sigmas of the noise a single new run may deviate by.</div>'
        + _table(
            ['Metric', 'Noise runs', 'Robust (MAD)', 'Classic (SD)', 'Noise used', 'Sigmas', 'Raw limit', 'Limit', 'Source'],
            rows,
        )
    )


def _result_report(checks: list[MetricCheck]) -> str:
    rows = []
    for check in checks:
        color = _FAILED_COLOR if check.failed else _OK_COLOR if not check.skip_reason else None
        rows.append([
            check.metric.name,
            f'{check.current:.2f}',
            'n/a' if check.baseline is None else f'{check.baseline:.2f}',
            check.skip_reason if check.improvement is None else f'{100 * check.improvement:+.2f}%',
            'n/a' if check.allowed is None else f'{100 * check.allowed:+.2f}%',
            'n/a' if check.ratio is None else f'{check.ratio:.2f}',
            (check.status, color),
        ])
    return (
        '<h4>Check result</h4>'
        '<div>Improvement is signed so that a greater value is a better run, whichever direction '
        'the metric itself grows in; a negative one is a degradation.</div>'
        + _table(['Metric', 'Current', 'Baseline (unixbench)', 'Improvement', 'Allowed', 'Of limit', 'Status'], rows)
    )


def _check(results: dict, run_type: str, run_ts: float, mode: CheckMode, report: list[str]) -> DeviationCheckResult:
    run_context = ResultsProcessor.get_tpcc_run_context()
    metrics = ResultsProcessor.get_tpcc_metrics(results)
    warehouses = int(metrics['warehouses'] or 0)
    if warehouses <= 0:
        raise ValueError(f'no warehouses count in the TPC-C results, got {metrics["warehouses"]!r}')
    cluster = run_context['cluster']
    branch = run_context['branch']
    forced = get_forced_limit()
    low, high = get_limit_bounds()
    report.append(_table(['Parameter', 'Value'], [
        ['Cluster', cluster],
        ['Branch', branch],
        ['Warehouses', warehouses],
        ['Run type', run_type],
        ['Mode', str(mode)],
        ['Baseline', f'unixbench (geometric mean) of the best {BASELINE_BEST_NUM}/{BASELINE_BEST_DEN} of {BASELINE_RUNS} previous runs'],
        ['Noise', f'robust relative spread of up to {SIGMA_RUNS} previous runs, at least {MIN_SIGMA_RUNS}'],
        [
            'Threshold',
            f'fixed {100 * forced:.2f}%, forced by the parameter' if forced is not None
            else f'adaptive, {get_sigmas():.2f} sigmas, bounded by {100 * low:.2f}% and {100 * high:.2f}%, '
                 f'{100 * get_fallback_limit():.2f}% when the noise cannot be estimated',
        ],
    ]))

    history = ResultsProcessor.get_tpcc_history(
        cluster=cluster,
        warehouses=warehouses,
        run_type=run_type,
        before_ts=run_ts,
        per_branch_limit=SIGMA_RUNS,
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
        checks.append(_check_metric(metric, current, runs))
    report.append(_previous_runs_report(displayed, checks))
    report.append(_threshold_report(checks))
    report.append(_result_report(checks))
    return DeviationCheckResult(
        errors=[check.error_message for check in checks if check.failed],
        summary=', '.join(check.report_text for check in checks),
        measurements=_measurements(checks),
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
            result = DeviationCheckResult(summary=f'{result.summary} (report only)', measurements=result.measurements)
        allure.attach('\n'.join(report), 'TPC-C deviation check', attachment_type=allure.attachment_type.HTML)
    return result
