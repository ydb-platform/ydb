"""Adaptive offered-load selection for local YDB benchmarks."""

from dataclasses import dataclass

from ydb.tools.ydb_bench.lib.common import BenchmarkError


@dataclass(frozen=True)
class LoadSearchResult:
    attempts: tuple
    selected_load: object
    stop_reason: str
    outcome: str
    passing_load: object = None
    failing_load: object = None


def _next_geometric(current, maximum, multiplier):
    candidate = max(current + 1, int(round(current * multiplier)))
    return min(maximum, candidate)


def _target_cpu(metrics, target_role):
    if target_role == "static":
        return metrics["static_cpu_mean"]
    if target_role == "dynamic":
        return metrics["dynamic_cpu_mean"]
    return metrics["host_cpu_mean"]


def _with_decision(metrics, load, passed, reason):
    return {**metrics, "load": load, "passed": bool(passed), "decision": reason}


def _best_throughput(attempts):
    candidates = [item for item in attempts if item["passed"]]
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item["throughput"], -item["load"]))["load"]


def _throughput_gain(lower, upper):
    if lower > 0:
        return 100.0 * (upper - lower) / lower
    if upper > lower:
        return None
    return 0.0


def _append_attempt(attempts, record, on_attempt):
    attempts.append(record)
    if on_attempt is not None:
        on_attempt(record)
    return record


def _run_points(config, measure, on_attempt):
    attempts = []
    allow_errors = config.get("allow_errors", False)
    for value in config["values"]:
        metrics = measure(value)
        errors = metrics["errors"]
        passed = allow_errors or not errors
        if errors:
            reason = "{} workload errors {}".format(errors, "allowed" if allow_errors else "reported")
        else:
            reason = "configured point"
        _append_attempt(
            attempts,
            _with_decision(metrics, value, passed, reason),
            on_attempt,
        )
    selected = _best_throughput(attempts)
    return LoadSearchResult(
        tuple(attempts),
        selected,
        "configured points completed",
        "best-observed" if selected is not None else "no-feasible-point",
        passing_load=selected,
    )


def _run_throughput(config, measure, on_attempt):
    search = config["search"]
    objective = config["objective"]
    attempts = []
    measured = {}
    allow_errors = config.get("allow_errors", False)
    failing_load = None
    plateau = 0
    plateau_confirmed = False

    def sample(load, reason, baseline=None):
        nonlocal failing_load
        if load in measured:
            return measured[load]
        metrics = measure(load)
        saturated = _target_cpu(metrics, objective["target_role"]) >= objective["cpu_saturation_percent"]
        passed = allow_errors or not metrics["errors"]
        gain = None if baseline is None else _throughput_gain(baseline["throughput"], metrics["throughput"])
        if not passed:
            decision = "workload reported errors"
            failing_load = load if failing_load is None else min(failing_load, load)
        else:
            decision = reason
            if baseline is not None and gain is None:
                decision += "; throughput increased from zero baseline"
            elif gain is not None:
                decision += "; throughput gain {:.3f}%".format(gain)
            if metrics["errors"]:
                decision += "; {} workload errors allowed".format(metrics["errors"])
        record = _with_decision(
            {**metrics, "throughput_gain_percent": gain, "target_cpu_saturated": saturated},
            load,
            passed,
            decision,
        )
        measured[load] = record
        return _append_attempt(attempts, record, on_attempt)

    start = search["start"]
    maximum = search["maximum"]
    first = sample(start, "minimum ternary-search load")
    if not first["passed"]:
        return LoadSearchResult(
            tuple(attempts),
            None,
            "workload errors at minimum load {}".format(start),
            "no-feasible-point",
            failing_load=start,
        )

    resolution = max(1, int(round((maximum - start) * search["resolution_percent"] / 100.0)))
    low = start
    high = maximum
    while high - low > resolution:
        third = max(1, (high - low) // 3)
        lower_load = low + third
        upper_load = high - third
        if lower_load >= upper_load:
            break
        lower = sample(lower_load, "lower ternary probe")
        if not lower["passed"]:
            plateau = 0
            high = lower_load - 1
            continue
        upper = sample(upper_load, "upper ternary probe", baseline=lower)
        if not upper["passed"]:
            plateau = 0
            high = upper_load - 1
            continue
        gain = _throughput_gain(lower["throughput"], upper["throughput"])
        saturated_plateau = (
            gain is not None and gain < objective["plateau_gain_percent"] and upper["target_cpu_saturated"]
        )
        if saturated_plateau:
            plateau += 1
            plateau_confirmed = plateau_confirmed or plateau >= objective["plateau_points"]
            high = upper_load - 1
        elif upper["throughput"] <= lower["throughput"]:
            plateau = 0
            high = upper_load - 1
        else:
            plateau = 0
            low = lower_load + 1

    for load in sorted({low, (low + high) // 2, high}):
        sample(load, "final ternary candidate")
    selected = _best_throughput(attempts)
    if selected is None:
        outcome = "no-feasible-point"
        stop_reason = "ternary search found no feasible load"
    elif plateau_confirmed:
        outcome = "plateau-found"
        stop_reason = "throughput plateau confirmed by ternary search at saturated {} CPU".format(
            objective["target_role"]
        )
    elif failing_load is not None:
        outcome = "bounded-by-errors"
        stop_reason = "workload errors bounded the ternary search below {}".format(failing_load)
    elif selected == maximum and measured.get(maximum, {}).get("passed"):
        outcome = "lower-bound"
        stop_reason = "maximum configured load {} remains the best observed point".format(maximum)
    else:
        outcome = "best-observed"
        stop_reason = "ternary interval [{}, {}] reached resolution {}".format(low, high, resolution)
    return LoadSearchResult(
        tuple(attempts),
        selected,
        stop_reason,
        outcome,
        passing_load=selected,
        failing_load=failing_load,
    )


def _latency_passes(config, load, metrics):
    objective = config["objective"]
    latency = metrics[objective["percentile"] + "_ms"]
    if latency > objective["max_ms"]:
        return False, "{} latency {:.3f} ms exceeds {:.3f} ms".format(
            objective["percentile"], latency, objective["max_ms"]
        )
    if not config.get("allow_errors", False) and metrics["errors"] > objective["max_errors"]:
        return False, "errors {} exceed {}".format(metrics["errors"], objective["max_errors"])
    if config["parameter"] == "rate":
        ratio = metrics["throughput"] / load
        if ratio < objective["min_achieved_rate_ratio"]:
            return False, "achieved rate ratio {:.4f} is below {:.4f}".format(
                ratio, objective["min_achieved_rate_ratio"]
            )
    reason = "latency SLO satisfied"
    if metrics["errors"]:
        reason += "; {} workload errors allowed".format(metrics["errors"])
    return True, reason


def _run_latency(config, measure, on_attempt):
    search = config["search"]
    attempts = []
    measured = {}

    def sample(load):
        if load in measured:
            return measured[load]
        metrics = measure(load)
        passed, reason = _latency_passes(config, load, metrics)
        record = _with_decision(metrics, load, passed, reason)
        _append_attempt(attempts, record, on_attempt)
        measured[load] = record
        return record

    current = search["start"]
    last_pass = None
    first_fail = None
    while True:
        record = sample(current)
        if record["passed"]:
            last_pass = current
            if current >= search["maximum"]:
                return LoadSearchResult(
                    tuple(attempts),
                    current,
                    "maximum load satisfies latency SLO",
                    "lower-bound",
                    passing_load=current,
                )
            current = _next_geometric(current, search["maximum"], search["multiplier"])
        else:
            first_fail = current
            break

    if last_pass is None:
        return LoadSearchResult(
            tuple(attempts),
            None,
            "minimum load {} does not satisfy latency SLO".format(first_fail),
            "no-feasible-point",
            failing_load=first_fail,
        )

    low = last_pass
    high = first_fail
    resolution = max(1, int(round(max(high, 1) * search["resolution_percent"] / 100.0)))
    while high - low > resolution:
        candidate = max(1, (low + high) // 2)
        if candidate in measured:
            break
        record = sample(candidate)
        if record["passed"]:
            low = candidate
        else:
            high = candidate

    selected = low or None
    reason = "latency SLO bracketed between {} and {}".format(low, high)
    return LoadSearchResult(
        tuple(attempts),
        selected,
        reason,
        "boundary-found" if selected is not None else "no-feasible-point",
        passing_load=selected,
        failing_load=high,
    )


def search_load(config, measure, on_attempt=None):
    """Run the configured controller using ``measure(load) -> metrics``."""
    if "values" in config:
        return _run_points(config, measure, on_attempt)
    objective_type = config["objective"]["type"]
    if objective_type == "maximize-throughput":
        return _run_throughput(config, measure, on_attempt)
    if objective_type == "latency-slo":
        return _run_latency(config, measure, on_attempt)
    raise BenchmarkError("unsupported load objective: {}".format(objective_type))
