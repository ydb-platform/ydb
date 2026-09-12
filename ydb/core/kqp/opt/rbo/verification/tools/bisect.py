"""Localize captured transformations without assuming monotone equivalence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .protocol import (
    Capture,
    CommandRunner,
    Config,
    Event,
    LocalizationError,
    TASK_BOUND,
    capture,
    digest,
    required,
    run_command,
    validate_config,
    verify,
)


def localize(config: Config, runner: CommandRunner | None = None) -> dict[str, Any]:
    """Check final first; investigate a failure (or an explicit all-step audit)."""

    validate_config(config)
    config.artifacts.mkdir(parents=True)
    run = runner or run_command
    completion_dir = config.artifacts / "completion"
    completion_dir.mkdir()
    completion = capture(
        config,
        config.max_events + 1,
        completion_dir,
        run,
    )
    if completion.status in {"PREFIX_CAPTURED", "PREFIX_UNSUPPORTED"}:
        raise LocalizationError(
            "optimizer reached the event limit; increase --max-events"
        )

    events = completion.events
    initial_digest = digest(completion.initial)
    if completion.status == "FINAL_UNSUPPORTED":
        final_verdict = {
            "status": "UNSUPPORTED",
            "reason": completion.unsupported_reason,
            "source": "SNAPSHOT_EXPORT",
        }
    else:
        final_verdict = verify(
            config,
            completion.initial,
            required(completion.final),
            completion_dir,
            None,
            run,
        )

    status = final_verdict["status"]
    if status not in {"VERIFIED_BOUNDED", "UNSUPPORTED", "UNKNOWN", "COUNTEREXAMPLE", "SCHEMA_MISMATCH"}:
        raise LocalizationError(f"cannot localize final verifier status {status}")
    if config.strategy == "divide-and-conquer" and (
        config.all_steps or status in {"COUNTEREXAMPLE", "SCHEMA_MISMATCH"}
    ):
        return _divide_and_conquer(config, completion, final_verdict, run)
    if status in {"VERIFIED_BOUNDED", "UNSUPPORTED", "UNKNOWN"}:
        outcome = {
            "VERIFIED_BOUNDED": "FINAL_VERIFIED_BOUNDED",
            "UNSUPPORTED": "FINAL_UNSUPPORTED",
            "UNKNOWN": "FINAL_INCONCLUSIVE",
        }[status]
        return _finish(
            config.artifacts,
            outcome,
            events,
            final_verdict,
            checked=0,
            scope="INITIAL_TO_FINAL",
        )
    if status not in {"COUNTEREXAMPLE", "SCHEMA_MISMATCH"}:
        raise LocalizationError(f"cannot localize final verifier status {status}")

    gaps: list[dict[str, Any]] = []
    last_verified = 0
    for ordinal, event in enumerate(events, 1):
        directory = config.artifacts / f"prefix-{ordinal:06d}"
        directory.mkdir()
        prefix = capture(config, ordinal, directory, run)
        _check_prefix(prefix, events[:ordinal], initial_digest)
        if prefix.status == "PREFIX_UNSUPPORTED":
            gaps.append(
                _gap(event, "UNSUPPORTED", prefix.unsupported_reason, "SNAPSHOT_EXPORT")
            )
            continue
        verdict = verify(
            config,
            prefix.initial,
            required(prefix.prefix),
            directory,
            "prefix",
            run,
        )
        prefix_status = verdict["status"]
        if prefix_status == "VERIFIED_BOUNDED":
            last_verified = ordinal
            continue
        if prefix_status in {"UNSUPPORTED", "UNKNOWN"}:
            gaps.append(
                _gap(event, prefix_status, verdict.get("reason"), "VERIFIER")
            )
            continue
        if prefix_status not in {"COUNTEREXAMPLE", "SCHEMA_MISMATCH"}:
            raise LocalizationError(f"cannot localize verifier status {prefix_status}")

        common = dict(
            checked=ordinal,
            scope="OPTIMIZER_TRANSFORMATION_PREFIX",
            last_verified=last_verified,
            event=event,
            prefix_verdict=verdict,
            gaps=gaps,
        )
        if ordinal == last_verified + 1:
            return _finish(
                config.artifacts,
                "FIRST_FAILING_PREFIX",
                events,
                final_verdict,
                **common,
            )
        return _finish(
            config.artifacts,
            "FAILING_PREFIX_INTERVAL",
            events,
            final_verdict,
            interval={
                "first_possible_event": last_verified + 1,
                "observed_failing_event": ordinal,
            },
            **common,
        )

    if last_verified == len(events):
        return _finish(
            config.artifacts,
            "GLOBAL_SUFFIX_FAILURE",
            events,
            final_verdict,
            checked=len(events),
            scope="INITIAL_TO_FINAL",
            last_verified=last_verified,
            localization_region="GLOBAL_SUFFIX_AFTER_TRANSFORMATIONS",
            gaps=gaps,
        )
    return _finish(
        config.artifacts,
        "FAILING_INTERVAL_TO_FINAL",
        events,
        final_verdict,
        checked=len(events),
        scope="INITIAL_TO_FINAL",
        last_verified=last_verified,
        gaps=gaps,
        interval={
            "first_possible_event": last_verified + 1,
            "last_dynamic_event": len(events),
            "observed_failing_boundary": "FINAL",
        },
    )


def _divide_and_conquer(
    config: Config,
    completion: Capture,
    final_verdict: Mapping[str, Any],
    run: CommandRunner,
) -> dict[str, Any]:
    """Check both halves before descending; even equivalent intervals are split.

    Only adjacent comparisons attribute events. N events plus the final suffix
    require N+1 adjacent checks; binary scheduling does not make this logarithmic.
    """
    events = completion.events
    final = len(events) + 1
    captures = {0: completion, final: completion}
    directories = {0: config.artifacts / "completion", final: config.artifacts / "completion"}
    snapshots = {0: completion.initial, final: completion.final}
    snapshot_hashes = {ordinal: digest(path) for ordinal, path in snapshots.items() if path is not None}
    comparisons: dict[tuple[int, int], dict[str, Any]] = {}
    initial_digest = digest(completion.initial)

    def artifacts(directory: Path, names: Mapping[str, str]) -> dict[str, Any]:
        return {
            role: {"path": str((directory / name).relative_to(config.artifacts)),
                   "sha256": digest(directory / name)}
            for role, name in names.items() if (directory / name).is_file()
        }

    def boundary(ordinal: int) -> Path | None:
        if ordinal not in captures:
            directory = config.artifacts / f"prefix-{ordinal:06d}"
            directory.mkdir()
            captured = capture(config, ordinal, directory, run)
            _check_prefix(captured, events[:ordinal], initial_digest)
            captures[ordinal] = captured
            directories[ordinal] = directory
            snapshots[ordinal] = captured.prefix
            if captured.prefix is not None:
                snapshot_hashes[ordinal] = digest(captured.prefix)
        if snapshots[ordinal] is not None and digest(snapshots[ordinal]) != snapshot_hashes[ordinal]:
            raise LocalizationError(f"cached snapshot changed at boundary {ordinal}")
        return snapshots[ordinal]

    def compare(left: int, right: int) -> dict[str, Any]:
        if (left, right) in comparisons:
            return comparisons[left, right]
        before, after = boundary(left), boundary(right)
        directory = config.artifacts / f"compare-{left:06d}-{right:06d}"
        if (left, right) == (0, final):
            directory = config.artifacts / "completion"
            verdict = final_verdict
        elif before is None or after is None:
            verdict = {"status": "UNSUPPORTED", "source": "SNAPSHOT_EXPORT",
                       "reason": "; ".join(f"boundary {ordinal}: {captures[ordinal].unsupported_reason}"
                                           for ordinal in (left, right) if snapshots[ordinal] is None)}
        else:
            directory.mkdir()
            verdict = verify(config, before, after, directory, "pair", run,
                             observation=required(boundary(0)))
        if verdict["status"] not in {"VERIFIED_BOUNDED", "COUNTEREXAMPLE", "SCHEMA_MISMATCH", "UNKNOWN", "UNSUPPORTED"}:
            raise LocalizationError(f"cannot localize verifier status {verdict['status']}")
        result = {
            "id": f"{left}:{right}", "before": left, "after": right,
            "adjacent": right == left + 1, "verifier": dict(verdict),
            "artifacts": artifacts(directory, {"verdict": "verdict.json", "formula": "obligation.smt2",
                                                "command": "verifier.command.json",
                                                "stdout": "verifier.stdout", "stderr": "verifier.stderr"}),
        }
        comparisons[left, right] = result
        return result

    def descend(left: int, right: int) -> None:
        if right - left <= 1:
            return
        middle = (left + right) // 2
        compare(left, middle)
        compare(middle, right)
        descend(left, middle)
        descend(middle, right)

    compare(0, final)
    descend(0, final)
    findings, gaps = [], []
    for comparison in comparisons.values():
        left, right = comparison["before"], comparison["after"]
        status = comparison["verifier"]["status"]
        if status in {"UNKNOWN", "UNSUPPORTED"}:
            gaps.append({"comparison": comparison["id"], "before": left, "after": right,
                         "adjacent": comparison["adjacent"], "status": status})
        elif comparison["adjacent"] and status in {"COUNTEREXAMPLE", "SCHEMA_MISMATCH"}:
            finding = {"comparison": comparison["id"], "status": status}
            if right <= len(events):
                finding["event"] = events[right - 1].to_json()
            else:
                finding["region"] = "GLOBAL_SUFFIX_AFTER_TRANSFORMATIONS"
            findings.append(finding)
    boundaries = []
    for ordinal in sorted(captures):
        snapshot = boundary(ordinal)
        directory = directories[ordinal]
        names = {"capture": "capture.json", "command": "capture.command.json",
                 "stdout": "capture.stdout", "stderr": "capture.stderr"}
        if snapshot is not None:
            names["snapshot"] = str(snapshot.relative_to(directory))
        boundaries.append({"ordinal": ordinal, "kind": "INITIAL" if ordinal == 0 else "FINAL" if ordinal == final else "PREFIX",
                           "status": "CAPTURED" if snapshot is not None else "UNSUPPORTED",
                           "artifacts": artifacts(directory, names)})
    # A timeout on a coarse interval does not leave an adjacent event unchecked.
    incomplete = any(gap["adjacent"] for gap in gaps)
    observation_kinds = {comparison["verifier"]["observation_kind"] for comparison in comparisons.values()
                         if comparison["verifier"].get("comparison_scope") == "OPTIMIZER_TRANSFORMATION_PAIR"
                         and "observation_kind" in comparison["verifier"]}
    if len(observation_kinds) > 1:
        raise LocalizationError("verifier observation kind changed across comparisons")
    result = {
        "format": "ydb-rbo-transformation-localization", "version": 1,
        "status": "LOCALIZED_FAILURES" if findings else "LOCALIZATION_INCOMPLETE" if incomplete else "STEPS_VERIFIED_BOUNDED",
        "strategy": "divide-and-conquer", "all_steps": config.all_steps,
        "row_bound": config.rows, "task_bound": TASK_BOUND,
        "timeout_ms": config.timeout_ms,
        "comparison_scope": "OPTIMIZER_TRANSFORMATION_PAIR",
        "observation_boundary": 0,
        "observation_snapshot_sha256": snapshot_hashes[0],
        "observation_kind": next(iter(observation_kinds), None),
        "completeness": "GAPS" if incomplete else "COMPLETE",
        "events_total": len(events), "events_attempted": len(events),
        "events_checked": sum(comparison["adjacent"] and comparison["after"] <= len(events)
                              and "verdict" in comparison["artifacts"] for comparison in comparisons.values()),
        "final_verifier": dict(final_verdict), "artifacts": str(config.artifacts),
        "events": [event.to_json() for event in events], "boundaries": boundaries,
        "comparisons": list(comparisons.values()), "findings": findings, "gaps": gaps,
    }
    (config.artifacts / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def _check_prefix(
    prefix: Capture,
    expected: tuple[Event, ...],
    initial_digest: str,
) -> None:
    ordinal = len(expected)
    if prefix.status not in {"PREFIX_CAPTURED", "PREFIX_UNSUPPORTED"}:
        raise LocalizationError(f"optimizer ended before expected event {ordinal}")
    if prefix.events != expected:
        raise LocalizationError(
            f"committed transformation sequence changed while capturing ordinal {ordinal}"
        )
    if digest(prefix.initial) != initial_digest:
        raise LocalizationError(f"initial snapshot changed while capturing ordinal {ordinal}")


def _gap(
    event: Event,
    status: str,
    reason: Any,
    source: str | None = None,
) -> dict[str, Any]:
    result = {**event.to_json(), "status": status}
    if reason is not None:
        result["reason"] = reason
    if source is not None:
        result["source"] = source
    return result


def _finish(
    artifacts: Path,
    status: str,
    events: tuple[Event, ...],
    final_verdict: Mapping[str, Any],
    *,
    checked: int,
    scope: str,
    last_verified: int | None = None,
    event: Event | None = None,
    prefix_verdict: Mapping[str, Any] | None = None,
    gaps: Sequence[Mapping[str, Any]] = (),
    interval: Mapping[str, Any] | None = None,
    localization_region: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": status,
        "comparison_scope": scope,
        "events_total": len(events),
        "events_checked": checked,
        "final_verifier": dict(final_verdict),
        "artifacts": str(artifacts),
    }
    optional = {
        "last_verified_ordinal": last_verified,
        "observed_failing_event": event.to_json() if event else None,
        "prefix_verifier": dict(prefix_verdict) if prefix_verdict else None,
        "prefix_gaps": list(gaps) if gaps else None,
        "failing_interval": dict(interval) if interval else None,
        "localization_region": localization_region,
    }
    result.update((key, value) for key, value in optional.items() if value is not None)
    (artifacts / "result.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result
