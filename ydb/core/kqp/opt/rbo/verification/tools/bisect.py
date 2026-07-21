"""Sequentially localize a failing RBO plan to transformation prefixes."""

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
    capture,
    digest,
    required,
    run_command,
    validate_config,
    verify,
)


def localize(config: Config, runner: CommandRunner | None = None) -> dict[str, Any]:
    """Check final first; scan 1..N only after a supported final failure."""

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
            False,
            run,
        )

    status = final_verdict["status"]
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
            True,
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
