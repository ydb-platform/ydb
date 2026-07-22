"""Sequence strict witness inspection and real-YDB replay."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .input import load_report, prepare_inputs, sha256, validate_config, write_json
from .model import FORMAT, VERSION, Candidate, Config, ConfirmationError
from .protocol import (
    CommandRunner,
    decode_process,
    invoke,
    phase_result,
    run_command,
    validate_replay,
    validate_trace,
)


def confirm(
    config: Config,
    run: CommandRunner | None = None,
) -> dict[str, Any]:
    """Confirm every counterexample in one coverage report, without early exit."""

    validate_config(config)
    report_path, report_bytes, suite, row_bound, task_bound, candidates = load_report(
        config.report
    )
    try:
        config.artifacts.mkdir()
        root = config.artifacts.resolve(strict=True)
        (root / "coverage.json").write_bytes(report_bytes)
    except OSError as error:
        raise ConfirmationError(f"cannot create confirmation artifacts: {error}") from error

    runner = run or run_command
    results: list[dict[str, Any]] = []
    for candidate in candidates:
        directory = root / f"q{candidate.query_id:06d}"
        try:
            directory.mkdir()
        except OSError as error:
            results.append(_unresolved(
                _candidate_result(candidate),
                "artifact",
                f"cannot create candidate artifact directory: {error}",
            ))
            continue
        result = _confirm_candidate(
            config,
            root,
            report_path.parent,
            row_bound,
            task_bound,
            candidate,
            directory,
            runner,
        )
        try:
            write_json(directory / "result.json", result)
        except ConfirmationError as error:
            result = _unresolved(result, "artifact", str(error))
        results.append(result)

    counts: dict[str, int] = {}
    for result in results:
        classification = result["classification"]
        counts[classification] = counts.get(classification, 0) + 1
    if not results:
        status = "NO_COUNTEREXAMPLES"
    elif counts.get("UNRESOLVED", 0):
        status = "UNRESOLVED"
    elif counts.get("REAL_RESULT_DIVERGENCE", 0):
        status = "REAL_RESULT_DIVERGENCE"
    else:
        status = "ALL_NOT_REPRODUCED"

    result = {
        "format": FORMAT,
        "version": VERSION,
        "status": status,
        "source": {
            "coverage_report": "coverage.json",
            "coverage_report_sha256": sha256(report_bytes),
            "suite": suite,
            "row_bound": row_bound,
            "task_bound": task_bound,
        },
        "targets": {
            "baseline": {
                "endpoint": config.baseline_endpoint,
                "database": config.baseline_database,
            },
            "candidate": {
                "endpoint": config.candidate_endpoint,
                "database": config.candidate_database,
            },
        },
        "summary": {"total": len(results), **dict(sorted(counts.items()))},
        "candidates": results,
    }
    write_json(root / "result.json", result)
    return result


def _confirm_candidate(
    config: Config,
    root: Path,
    source_root: Path,
    row_bound: int,
    task_bound: int,
    candidate: Candidate,
    directory: Path,
    run: CommandRunner,
) -> dict[str, Any]:
    result = _candidate_result(candidate)

    try:
        inputs = prepare_inputs(root, source_root, candidate, directory)
        result["inputs"] = inputs.result
    except (ConfirmationError, OSError) as error:
        return _unresolved(result, "input", str(error))

    if "witness" not in inputs.verdict:
        return _unresolved(
            result,
            "inspector",
            "verifier counterexample has no extracted witness",
        )

    inspector_arguments = [
        str(config.inspector),
        "witness",
        str(inputs.initial),
        str(inputs.final),
        "--rows",
        str(row_bound),
        "--timeout-ms",
        str(config.solver_timeout_ms),
        "--solver",
        str(config.solver),
        "--query",
        str(inputs.query),
        "--verifier-verdict",
        str(inputs.verifier),
    ]
    inspector_timeout = max(30, 2 * (config.solver_timeout_ms // 1_000 + 5) + 30)
    try:
        process = invoke(
            inspector_arguments,
            inspector_timeout,
            directory / "inspector",
            run,
        )
        trace = decode_process(process, "inspector")
        result["inspector"] = phase_result(
            root,
            directory / "inspector",
            process,
            trace,
        )
        comparison, columns = validate_trace(
            process,
            trace,
            inputs.verdict["witness"],
            row_bound,
            task_bound,
            inputs.query_sha256,
        )
    except ConfirmationError as error:
        return _unresolved(result, "inspector", str(error))

    replay_arguments = [
        str(config.replay),
        str(inputs.initial),
        str(inputs.final),
        str(directory / "inspector.stdout"),
        str(inputs.query),
        "--ydb",
        str(config.ydb),
        "--baseline-endpoint",
        config.baseline_endpoint,
        "--baseline-database",
        config.baseline_database,
        "--candidate-endpoint",
        config.candidate_endpoint,
        "--candidate-database",
        config.candidate_database,
        "--timeout-seconds",
        str(config.replay_timeout_seconds),
    ]
    try:
        process = invoke(
            replay_arguments,
            config.replay_process_timeout_seconds,
            directory / "replay",
            run,
        )
        replay = decode_process(process, "replay")
        result["replay"] = phase_result(
            root,
            directory / "replay",
            process,
            replay,
        )
        classification = validate_replay(
            process,
            replay,
            row_bound,
            comparison,
            columns,
            config.baseline_database,
            config.candidate_database,
        )
    except ConfirmationError as error:
        return _unresolved(result, "replay", str(error))

    result["classification"] = classification
    return result


def _candidate_result(candidate: Candidate) -> dict[str, Any]:
    return {
        "query_id": candidate.query_id,
        "source": candidate.source,
        "classification": "UNRESOLVED",
    }


def _unresolved(result: dict[str, Any], phase: str, reason: str) -> dict[str, Any]:
    result["classification"] = "UNRESOLVED"
    result["unresolved_phase"] = phase
    result["reason"] = reason
    return result
