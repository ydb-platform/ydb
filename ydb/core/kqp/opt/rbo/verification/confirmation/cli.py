"""Command line for automatic confirmation of benchmark counterexamples."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .driver import confirm
from .model import Config, ConfirmationError


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Confirm every bounded benchmark counterexample on isolated YDB targets"
    )
    result.add_argument(
        "report",
        type=Path,
        help="version-four or version-five benchmark coverage report",
    )
    result.add_argument("--inspector", type=Path, required=True)
    result.add_argument("--solver", type=Path, required=True)
    result.add_argument("--replay", type=Path, required=True)
    result.add_argument("--ydb", type=Path, required=True)
    result.add_argument("--artifacts", type=Path, required=True)
    result.add_argument("--baseline-endpoint", required=True)
    result.add_argument("--baseline-database", required=True)
    result.add_argument("--candidate-endpoint", required=True)
    result.add_argument("--candidate-database", required=True)
    result.add_argument("--solver-timeout-ms", type=int, default=60_000)
    result.add_argument("--replay-timeout-seconds", type=int, default=300)
    result.add_argument("--replay-process-timeout-seconds", type=int, default=3_600)
    return result


def main(arguments: Sequence[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    config = Config(
        report=options.report,
        inspector=options.inspector,
        solver=options.solver,
        replay=options.replay,
        ydb=options.ydb,
        artifacts=options.artifacts,
        baseline_endpoint=options.baseline_endpoint,
        baseline_database=options.baseline_database,
        candidate_endpoint=options.candidate_endpoint,
        candidate_database=options.candidate_database,
        solver_timeout_ms=options.solver_timeout_ms,
        replay_timeout_seconds=options.replay_timeout_seconds,
        replay_process_timeout_seconds=options.replay_process_timeout_seconds,
    )
    try:
        result = confirm(config)
    except ConfirmationError as error:
        print(
            json.dumps({"status": "CONFIRMATION_ERROR", "reason": str(error)}, sort_keys=True),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return {
        "NO_COUNTEREXAMPLES": 0,
        "ALL_NOT_REPRODUCED": 0,
        "REAL_RESULT_DIVERGENCE": 1,
        "UNRESOLVED": 2,
    }[result["status"]]
