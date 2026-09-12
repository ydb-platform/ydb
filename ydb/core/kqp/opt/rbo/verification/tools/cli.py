"""Command line for transformation localization."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .bisect import Config, LocalizationError, localize


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Localize new-RBO changes using exhaustive midpoint-first comparisons"
    )
    result.add_argument("--verifier", type=Path, required=True)
    result.add_argument("--solver", type=Path, required=True)
    result.add_argument("--artifacts", type=Path, required=True)
    result.add_argument("--rows", type=int, default=2)
    result.add_argument("--timeout-ms", type=int, default=10_000)
    result.add_argument("--capture-timeout-seconds", type=int, default=300)
    result.add_argument("--max-events", type=int, default=10_000)
    result.add_argument("--strategy", choices=("divide-and-conquer", "sequential"),
                        default="divide-and-conquer")
    result.add_argument("--all-steps", action="store_true",
                        help="audit every step even when the final check has no counterexample")
    result.add_argument(
        "capture_command",
        nargs=argparse.REMAINDER,
        help="capture command after --; protocol arguments are appended",
    )
    return result


def main(arguments: Sequence[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    artifacts_preexisted = options.artifacts.exists()
    command = tuple(options.capture_command)
    if command[:1] == ("--",):
        command = command[1:]
    config = Config(
        capture_command=command,
        verifier=options.verifier,
        solver=options.solver,
        artifacts=options.artifacts,
        rows=options.rows,
        timeout_ms=options.timeout_ms,
        capture_timeout_seconds=options.capture_timeout_seconds,
        max_events=options.max_events,
        strategy=options.strategy,
        all_steps=options.all_steps,
    )
    try:
        result = localize(config)
    except LocalizationError as error:
        result = {"status": "LOCALIZATION_ERROR", "reason": str(error)}
        if not artifacts_preexisted and options.artifacts.is_dir():
            (options.artifacts / "result.json").write_text(
                json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
        print(json.dumps(result, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["status"] in {
        "FIRST_FAILING_PREFIX",
        "FAILING_PREFIX_INTERVAL",
        "FAILING_INTERVAL_TO_FINAL",
        "GLOBAL_SUFFIX_FAILURE",
        "LOCALIZED_FAILURES",
    }:
        return 1
    return 0 if result["status"] in {"FINAL_VERIFIED_BOUNDED", "STEPS_VERIFIED_BOUNDED"} else 2
