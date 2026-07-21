"""Command line for isolated real-YDB confirmation of an inspector trace."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Sequence

from ..rbo_verifier.ir import SnapshotError
from .model import InconclusiveReplay, ReplayError, load_json, load_snapshot, prepare_case
from .runner import Target, run_replay


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Replay one bounded new-RBO counterexample on isolated real YDB targets"
    )
    result.add_argument("before", type=Path, help="initial semantic snapshot")
    result.add_argument("after", type=Path, help="final StageGraph semantic snapshot")
    result.add_argument("trace", type=Path, help="kqp_rbo_inspect counterexample JSON")
    result.add_argument("query", type=Path, help="exact query text used to capture the snapshots")
    result.add_argument("--ydb", type=Path, required=True, help="explicit YDB CLI executable")
    result.add_argument("--baseline-endpoint", required=True)
    result.add_argument("--baseline-database", required=True)
    result.add_argument("--candidate-endpoint", required=True)
    result.add_argument("--candidate-database", required=True)
    result.add_argument("--timeout-seconds", type=int, default=300)
    return result


def main(arguments: Sequence[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    try:
        query_bytes = options.query.read_bytes()
        query = query_bytes.decode("utf-8", errors="strict")
        case = prepare_case(
            load_snapshot(options.before),
            load_snapshot(options.after),
            load_json(options.trace),
            query,
            hashlib.sha256(query_bytes).hexdigest(),
        )
        result = run_replay(
            case,
            Target("baseline", options.baseline_endpoint, options.baseline_database),
            Target("candidate", options.candidate_endpoint, options.candidate_database),
            options.ydb,
            options.timeout_seconds,
        )
    except InconclusiveReplay as error:
        print(
            json.dumps(
                {"status": "INCONCLUSIVE_NONDETERMINISM", "reason": str(error)},
                sort_keys=True,
            )
        )
        return 2
    except (OSError, UnicodeError, SnapshotError, ReplayError) as error:
        print(json.dumps({"status": "SETUP_ERROR", "reason": str(error)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["status"] == "REAL_RESULT_DIVERGENCE" else 0
