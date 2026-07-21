"""Small subprocess boundary for replaying one validated case in two YDBs."""

from __future__ import annotations

import json
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from .materialize import (
    TargetBundle,
    target_bundle,
    validate_database_path,
)
from .model import ReplayCase, ReplayError
from .observation import compare_results, optimizer_mode, parse_result


REPLAY_FORMAT = "ydb-rbo-real-replay"
REPLAY_VERSION = 1


@dataclass(frozen=True, slots=True)
class Target:
    name: str
    endpoint: str
    database: str

    def validate(self) -> None:
        if self.name not in {"baseline", "candidate"}:
            raise ReplayError(f"invalid replay target role {self.name!r}")
        if not self.endpoint or any(ord(character) < 32 for character in self.endpoint):
            raise ReplayError(f"{self.name} endpoint is empty or contains control characters")
        validate_database_path(self.database)


Invoke = Callable[..., subprocess.CompletedProcess[str]]


class YdbCli:
    def __init__(
        self,
        executable: str | Path,
        timeout_seconds: int,
        invoke: Invoke = subprocess.run,
    ) -> None:
        self.executable = str(executable)
        self.timeout_seconds = timeout_seconds
        self.invoke = invoke

    def command(self, target: Target, arguments: list[str], stdin: str | None = None) -> str:
        command = [
            self.executable,
            "--endpoint",
            target.endpoint,
            "--database",
            target.database,
            "--no-discovery",
            *arguments,
        ]
        try:
            process = self.invoke(
                command,
                input=stdin,
                stdin=subprocess.DEVNULL if stdin is None else None,
                text=True,
                capture_output=True,
                timeout=self.timeout_seconds,
                check=False,
            )
        except (OSError, UnicodeError) as error:
            raise ReplayError(f"cannot execute YDB CLI {self.executable!r}: {error}") from error
        except subprocess.TimeoutExpired as error:
            raise ReplayError(
                f"{target.name} command exceeded {self.timeout_seconds} seconds: "
                + " ".join(arguments[:3])
            ) from error
        if process.returncode != 0:
            diagnostic = process.stderr.strip() or process.stdout.strip()
            if len(diagnostic) > 4_000:
                diagnostic = diagnostic[:4_000] + "..."
            raise ReplayError(
                f"{target.name} YDB command failed with code {process.returncode}: "
                f"{' '.join(arguments[:3])}; {diagnostic}"
            )
        return process.stdout

    def describe_database(self, target: Target) -> None:
        self.command(target, ["scheme", "describe", target.database])

    def create(self, target: Target, bundle: TargetBundle) -> None:
        self.command(target, ["scheme", "mkdir", bundle.prefix])
        for ddl in bundle.ddls:
            self.command(target, ["sql", "--file", "-"], ddl)

    def import_rows(self, target: Target, bundle: TargetBundle) -> None:
        for path, rows in zip(bundle.paths, bundle.imports):
            if not rows:
                continue
            self.command(
                target,
                [
                    "import",
                    "file",
                    "json",
                    "--path",
                    path,
                    "--input-binary-strings",
                    "base64",
                    "--threads",
                    "1",
                    "--max-in-flight",
                    "1",
                ],
                rows,
            )

    def explain(self, target: Target, query: str) -> Any:
        output = self.command(
            target,
            ["sql", "--explain", "--file", "-", "--format", "json-unicode"],
            query,
        )
        try:
            return json.loads(output)
        except json.JSONDecodeError as error:
            raise ReplayError(f"{target.name} explain output is not one JSON document") from error

    def execute(self, target: Target, bundle: TargetBundle, output: tuple[str, ...]):
        text = self.command(
            target,
            ["sql", "--file", "-", "--format", "json-base64-array"],
            bundle.query,
        )
        return parse_result(text, output)


def run_replay(
    case: ReplayCase,
    baseline: Target,
    candidate: Target,
    ydb: str | Path,
    timeout_seconds: int,
    *,
    invoke: Invoke = subprocess.run,
    namespace: str | None = None,
) -> Mapping[str, Any]:
    baseline.validate()
    candidate.validate()
    if (baseline.endpoint, baseline.database) == (candidate.endpoint, candidate.database):
        raise ReplayError("baseline and candidate must be different YDB targets")
    if type(timeout_seconds) is not int or timeout_seconds <= 0:
        raise ReplayError("command timeout must be a positive integer")
    namespace = namespace or f"_rbo_replay_{uuid.uuid4().hex}"
    baseline_bundle = target_bundle(case, baseline.database, namespace)
    candidate_bundle = target_bundle(case, candidate.database, namespace)
    cli = YdbCli(ydb, timeout_seconds, invoke)

    # All pure validation above precedes the first connection or mutation.
    try:
        cli.describe_database(baseline)
        cli.describe_database(candidate)
        cli.create(baseline, baseline_bundle)
        cli.create(candidate, candidate_bundle)
        cli.import_rows(baseline, baseline_bundle)
        cli.import_rows(candidate, candidate_bundle)

        baseline_plan = cli.explain(baseline, baseline_bundle.query)
        candidate_plan = cli.explain(candidate, candidate_bundle.query)
        baseline_mode, baseline_stats = optimizer_mode(baseline_plan)
        candidate_mode, candidate_stats = optimizer_mode(candidate_plan)
        if baseline_mode != "LEGACY_RBO":
            raise ReplayError("baseline explain was produced by the new RBO")
        if candidate_mode != "NEW_RBO":
            raise ReplayError("candidate explain was not produced by the new RBO")

        baseline_rows = cli.execute(baseline, baseline_bundle, case.output)
        candidate_rows = cli.execute(candidate, candidate_bundle, case.output)
        equal, difference = compare_results(baseline_rows, candidate_rows, case.ordered)
    except Exception as error:
        detail = (
            str(error)
            if isinstance(error, ReplayError)
            else f"{type(error).__name__}: {error}"
        )
        raise ReplayError(
            f"{detail}; generated objects are retained if creation began: "
            f"{baseline_bundle.prefix}, {candidate_bundle.prefix}"
        ) from error
    return {
        "format": REPLAY_FORMAT,
        "version": REPLAY_VERSION,
        "status": "NOT_REPRODUCED" if equal else "REAL_RESULT_DIVERGENCE",
        "comparison": "sequence" if case.ordered else "bag",
        "row_bound": case.row_bound,
        "symbolic_string_cells": case.symbolic_string_cells,
        "trace_plan_reproduced": False,
        "namespaces_retained": True,
        "baseline": {
            "database": baseline.database,
            "namespace": baseline_bundle.prefix,
            "optimizer": baseline_mode,
            "optimizer_stats": baseline_stats,
            "rows": baseline_rows,
        },
        "candidate": {
            "database": candidate.database,
            "namespace": candidate_bundle.prefix,
            "optimizer": candidate_mode,
            "optimizer_stats": candidate_stats,
            "rows": candidate_rows,
        },
        "difference": difference,
    }
