import argparse
import math
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ydb.tools.ydb_bench.lib.actors_core import RunConfiguration, run_actors_core
from ydb.tools.ydb_bench.lib.common import BenchmarkError, BenchmarkInterrupted, extract_executable
from ydb.tools.ydb_bench.lib.topology import AFFINITY_MODES


SCENARIO_NAME = "actors-core"
RESOURCE_NAME = "actors_core_ut_fat"


@dataclass(frozen=True)
class Profile:
    name: str
    description: str
    threads: tuple
    actor_pairs: tuple
    inflights: tuple
    duration_seconds: int
    repetitions: int


PROFILES = {
    "smoke": Profile(
        name="smoke",
        description="one short actor-system measurement",
        threads=(2,),
        actor_pairs=(32,),
        inflights=(1,),
        duration_seconds=1,
        repetitions=1,
    ),
    "baseline": Profile(
        name="baseline",
        description="fixed cross-platform comparison at 1, 2, 4, 8, and 16 threads",
        threads=(1, 2, 4, 8, 16),
        actor_pairs=(512,),
        inflights=(1,),
        duration_seconds=2,
        repetitions=3,
    ),
}


def _scale_profile():
    cpu_count = max(1, os.cpu_count() or 1)
    threads = []
    value = 1
    while value < cpu_count:
        threads.append(value)
        value *= 2
    threads.append(cpu_count)
    return Profile(
        name="scale",
        description="power-of-two thread sweep up to the current CPU count",
        threads=tuple(threads),
        actor_pairs=(512,),
        inflights=(1,),
        duration_seconds=2,
        repetitions=3,
    )


def _profile(name):
    if name == "scale":
        return _scale_profile()
    return PROFILES[name]


def _positive_integer(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _positive_float(value):
    parsed = float(value)
    if parsed <= 0 or not math.isfinite(parsed):
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return parsed


def _positive_integer_list(value):
    try:
        parsed = tuple(_positive_integer(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a comma-separated list of positive integers") from error
    if not parsed:
        raise argparse.ArgumentTypeError("must not be empty")
    return parsed


def _affinity_modes(value):
    if value == "all":
        return AFFINITY_MODES
    modes = tuple(part.strip() for part in value.split(",") if part.strip())
    invalid = sorted(set(modes) - set(AFFINITY_MODES))
    if not modes or invalid:
        raise argparse.ArgumentTypeError(
            "must be 'all' or a comma-separated subset of {}".format(", ".join(AFFINITY_MODES))
        )
    return modes


def _default_output_directory():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("ydb-bench-results") / "{}-{}".format(timestamp, SCENARIO_NAME)


def _create_parser():
    parser = argparse.ArgumentParser(prog="ydb_bench")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list", help="list available benchmark scenarios")

    describe = subparsers.add_parser("describe", help="describe a benchmark scenario")
    describe.add_argument("scenario", choices=(SCENARIO_NAME,))

    run = subparsers.add_parser("run", help="run a benchmark scenario")
    run.add_argument("scenario", choices=(SCENARIO_NAME,))
    run.add_argument("--profile", choices=("smoke", "baseline", "scale"), default="baseline")
    run.add_argument("--output", type=Path)
    run.add_argument("--work-dir", type=Path)
    run.add_argument("--threads", type=_positive_integer_list)
    run.add_argument("--actor-pairs", type=_positive_integer_list)
    run.add_argument("--inflight", type=_positive_integer_list)
    run.add_argument("--duration", type=_positive_integer)
    run.add_argument("--repetitions", type=_positive_integer)
    run.add_argument("--timeout", type=_positive_float)
    run.add_argument(
        "--perf",
        action="store_true",
        help="record cycles:u samples with perf; requires a --build=profile ydb_bench binary",
    )
    run.add_argument(
        "--perf-frequency",
        type=_positive_integer,
        default=99,
        help="sampling frequency for --perf (default: 99 Hz)",
    )
    run.add_argument(
        "--affinity",
        type=_affinity_modes,
        default=AFFINITY_MODES,
        help="all (default), or a comma-separated subset of: {}".format(", ".join(AFFINITY_MODES)),
    )
    return parser


def _describe():
    print("{}: bundled ydb/library/actors/core/ut_fat actor-system throughput benchmark".format(SCENARIO_NAME))
    for name in ("smoke", "baseline", "scale"):
        profile = _profile(name)
        print("  {}: {}".format(profile.name, profile.description))
    print("metrics: threads, actorPairs, in_flight, msgs_per_sec, elapsed_seconds")
    print("affinity: {}".format(", ".join(AFFINITY_MODES)))
    print("artifacts: run.json, per-repeat stdout/stderr/metrics.csv, summary.csv")


def _configuration(arguments):
    profile = _profile(arguments.profile)
    threads = arguments.threads or profile.threads
    actor_pairs = arguments.actor_pairs or profile.actor_pairs
    inflights = arguments.inflight or profile.inflights
    duration = arguments.duration or profile.duration_seconds
    repetitions = arguments.repetitions or profile.repetitions
    combinations = len(threads) * len(actor_pairs) * len(inflights)
    default_timeout = combinations * duration * 3 + 30
    return RunConfiguration(
        profile=profile.name,
        threads=threads,
        actor_pairs=actor_pairs,
        inflights=inflights,
        duration_seconds=duration,
        repetitions=repetitions,
        timeout_seconds=arguments.timeout or default_timeout,
        affinity_modes=arguments.affinity,
        perf_enabled=arguments.perf,
        perf_frequency=arguments.perf_frequency,
    )


def _prepare_output(path):
    path = path or _default_output_directory()
    try:
        path.mkdir(parents=True)
    except FileExistsError as error:
        raise BenchmarkError("output directory already exists: {}".format(path)) from error
    return path.resolve()


def _run(arguments, resource_loader, tool_revision):
    if resource_loader is None:
        raise BenchmarkError("the benchmark executable resource loader is not configured")
    if arguments.perf and str(tool_revision.get("build_type", "")).lower() != "profile":
        raise BenchmarkError("--perf requires ydb_bench built with --build=profile")

    output_directory = _prepare_output(arguments.output)
    if arguments.work_dir is not None:
        arguments.work_dir.mkdir(parents=True, exist_ok=True)
        work_dir_parent = arguments.work_dir.resolve()
    else:
        work_dir_parent = None

    with tempfile.TemporaryDirectory(prefix="ydb-bench-", dir=work_dir_parent) as temporary_directory:
        binary = extract_executable(resource_loader(RESOURCE_NAME), temporary_directory, RESOURCE_NAME)
        manifest = run_actors_core(
            binary,
            _configuration(arguments),
            output_directory,
            tool_revision=tool_revision,
            work_dir_hint=temporary_directory,
        )
    print("completed {}: {}".format(SCENARIO_NAME, output_directory))
    for affinity in manifest["affinity"]:
        details = ""
        if affinity.get("cpus") is not None:
            details = " cpus={}".format(",".join(map(str, affinity["cpus"])))
        elif affinity.get("reason"):
            details = " ({})".format(affinity["reason"])
        print("affinity {}: {}{}".format(affinity["mode"], affinity["status"], details))
    print("summary: {}".format(output_directory / manifest["summary"]))
    return 0


def main(argv=None, resource_loader=None, tool_revision=None):
    arguments = _create_parser().parse_args(argv)
    try:
        if arguments.command == "list":
            print("{}\tbundled actor-system throughput benchmark".format(SCENARIO_NAME))
            return 0
        if arguments.command == "describe":
            _describe()
            return 0
        return _run(arguments, resource_loader, tool_revision or {"commit_id": "unknown"})
    except BenchmarkInterrupted as error:
        print("ydb_bench: error: {}".format(error), file=sys.stderr)
        return 130
    except BenchmarkError as error:
        print("ydb_bench: error: {}".format(error), file=sys.stderr)
        return 1
