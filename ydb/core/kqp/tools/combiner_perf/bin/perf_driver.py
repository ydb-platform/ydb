#!/usr/bin/env python3

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


ATTEMPTS = 5
PERF_BINARY = "perf"
# /spilling is the known NVMe mount on the dedicated performance host
DATA_TMP_DIR = Path(os.environ.get("COMBINER_PERF_DATA_DIR", "/spilling/perfdata"))
# Try using a non-libbfd build (slower symbolization) if getting BFD dwarf parsing errors
# PERF_BINARY = "/usr/lib/linux-tools/5.4.0-216-generic/perf"
PERF_RECORD_ARGS = ["-F", "250", "--call-graph", "dwarf,16384"]


def usage():
    program = Path(sys.argv[0]).name
    print("Usage: {} OUTPUT_FILE [--] COMMAND [ARG ...]".format(program), file=sys.stderr)


def parse_arguments():
    if len(sys.argv) < 3:
        usage()
        sys.exit(2)

    output_path = Path(sys.argv[1])
    command = sys.argv[2:]
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        usage()
        sys.exit(2)

    return output_path, command


def parse_result_time(stdout, attempt):
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            "attempt {} produced invalid JSON on stdout: {}, output: {}".format(attempt, error, stdout)
        )

    if not isinstance(result, dict):
        raise RuntimeError("attempt {} produced JSON that is not an object".format(attempt))

    result_time = result.get("resultTime")
    if isinstance(result_time, bool) or not isinstance(result_time, int):
        raise RuntimeError(
            "attempt {} has no integer resultTime in its JSON output".format(attempt)
        )

    return result_time


def main():
    output_path, command = parse_arguments()
    if not output_path.parent.is_dir():
        raise RuntimeError(
            "output directory does not exist: {}".format(output_path.parent)
        )
    DATA_TMP_DIR.mkdir(parents=True, exist_ok=True)
    if not DATA_TMP_DIR.is_dir():
        raise RuntimeError("perf data path is not a directory: {}".format(DATA_TMP_DIR))

    runs = []
    for attempt in range(1, ATTEMPTS + 1):
        data_path = DATA_TMP_DIR / "{}.{}.attempt-{}.perf.data".format(
            output_path.name, os.getpid(), attempt
        )
        perf_command = (
            [PERF_BINARY, "record"]
            + PERF_RECORD_ARGS
            + ["-o", str(data_path), "--"]
            + command
        )

        print("Recording attempt {}/{}...".format(attempt, ATTEMPTS), file=sys.stderr)
        sys.stderr.flush()
        completed = subprocess.run(perf_command, stdout=subprocess.PIPE, text=True)
        if completed.returncode != 0:
            raise RuntimeError(
                "perf record failed on attempt {} with exit status {}".format(
                    attempt, completed.returncode
                )
            )

        result_time = parse_result_time(completed.stdout, attempt)
        runs.append((result_time, data_path))
        print("Attempt {} resultTime: {}".format(attempt, result_time), file=sys.stderr)
        sys.stderr.flush()

    best_time, best_data_path = min(runs, key=lambda run: run[0])
    print(
        "Fastest attempt: {} (resultTime {})".format(best_data_path, best_time),
        file=sys.stderr,
    )

    temporary_output = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            prefix=".{}-".format(output_path.name),
            dir=str(output_path.parent),
            delete=False,
        ) as output_file:
            temporary_output = Path(output_file.name)
            completed = subprocess.run(
                [PERF_BINARY, "script", "-i", str(best_data_path)], stdout=output_file
            )

        if completed.returncode != 0:
            raise RuntimeError(
                "perf script failed with exit status {}".format(completed.returncode)
            )
        os.replace(str(temporary_output), str(output_path))
        temporary_output = None
    finally:
        if temporary_output is not None:
            try:
                temporary_output.unlink()
            except FileNotFoundError:
                pass

    print("Wrote perf script output to {}".format(output_path), file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError) as error:
        print("error: {}".format(error), file=sys.stderr)
        sys.exit(1)
