import signal
import subprocess
from multiprocessing import Pool
from pathlib import Path

"""
Parallel backfilling helper for pyperformance runs on isolated CPUs.

Reads `sha=branch` pairs from backfill_shas.txt, invokes run-pyperformance.sh
for each revision, and lets that wrapper pin the workload to an isolated CPU,
materialize benchmark.conf, build CPython, and upload results to
speed.python.org. Stdout/stderr for each revision are captured under
output/<branch>-<sha>.(out|err).
"""


def get_revisions() -> tuple[str, str]:
    revisions = []
    with open("backfill_shas.txt", "r") as f:
        for line in f:
            sha, branch = line.split("=")
            revisions.append((sha, branch.rstrip()))
    return revisions


def run_pyperformance(revision):
    sha, branch = revision
    print(f"Running run-pyperformance.sh with sha: {sha}, branch: {branch}")
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{branch}-{sha}.out"
    err_file = output_dir / f"{branch}-{sha}.err"
    with open(out_file, "w") as output, open(err_file, "w") as error:
        subprocess.run(
            [
                "./run-pyperformance.sh",
                "-x",
                "--",
                "compile",
                "benchmark.conf",
                sha,
                branch,
            ],
            stdout=output,
            stderr=error,
        )


if __name__ == "__main__":
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGINT, original_sigint_handler)
    with Pool(8) as pool:
        res = pool.map_async(run_pyperformance, get_revisions())
        # Without the timeout this blocking call ignores all signals.
        res.get(86400)
