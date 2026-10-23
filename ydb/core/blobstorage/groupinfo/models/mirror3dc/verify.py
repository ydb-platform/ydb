"""Generate and exhaustively verify the bounded mirror-3-dc physical-realm model."""

import argparse
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import sys

from model_common import read_source
from realms import generate


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command, cwd, log, timeout):
    with log.open("w") as output:
        output.write(shlex.join(command) + "\n")
        output.flush()
        try:
            return subprocess.run(command, cwd=cwd, stdout=output, stderr=output, timeout=timeout).returncode
        except subprocess.TimeoutExpired:
            output.write("\nVerification command timed out.\n")
            return 124


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, required=True, help="New or empty directory for generated files and logs"
    )
    parser.add_argument("--depth", type=int, default=100000)
    parser.add_argument("--memory", type=int, default=2048, help="Verifier memory limit in MiB")
    parser.add_argument("--timeout", type=int, default=600, help="Timeout in seconds for each command")
    args = parser.parse_args()
    if min(args.depth, args.memory, args.timeout) <= 0:
        parser.error("Limits must be positive")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        parser.error("Output directory must be empty")
    spin = os.environ.get("SPIN") or shutil.which("spin")
    compiler = shlex.split(os.environ.get("CC", "gcc"))
    if not spin or not compiler or not shutil.which(compiler[0]):
        parser.error("Install SPIN and a C compiler, or set SPIN and CC")
    output.mkdir(parents=True, exist_ok=True)
    compile_flags = ["-O0", "-DSAFETY", f"-DMEMLIM={args.memory}"]
    runtime_flags = [f"-m{args.depth}", "-w20"]
    manifest = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "host": platform.node(),
        "python": sys.version,
        "executable": sys.executable,
        "source_sha256": {
            name: hashlib.sha256(read_source(name)).hexdigest()
            for name in ("graph.json", "model_common.py", "realms.py", "verify.py")
        },
        "spin": subprocess.check_output([spin, "-V"], text=True).strip(),
        "compiler": subprocess.check_output([*compiler, "--version"], text=True).strip(),
        "compile_flags": compile_flags,
        "runtime_flags": runtime_flags,
        "command_timeout_seconds": args.timeout,
    }
    manifest_path = output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    sources = output / "sources"
    sources.mkdir()
    for name in manifest["source_sha256"]:
        (sources / name).write_bytes(read_source(name))
    with (output / "generation.txt").open("w") as log:
        with redirect_stdout(log), redirect_stderr(log):
            audit = generate(output)
    model = output / "model.pml"
    manifest["model"] = str(model)
    manifest["model_sha256"] = digest(model)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    results = {}
    for name, defines in (("safety", []), ("recovery", ["-DCHECK_RECOVERY"])):
        work = output / name
        work.mkdir()
        commands = [
            ([spin, "-a", *defines, str(model)], "generate.txt"),
            ([*compiler, *compile_flags, "-o", "pan", "pan.c"], "compile.txt"),
            ([str(work / "pan"), *runtime_flags], "verification.txt"),
        ]
        results[name] = {"status": "unknown", "defines": defines}
        for command, filename in commands:
            code = run(command, work, work / filename, args.timeout)
            if code != 0:
                results[name].update(failed_command=command, exit_code=code)
                break
        log = work / "verification.txt"
        if log.exists():
            text = log.read_text()
            match = re.search(r"errors:\s*(\d+)", text)
            if match:
                errors = int(match[1])
                results[name]["errors"] = errors
                if errors:
                    results[name]["status"] = "violated"
                    run(
                        [spin, *defines, "-t", "-p", "-g", "-k", str(work / "model.pml.trail"), str(model)],
                        work,
                        work / "counterexample.txt",
                        args.timeout,
                    )
                elif (
                    code == 0
                    and "Full statespace search" in text
                    and not re.search(
                        r"Search not completed|max search depth too small|out of memory|timed out|hash table overflow",
                        text,
                        re.IGNORECASE,
                    )
                ):
                    results[name]["status"] = "holds"
        print(f"{name}: {results[name]['status']} ({log})", flush=True)
    (output / "verification.json").write_text(json.dumps(results, indent=2) + "\n")
    return (
        0
        if (
            not audit["unsafe_count"]
            and not audit["blocked_count"]
            and all(result["status"] == "holds" for result in results.values())
        )
        else 1
    )


if __name__ == "__main__":
    sys.exit(main())
