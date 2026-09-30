#!/usr/bin/env python3
"""Run ya coverage with bounded failure diagnostics and one targeted retry.

Only a corrupt raw LLVM profile in one identified failed coverage node after a
complete, successful test run permits retrying. Ambiguous failures stay failed.
--retest gives test nodes new UIDs (and thus new coverage nodes), while
--build-root isolates temporary files without changing ya's compilation cache.
Never merge around, delete, or silently exclude a damaged profile.

Artifacts contain at most 50 MiB per attempt (two attempts maximum). Logs are
redacted and may be truncated; raw profiles are either complete or omitted.
Only selected logs and the named damaged profiles are collected, not credentials,
environment dumps, binaries, or entire test-output/coverage archives.
"""

from __future__ import annotations

import argparse
from collections import deque
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile

MIB = 1024 * 1024
LOG_LIMIT = 8 * MIB
PROFILE_LIMIT = 16 * MIB
ATTEMPT_LIMIT = 50 * MIB
BAD_PROFILE = re.compile(
    r"(?:warning|error): ([^\s:]+\.profraw): "
    r"invalid instrumentation profile data \(file header is corrupt\)"
)
FAILED_COMMAND = re.compile(
    r"command \(pid: \d+\) (.+) failed with exit code \d+ in (\S+)"
)
WORK_DIR = re.compile(r"Tests run working directory: (\S+)")
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
LOG_NAMES = {
    "run_test.log", "ytest.report.trace", "results_accumulator.log",
    "results_merge.log", "build_clang_coverage_report.log",
    "run.stdout", "run.messages", "stderr", "list_stderr",
}


class Redactor:
    def __init__(self, environ: dict[str, str], secret_files: list[Path]):
        secrets = set()

        def remember(value):
            if isinstance(value, str) and len(value) >= 4:
                secrets.add(value)
                secrets.add(json.dumps(value)[1:-1])
                secrets.update(line for line in value.splitlines() if len(line) >= 4)
            elif isinstance(value, dict):
                for item in value.values():
                    remember(item)
            elif isinstance(value, list):
                for item in value:
                    remember(item)

        for key, value in environ.items():
            if re.search(r"TOKEN|PASSWORD|SECRET|ACCESS_KEY|PRIVATE_KEY", key):
                remember(value)
        for path in secret_files:
            # Read only explicitly supplied credential sources; never archive them.
            value = path.read_text(encoding="utf-8")
            remember(value)
            try:
                remember(json.loads(value))
            except ValueError:
                pass
        self.secrets = sorted(secrets, key=len, reverse=True)

    def __call__(self, text: str) -> str:
        for value in self.secrets:
            text = text.replace(value, "[REDACTED]")
        return text


def read_report(path: Path) -> dict:
    if path.stat().st_size > 64 * MIB:
        raise ValueError("test report exceeds 64 MiB")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("results"), list):
        raise ValueError("missing results array")
    return data


def tests_passed(report: dict) -> bool:
    """Fail closed on missing, partial, unknown, or unsuccessful ya results."""
    results = report.get("results", [])
    if not results or any(
        not isinstance(item, dict) or item.get("status") not in {"OK", "SKIPPED"}
        for item in results
    ):
        return False
    if any(
        item["status"] != "OK"
        for item in results
        if item.get("type") != "test" or item.get("suite") or item.get("chunk")
    ):
        return False
    suites = [item for item in results if item.get("type") == "test" and item.get("suite")]
    passed = [
        item for item in results
        if item.get("type") == "test" and not item.get("suite")
        and not item.get("chunk") and item.get("status") == "OK"
    ]
    progress = report.get("progress")
    if not suites or not passed or not isinstance(progress, dict) or not progress:
        return False
    entries = []
    for items in progress.values():
        if not isinstance(items, list):
            return False
        entries.extend(items)
    if not entries or not all(
        isinstance(item, dict) and type(item.get("total")) is int
        and type(item.get("done")) is int and item["total"] >= 0
        and item["done"] == item["total"] for item in entries
    ):
        return False
    return any(item.get("type") == "test" and item["total"] > 0 for item in entries)


def evidence(log: str) -> dict:
    log = ANSI.sub("", log)
    nodes = []
    unknown_failure = False
    for match in FAILED_COMMAND.finditer(log):
        try:
            argv = shlex.split(match[1])
        except ValueError:
            unknown_failure = True
            continue
        kind = next((arg for arg in argv if arg in {
            "resolve_clang_coverage", "build_clang_coverage_report",
        }), None)
        if kind is None:
            unknown_failure = True
            continue
        flags = {}
        archives = []
        for index, arg in enumerate(argv[:-1]):
            if arg == "--coverage-path":
                archives.append(argv[index + 1])
            elif arg in {"--log-path", "--llvm-profdata-tool", "--llvm-cov-tool"}:
                flags[arg] = argv[index + 1]
        node = {"cwd": match[2], "kind": kind, "coverage_paths": archives, **flags}
        if node not in nodes:
            nodes.append(node)
    profile_paths = sorted({m[1] for m in BAD_PROFILE.finditer(log)})
    profiles = sorted({PurePosixPath(path).name for path in profile_paths})
    work_dirs = list(dict.fromkeys(m[1] for m in WORK_DIR.finditer(log)))
    return {
        "bad_profiles": profiles[:128],
        "profile_paths": profile_paths[:128],
        "nodes": nodes[:32],
        "unknown_failure": unknown_failure or len(nodes) > 32 or len(profile_paths) > 128,
        "work_dirs": work_dirs[:2048],
    }


def merge_evidence(details: dict, update: dict):
    """Keep retry evidence from the entire stream, independent of the log tail."""
    details["unknown_failure"] |= update["unknown_failure"]
    for key, limit in (("nodes", 32), ("bad_profiles", 128), ("profile_paths", 128), ("work_dirs", 2048)):
        for item in update[key]:
            if item not in details[key]:
                if len(details[key]) < limit:
                    details[key].append(item)
                elif key != "work_dirs":
                    details["unknown_failure"] = True


def should_retry(returncode: int, report: dict, details: dict) -> bool:
    return (
        returncode == 255 and tests_passed(report)
        and bool(details["bad_profiles"]) and len(details["nodes"]) == 1
        and not details["unknown_failure"]
    )


def inside(path: Path, roots: list[Path]) -> bool:
    try:
        return any(path.resolve().is_relative_to(root.resolve()) for root in roots)
    except (OSError, RuntimeError):
        return False


class Bundle:
    def __init__(self, directory: Path, redactor: Redactor, limit: int = ATTEMPT_LIMIT):
        directory.mkdir(parents=True, exist_ok=False)
        self.directory = directory
        self.redact = redactor
        self.remaining = limit - MIB  # Reserve space for a bounded manifest.
        self.entries = []

    def save(self, name: str, data: bytes, source: str, *, text: bool = False):
        if len(self.entries) >= 1000:
            return False
        original_size = len(data)
        if text:
            data = self.redact(data.decode("utf-8", errors="replace")).encode("utf-8")
        if len(data) > self.remaining:
            self.note(source, "omitted: artifact size limit")
            return False
        (self.directory / name).write_bytes(data)
        self.remaining -= len(data)
        self.entries.append({
            "file": name, "source": source, "input_bytes": original_size,
            "saved_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
        })
        return True

    def note(self, source: str, reason: str):
        if len(self.entries) < 1000:
            self.entries.append({"source": source, "note": reason})

    def copy(self, path: Path, roots: list[Path], *, raw: bool = False) -> bool:
        try:
            if (not inside(path, roots) or not stat.S_ISREG(path.stat().st_mode)
                    or (raw and path.is_symlink())):
                self.note(str(path), "omitted: outside attempt roots or not a regular file")
                return False
            original_stat = path.stat()
            size = original_stat.st_size
            cap = PROFILE_LIMIT if raw else LOG_LIMIT
            if raw and (size > cap or size > self.remaining):
                self.note(str(path), f"omitted: raw profile size {size} exceeds limit")
                return False
            with path.open("rb") as stream:
                if size > cap:
                    stream.seek(size - cap)
                data = stream.read(cap + 1 if raw else cap)
            final_stat = path.stat()
            if raw and (len(data) != size or final_stat.st_size != size
                        or final_stat.st_mtime_ns != original_stat.st_mtime_ns):
                self.note(str(path), "omitted: raw profile changed while being collected")
                return False
            name = f"{len(self.entries):04d}-{path.name}"
            saved = self.save(name, data, str(path), text=not raw)
            if saved:
                self.entries[-1].update(source_bytes=size, source_mtime_ns=original_stat.st_mtime_ns)
            if size > cap:
                self.note(str(path), f"log truncated: saved last {cap} of {size} bytes")
            return saved
        except OSError as exc:
            self.note(str(path), f"unavailable: {exc}")
            return False

    def profile_from_tar(self, archive: Path, names: set[str], roots: list[Path]):
        if not names or not inside(archive, roots) or not archive.is_file():
            return
        try:
            with tarfile.open(archive, "r|*") as tar:
                for index, member in enumerate(tar):
                    if index >= 100000:
                        self.note(str(archive), "archive member scan limit reached")
                        break
                    name = PurePosixPath(member.name).name
                    if name not in names or not member.isfile():
                        continue
                    source = f"{archive}:{member.name}"
                    if member.size > min(PROFILE_LIMIT, self.remaining):
                        self.note(source, "omitted: raw profile size limit")
                        continue
                    with tar.extractfile(member) as stream:
                        data = stream.read(PROFILE_LIMIT + 1)
                    if len(data) != member.size:
                        self.note(source, "omitted: incomplete raw profile in archive")
                        continue
                    if self.save(f"{len(self.entries):04d}-{name}", data, source):
                        names.remove(name)
                    if not names:
                        break
        except (OSError, tarfile.TarError) as exc:
            self.note(str(archive), f"could not inspect archive: {exc}")

    def finish(self, metadata: dict):
        text = self.redact(json.dumps({**metadata, "files": self.entries[:1000]}, indent=2))
        if len(text.encode()) > MIB:
            text = json.dumps({"error": "manifest exceeded 1 MiB", "files_recorded": len(self.entries)})
        (self.directory / "manifest.json").write_text(text, encoding="utf-8")


def collect_diagnostics(directory: Path, build_root: Path, output: Path, log: Path,
                        report: dict, details: dict, redactor: Redactor, metadata: dict):
    bundle = Bundle(directory, redactor)
    roots = [build_root, output]
    bundle.copy(log, [log.parent])
    suites = set()
    tools = set()
    profile_paths = details["profile_paths"] if len(details["nodes"]) == 1 else []
    if len(details["nodes"]) > 1:
        bundle.note("profile attribution", "multiple failed nodes: raw profiles omitted and automatic retry disabled")
    for node in details["nodes"]:
        cwd = Path(node["cwd"])
        if not inside(cwd, roots):
            bundle.note(str(cwd), "omitted: node outside attempt roots")
            continue
        archives = [cwd / path for path in node["coverage_paths"]]
        for archive in archives:
            if archive.is_relative_to(cwd):
                suite = archive.parent.relative_to(cwd)
                if suite.parts and ".." not in suite.parts:
                    suites.add(suite)
        for profile in profile_paths:
            path = cwd / profile
            if not inside(path, [cwd]):
                bundle.note(profile, "omitted: profile outside coverage node")
                continue
            if bundle.copy(path, roots, raw=True):
                continue
            # Report-node extraction uses tmp/<archive index>/<profile>. Never
            # substitute a same-named, healthy profile from a different suite.
            relative = path.relative_to(cwd) if path.is_relative_to(cwd) else Path()
            archive_index = None
            if node["kind"] == "resolve_clang_coverage" and len(archives) == 1:
                archive_index = 0
            elif (len(relative.parts) >= 3 and relative.parts[0] == "tmp"
                  and relative.parts[1].isdigit()):
                archive_index = int(relative.parts[1])
            missing = {path.name}
            if archive_index is not None and archive_index < len(archives):
                bundle.profile_from_tar(archives[archive_index], missing, roots)
            if missing:
                bundle.note(profile, "damaged profile not recovered from the attempt")
        if "--log-path" in node:
            bundle.copy(Path(node["--log-path"]), roots)
        tools.update(node[key] for key in ("--llvm-profdata-tool", "--llvm-cov-tool") if key in node)

    # Diagnostics must not execute arbitrary paths printed by PR code.
    versions = {}
    tool_root = Path.home() / ".ya/tools"
    for tool in sorted(tools):
        path = Path(tool)
        if path.name not in {"llvm-profdata", "llvm-cov"} or not inside(path, [tool_root]):
            continue
        try:
            result = subprocess.run([tool, "--version"], capture_output=True, timeout=10)
            versions[tool] = result.stdout[:65536].decode("utf-8", errors="replace")
        except (OSError, subprocess.TimeoutExpired) as exc:
            versions[tool] = str(exc)

    log_dirs = [output / suite for suite in sorted(suites)]
    for work in details["work_dirs"]:
        path = Path(work)
        # Split and unsplit tests both belong to a suite root. The latter has
        # no testing_out_stuff ancestor; its working directory is the root.
        for parent in (path, *path.parents):
            if any(parent.parts[-len(suite.parts):] == suite.parts for suite in suites):
                log_dirs.append(parent)
                break
    seen = set()
    for directory_root in dict.fromkeys(log_dirs):
        if not inside(directory_root, roots):
            continue
        for index, (parent, dirs, files) in enumerate(os.walk(directory_root, followlinks=False)):
            dirs.sort()
            if index >= 2000 or bundle.remaining < 65536 or len(seen) >= 500:
                bundle.note(str(directory_root), "test log scan/budget limit reached")
                break
            for name in sorted(files):
                if len(seen) >= 500:
                    break
                if name not in LOG_NAMES and not (name.startswith("clangcov_resolve") and name.endswith(".log")):
                    continue
                path = Path(parent) / name
                resolved = path.resolve()
                if resolved not in seen:
                    seen.add(resolved)
                    bundle.copy(path, roots)
    # Own reduced report: never include arbitrary report fields/environment dumps.
    results = [{key: item[key] for key in ("type", "path", "name", "subtest_name", "status", "suite", "chunk")
                if key in item} for item in report.get("results", []) if isinstance(item, dict)]
    data = json.dumps(results, indent=2).encode()
    if len(data) <= LOG_LIMIT:
        bundle.save("test-results.json", data, "ya build-results-report (selected fields)", text=True)
    bundle.finish({**metadata, "evidence": details, "llvm_versions": versions})


def run_attempt(command: list[str], log: Path, redactor: Redactor) -> tuple[int, bool, dict]:
    tail = deque()
    details = evidence("")
    size = 0
    truncated = False
    cancelled = False
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, errors="replace", start_new_session=True)

    def cancel(signum, _frame):
        nonlocal cancelled
        cancelled = True
        try:
            os.killpg(proc.pid, signum)
        except ProcessLookupError:
            pass

    previous = {sig: signal.signal(sig, cancel) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        for line in proc.stdout:
            merge_evidence(details, evidence(line))
            line = redactor(line)
            print(line, end="", flush=True)
            data = line.encode("utf-8")
            if len(data) > LOG_LIMIT:
                data = data[-LOG_LIMIT:]
                truncated = True
            tail.append(data)
            size += len(data)
            while size > LOG_LIMIT:
                size -= len(tail.popleft())
                truncated = True
        rc = proc.wait()
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        proc.stdout.close()
        log.write_bytes((b"[Earlier output truncated]\n" if truncated else b"") + b"".join(tail))
    return (rc if rc >= 0 else 128 - rc), cancelled, details


def github_output(key: str, value: str):
    if "\n" in value or "\r" in value:
        raise ValueError("multiline GitHub output refused")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
            stream.write(f"{key}={value}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, choices=("cli", "cpp_sdk", "cli_workload"))
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--secret-file", action="append", default=[], type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("expected ya make command after --")
    args.work_dir.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix=f"{args.suite}-", dir=args.work_dir)).resolve()
    secret_files = list(args.secret_file)
    credential = os.environ.get("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")
    if credential:
        secret_files.append(Path(credential))
    redactor = Redactor(dict(os.environ), secret_files)
    diagnostics = root / "diagnostics"
    github_output("diagnostics_dir", str(diagnostics))

    for number in (1, 2):
        attempt = root / f"attempt-{number}"
        attempt.mkdir()
        output = attempt / "output"
        # Keep the standard marker recognized by the LCOV generated-source filter.
        build_root = attempt / ".ya/build/build_root"
        output.mkdir()
        build_root.mkdir(parents=True)
        log = attempt / "ya.log"
        report_path = attempt / "report.json"
        invocation = command + ["--output", str(output), "--build-root", str(build_root),
                                "--build-results-report", str(report_path)]
        if number == 2:
            invocation.append("--retest")
        print(f"Coverage {args.suite}: attempt {number}/2", flush=True)
        rc, cancelled, details = run_attempt(invocation, log, redactor)
        if rc == 0 and not cancelled:
            github_output("out_dir", str(output))
            if number == 2:
                print("::warning::Coverage succeeded on a fresh retry; first-attempt diagnostics are retained.")
            return 0

        github_output("had_failure", "true")
        report = {}
        report_error = None
        try:
            report = read_report(report_path)
        except (OSError, ValueError) as exc:
            report_error = str(exc)
        retry = number == 1 and not cancelled and should_retry(rc, report, details)
        metadata = {"suite": args.suite, "attempt": number, "exit_code": rc,
                    "cancelled": cancelled, "retry": retry, "report_error": report_error,
                    "run_id": os.environ.get("GITHUB_RUN_ID"),
                    "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT")}
        try:
            collect_diagnostics(diagnostics / f"attempt-{number}", build_root, output,
                                log, report, details, redactor, metadata)
        except Exception as exc:
            # Collection must not hide the original failure or consume the retry.
            print(redactor(f"::warning::Could not collect coverage diagnostics: {exc}"), flush=True)
        if not retry:
            return rc or 1
        print("::warning::All tests passed, but LLVM rejected a corrupt raw profile. Retrying once with fresh test/coverage nodes.", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
