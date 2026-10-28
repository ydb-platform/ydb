#!/usr/bin/env python3
"""Run complete coverage attempts and keep test verdicts separate from measurement.

Ordinary assertion failures may supply coverage, but only after checking the raw
test trace: YA's JSON report calls both assertions and crashes REGULAR failures.
Known test failures get up to three complete attempts; corrupt profiles retain
the narrower, one-retry policy. Ambiguous or incomplete measurements stay failed.
--retest gives test nodes new UIDs (and thus new coverage nodes), while
--build-root isolates temporary files without changing ya's compilation cache.
Never merge around, delete, or silently exclude a damaged profile.

Artifacts contain at most 50 MiB per attempt (three attempts maximum). Logs are
redacted and may be truncated; raw profiles are either complete or omitted.
Only selected logs and the named damaged profiles are collected, not credentials,
environment dumps, binaries, or entire test-output/coverage archives.
"""

from __future__ import annotations

import argparse
import ast
from collections import deque
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import shlex
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time

MIB = 1024 * 1024
LOG_LIMIT = 8 * MIB
PROFILE_LIMIT = 16 * MIB
ATTEMPT_LIMIT = 50 * MIB
MAX_ATTEMPTS = 3
PROFILE_PROBE_TIMEOUT = 60
PROFILE_PROBE_OUTPUT_LIMIT = 64 * 1024
BAD_PROFILE = re.compile(
    r"(?:warning|error): ([^\s:]+\.profraw): "
    r"invalid instrumentation profile data \(file header is corrupt\)"
)
FAILED_COMMAND = re.compile(
    r"command \(pid: \d+\) (.+) failed with exit code \d+ in (\S+)"
)
WORK_DIR = re.compile(r"Tests run working directory: (\S+)")
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
# pytest can turn a failed child process into a normal test assertion. Preserve
# explicit evidence of interruption instead of accepting the generic 'fail'.
INTERRUPTED_PROCESS = re.compile(
    r"ExecutionTimeoutError|SignalInterruptionError|subprocess\.TimeoutExpired|"
    r"yatest\.common\.process\.TimeoutError|"
    r"\bhas failed with code (?:-\d+|134|137|139|143)\b|"
    r"\bdied with <Signals\.[A-Z0-9_]+:\s*\d+>|"
    r"\b(?:exit[ _]code|returncode)\s*[:=]?\s*-\d+\b|"
    r"(?<!\w)-\d+\s*=\s*[^\n]{0,200}\.(?:returncode|exit_code)\b|"
    r"\b(?:killed|terminated) by signal\b|\bSegmentation fault\b|\bcore dumped\b|"
    r"\b(?:AddressSanitizer|UndefinedBehaviorSanitizer|ThreadSanitizer):",
    re.IGNORECASE,
)
# Match the expanded negative value only in the same pytest failure block as
# an assertion about a process-result field. Bare 'assert -9 == 0' is ambiguous.
INTERRUPTED_DICT_ASSERTION = re.compile(
    r"^[ \t]*(?:>[ \t]*)?assert[ \t]+[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*"
    r"\[[ \t]*(?P<quote>['\"])(?:exit_code|returncode|return_code)(?P=quote)[ \t]*\]"
    r"[ \t]*==[ \t]*0\b[^\n]*"
    r"(?:\n[ \t]*E(?:[ \t][^\n]*)?)*?"
    r"\n[ \t]*E[ \t]+assert[ \t]+-[1-9]\d*[ \t]+==[ \t]+0\b",
    re.MULTILINE,
)
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


def test_failure_report(report: dict) -> bool:
    """Recognize test-only failures without normalizing incomplete raw statuses.

    This permits a retry, not publication. In particular REGULAR includes CRASHED
    in YA's report_prototype.py, and NOT_LAUNCHED must never become SKIPPED here.
    """
    results = report.get("results", [])
    if not isinstance(results, list) or not results:
        return False
    failed = False
    normalized = []
    for row in results:
        if not isinstance(row, dict):
            return False
        row = dict(row)
        if row.get("type") == "test" and row.get("status") in {"FAILED", "NOT_LAUNCHED"}:
            failed = True
            row["status"] = "OK"
        normalized.append(row)
    return failed and tests_passed({**report, "results": normalized})


def read_mutes(path: Path | None) -> list[tuple[re.Pattern, re.Pattern]]:
    """Use the common CI mute syntax: only '*' is a wildcard; other chars literal.

    Mute changes diagnostics/retry policy, never execution, raw test status or the
    coverage completeness requirements. No imports from measured PR helper code.
    """
    if path is None:
        return []
    patterns = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        parts = line.strip().split(" ", 1)
        if len(parts) != 2 or not all(parts):
            raise ValueError(f"Invalid mute entry in {path}")
        patterns.append(tuple(re.compile("^" + re.escape(part).replace(r"\*", ".*") + "$")
                              for part in parts))
    return patterns


def failed_test_rows(report: dict, mutes: list) -> list[dict]:
    failures = []
    for row in report.get("results", []):
        if (not isinstance(row, dict) or row.get("type") != "test" or row.get("suite")
                or row.get("chunk") or row.get("status") != "FAILED"):
            continue
        name = ".".join(part for part in (row.get("name", "").replace(".py::", ".py."),
                                         row.get("subtest_name", "")) if part)
        failures.append({"path": row.get("path", ""), "name": name,
                         "muted": any(p.fullmatch(row.get("path", "")) and t.fullmatch(name)
                                      for p, t in mutes)})
    return failures


def interrupted_process(message: str) -> bool:
    # YA/pytest may surround source and error lines with presentation markup.
    text = re.sub(r"\[\[[^\]\n]*\]\]", "", ANSI.sub("", message))
    return bool(INTERRUPTED_PROCESS.search(text) or INTERRUPTED_DICT_ASSERTION.search(text))


def assertion_only_failure(report: dict, output: Path, build_root: Path) -> tuple[bool, str]:
    """Check evidence of ordinary failures in the authoritative merged trace.

    The suite's relaxed status must be OK (no suite-level infrastructure failure).
    Trace container errors, unfinished tests, crashes, timeouts and unknown statuses
    are rejected even when the JSON report presents them as regular failures.
    This cannot prove that every hidden child process flushed every counter;
    downstream LLVM export must still validate the collected profile.
    """
    if not test_failure_report(report):
        return False, "not a complete test-only failure report"
    results = report["results"]
    if any(row.get("status") not in {"OK", "SKIPPED", "FAILED"} for row in results):
        return False, "a test was not launched or has an unknown status"
    failing = [row for row in results if row.get("type") == "test" and row.get("status") == "FAILED"]
    if any(row.get("error_type") != "REGULAR" for row in failing):
        return False, "test report contains non-regular failures"
    if any(interrupted_process(row.get("rich-snippet", "")) for row in failing):
        return False, "test failure reports an interrupted child process or sanitizer error"
    suites = [row for row in results if row.get("type") == "test" and row.get("suite")]
    leaves = [row for row in failing if not row.get("suite") and not row.get("chunk")]
    if not leaves:
        return False, "failed container without a completed failed test"
    checked = set()
    for failed in failing:
        # A suite row is already its own container; it has hid, not suite_hid.
        # Several suites (e.g. py3test and import_test) can share one path.
        matches = [failed] if failed.get("suite") else [
            suite for suite in suites if suite.get("path") == failed.get("path")
            and (not failed.get("suite_hid") or suite.get("hid") == failed["suite_hid"])
        ]
        if len(matches) != 1:
            return False, "failed test cannot be attributed to exactly one suite"
        suite = matches[0]
        if suite.get("suite_status") != "OK":
            return False, "failed suite has an infrastructure/container error"
        path, name = suite.get("path", ""), suite.get("name", "")
        trace = output / path / "test-results" / name / "ytest.report.trace"
        if not path or not name or not inside(trace, [output, build_root]):
            return False, "invalid trace location"
        if trace in checked:
            continue
        checked.add(trace)
        try:
            if trace.stat().st_size > 64 * MIB:
                raise ValueError("trace exceeds 64 MiB")
            states = {}
            with trace.open(encoding="utf-8") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    value = event["value"]
                    event_name = event["name"]
                    if event_name in {"suite-event", "chunk-event", "suite_event", "chunk_event"} and value.get("errors"):
                        raise ValueError("trace contains container errors")
                    if event_name not in {"subtest-started", "subtest-finished"}:
                        continue
                    key = (value["class"], value["subtest"], value.get("chunk_index", 0),
                           value.get("chunk_filename", ""))
                    status = value.get("status") if event_name == "subtest-finished" else "started"
                    if status not in {"started", "good", "fail", "diff", "skipped", "deselected", "xfail", "xpass"}:
                        raise ValueError(f"unsafe test trace status: {status}")
                    if status in {"fail", "diff"} and interrupted_process(value.get("comment", "")):
                        raise ValueError("failed test reports an interrupted child process or sanitizer error")
                    states[key] = status
            if not states or "started" in states.values():
                raise ValueError("empty trace or unfinished test")
            for row in leaves:
                if row.get("path") != path or (row.get("suite_hid") and row["suite_hid"] != suite.get("hid")):
                    continue
                matching = [status for key, status in states.items()
                            if key[:2] == (row.get("name"), row.get("subtest_name"))]
                if not matching or any(status not in {"fail", "diff"} for status in matching):
                    raise ValueError("failed test has no unambiguous assertion-failure trace")
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            return False, f"cannot establish complete assertion-only execution: {exc}"
    return True, "all failed tests finished normally; no trace infrastructure errors"


def coverage_artifacts_present(output: Path) -> bool:
    """The downstream llvm-cov export must still parse/validate the merged profile.

    Do not reject 'No valid coverage profiles' messages globally: unused DEPENDS
    binaries legitimately produce none in individual FORK_SUBTESTS chunks.
    """
    try:
        return all(path.is_file() and path.stat().st_size > 0 for path in (
            output / "coverage.report/coverage.profdata", output / "build_clang_coverage_report.log",
        ))
    except OSError:
        return False


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


def probe_profile_merge(command: list[str], cwd: Path) -> tuple[int, str, bool, str]:
    """Read profiles with LLVM, discarding merged output; bound stderr and time."""
    data = bytearray()
    cancelled = False
    problem = ""
    proc = subprocess.Popen(command, cwd=cwd, stdout=subprocess.DEVNULL,
                            stderr=subprocess.PIPE, start_new_session=True)

    def stop():
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def cancel(_signum, _frame):
        nonlocal cancelled
        cancelled = True
        stop()

    previous = {sig: signal.signal(sig, cancel) for sig in (signal.SIGINT, signal.SIGTERM)}
    deadline = time.monotonic() + PROFILE_PROBE_TIMEOUT
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(proc.stderr, selectors.EVENT_READ)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if cancelled or remaining <= 0:
                    problem = "cancelled" if cancelled else "deadline exceeded"
                    break
                if not selector.select(min(remaining, 0.1)):
                    continue
                chunk = os.read(proc.stderr.fileno(), min(4096, PROFILE_PROBE_OUTPUT_LIMIT + 1 - len(data)))
                if not chunk:
                    selector.unregister(proc.stderr)
                    break
                data.extend(chunk)
                if len(data) > PROFILE_PROBE_OUTPUT_LIMIT:
                    problem = "stderr size limit exceeded"
                    break
        if problem:
            stop()
        try:
            rc = proc.wait(timeout=max(0.01, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            problem = "deadline exceeded"
            stop()
            rc = proc.wait()
    finally:
        if proc.poll() is None:
            stop()
        proc.wait()
        proc.stderr.close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return rc, bytes(data[:PROFILE_PROBE_OUTPUT_LIMIT]).decode("utf-8", errors="replace"), cancelled, problem


def recover_profile_evidence(returncode: int, report: dict, details: dict,
                             build_root: Path, log: Path, redactor: Redactor,
                             cancelled: bool = False) -> bool:
    """Recover the first LLVM warning swallowed by YA's progress-status renderer.

    This is not another coverage attempt or a repaired report. Only the known
    failed report node is diagnosed, and its merged output goes to /dev/null.
    Never execute a command copied from a log: validate the expected tool and
    input layout, construct a new argv, and preserve failure on any ambiguity.
    """
    if (cancelled or returncode != 255 or not tests_passed(report)
            or details["unknown_failure"] or details["bad_profiles"]
            or len(details["nodes"]) != 1
            or details["nodes"][0]["kind"] != "build_clang_coverage_report"):
        return cancelled
    diagnostic = ""
    try:
        node = details["nodes"][0]
        cwd = Path(node["cwd"])
        source_log = Path(node["--log-path"])
        tool = Path(node["--llvm-profdata-tool"])
        tool_root = Path(os.environ.get("YA_CACHE_DIR") or Path.home() / ".ya") / "tools"
        if (not cwd.is_absolute() or not cwd.is_dir() or not inside(cwd, [build_root])
                or source_log != cwd / "build_clang_coverage_report.log"
                or source_log.is_symlink() or not inside(source_log, [cwd])
                or not stat.S_ISREG(source_log.stat().st_mode)
                or not tool.is_absolute() or tool.name != "llvm-profdata"
                or not tool_root.is_absolute() or not inside(tool, [tool_root])
                or not stat.S_ISREG(tool.stat().st_mode)):
            raise ValueError("unexpected report node, log or LLVM tool path")
        with source_log.open("rb") as stream:
            contents = stream.read(LOG_LIMIT + 1)
        if len(contents) > LOG_LIMIT:
            raise ValueError("node log exceeds size limit")
        commands = re.findall(r"^.*\bExecuting (\[.*\])$", contents.decode("utf-8"), re.MULTILINE)
        if len(commands) != 1:
            raise ValueError("expected exactly one logged profile merge")
        original = ast.literal_eval(commands[0])
        prefix = [str(tool), "merge", "-sparse", "-o", str(cwd / "coverage/coverage.profdata")]
        if (not isinstance(original, list) or original[:5] != prefix
                or not 6 <= len(original) <= 16389 or not all(isinstance(arg, str) for arg in original)):
            raise ValueError("unexpected profile merge arguments")
        inputs = {}
        for name in original[5:]:
            relative = Path(name)
            path = cwd / relative
            if (len(relative.parts) != 3 or relative.parts[0] != "tmp"
                    or not relative.parts[1].isdigit() or relative.suffix != ".profraw"
                    or path.is_symlink() or not inside(path, [cwd])
                    or not stat.S_ISREG(path.stat().st_mode)):
                raise ValueError("unexpected raw profile path")
            inputs[str(path.resolve())] = name
        if len(inputs) != len(original) - 5:
            raise ValueError("duplicate raw profile inputs")
        # No options, response files, alternate output or executable from the log.
        command = [str(tool.resolve()), "merge", "-sparse", "-o", os.devnull, *inputs]
        rc, diagnostic, cancelled, problem = probe_profile_merge(command, cwd)
        if cancelled or problem:
            raise ValueError(problem or "cancelled")
        recovered = evidence("")
        for line in diagnostic.splitlines():
            line = line.strip()
            if not line or line == "error: no profile can be merged":
                continue
            match = BAD_PROFILE.fullmatch(line)
            if match is None or match[1] not in inputs:
                raise ValueError("LLVM reported an unrelated or unattributed error")
            merge_evidence(recovered, evidence(line.replace(match[1], inputs[match[1]], 1)))
        if rc != 1 or not recovered["bad_profiles"] or recovered["unknown_failure"]:
            raise ValueError("LLVM did not confirm the known corrupt-header failure")
        merge_evidence(details, recovered)
        diagnostic += "Confirmed corrupt raw profile; normal guarded retry policy applies.\n"
    except (OSError, ValueError, KeyError, SyntaxError, TypeError, RecursionError) as exc:
        diagnostic += f"Raw-profile diagnostic not used: {exc}\n"
    try:
        log.write_text(redactor(diagnostic), encoding="utf-8")
    except OSError as exc:
        print(redactor(f"::warning::Could not save raw-profile diagnostic: {exc}"), flush=True)
    return cancelled


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
    profile_diagnostic = log.parent / "profile-diagnostic.log"
    if profile_diagnostic.exists():
        bundle.copy(profile_diagnostic, [log.parent])
    suites = set()
    for row in report.get("results", []):
        if (isinstance(row, dict) and row.get("type") == "test" and row.get("suite")
                and row.get("status") != "OK" and row.get("path") and row.get("name")):
            suite = Path(row["path"]) / "test-results" / row["name"]
            if not suite.is_absolute() and ".." not in suite.parts:
                suites.add(suite)
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
    results = [{key: item[key] for key in ("type", "path", "name", "subtest_name", "status", "suite", "chunk",
                                         "error_type", "suite_status")
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
    parser.add_argument("--mute-file", type=Path,
                        help="Trusted common CI mute list; labels failures, never excludes tests")
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
    mutes = read_mutes(args.mute_file)
    diagnostics = root / "diagnostics"
    github_output("diagnostics_dir", str(diagnostics))

    corruption_retries = 0
    for number in range(1, MAX_ATTEMPTS + 1):
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
        if number > 1:
            invocation.append("--retest")
        print(f"Coverage {args.suite}: attempt {number}/{MAX_ATTEMPTS}", flush=True)
        rc, cancelled, details = run_attempt(invocation, log, redactor)
        report = {}
        report_error = None
        try:
            report = read_report(report_path)
        except (OSError, ValueError) as exc:
            report_error = str(exc)
        if corruption_retries == 0 and number < MAX_ATTEMPTS:
            cancelled = recover_profile_evidence(
                rc, report, details, build_root, attempt / "profile-diagnostic.log", redactor, cancelled,
            )
        artifacts_present = coverage_artifacts_present(output)
        if (rc == 0 and not cancelled and tests_passed(report) and artifacts_present
                and not details["unknown_failure"] and not details["nodes"] and not details["bad_profiles"]):
            github_output("out_dir", str(output))
            if number > 1:
                print("::warning::Coverage succeeded on a fresh retry; earlier diagnostics are retained.")
            return 0

        github_output("had_failure", "true")
        failures = failed_test_rows(report, mutes)
        assertion_only, reason = assertion_only_failure(report, output, build_root)
        # Exit 10 is YA's test-verdict code. Never reinterpret build failures,
        # cancellations, failed coverage commands or partial reports as tests.
        test_failure = (rc == 10 and test_failure_report(report) and not cancelled
                        and not details["unknown_failure"] and not details["nodes"]
                        and not details["bad_profiles"])
        usable_assertions = test_failure and assertion_only and artifacts_present
        muted_only = bool(failures) and all(row["muted"] for row in failures)
        corrupt_retry = (corruption_retries == 0 and should_retry(rc, report, details))
        retry = (number < MAX_ATTEMPTS and not cancelled
                 and (corrupt_retry or (test_failure and not (usable_assertions and muted_only))))
        accepted_assertions = usable_assertions and not retry
        print(redactor(f"Coverage assessment: exit={rc}; {reason}; "
                       f"merged artifacts present={artifacts_present}; retry={retry}"), flush=True)
        metadata = {"suite": args.suite, "attempt": number, "exit_code": rc,
                    "cancelled": cancelled, "retry": retry, "report_error": report_error,
                    "assertion_only": assertion_only, "assessment": reason,
                    "coverage_artifacts_present": artifacts_present,
                    "accepted_assertions": accepted_assertions, "failed_tests": failures,
                    "run_id": os.environ.get("GITHUB_RUN_ID"),
                    "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT")}
        try:
            collect_diagnostics(diagnostics / f"attempt-{number}", build_root, output,
                                log, report, details, redactor, metadata)
        except Exception as exc:
            # Collection must not hide the original failure or consume the retry.
            print(redactor(f"::warning::Could not collect coverage diagnostics: {exc}"), flush=True)
        if accepted_assertions:
            github_output("out_dir", str(output))
            github_output("test_failures", str(len(failures)))
            print("::warning::Tests contain completed assertion failures; retaining their coverage. "
                  "The normal test workflow owns their verdict. LLVM export must still validate the report.", flush=True)
            for failure in failures[:20]:
                label = "muted" if failure["muted"] else "unmuted"
                print(redactor(f"  [{label}] {failure['path']}: {failure['name']}"), flush=True)
            return 0
        if not retry:
            return rc or 1
        if corrupt_retry:
            corruption_retries += 1
            print("::warning::All tests passed, but LLVM rejected a corrupt raw profile. Retrying once with fresh test/coverage nodes.", flush=True)
        else:
            print("::warning::Retrying the entire component after test failures with fresh test/coverage nodes; "
                  "no subset report or cross-attempt coverage merge is used.", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
