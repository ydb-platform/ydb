#!/usr/bin/env python3
"""Upload unchanged LCOV without collecting metadata for the entire checkout.

Codecov CLI's --file/--disable-search restrict reports, not source-file metadata.
Run the official CLI outside Git with copies of exactly the LCOV sources. Keep
file fixes enabled: each measured source retains its original contents and
repository-relative name. The report and trusted config remain outside the scan.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

from codecov_suites import COVERAGE_EXCLUDE_REGEXP, SUITES


UPLOAD_TIMEOUT_SECONDS = 15 * 60


class UploadError(Exception):
    """An upload cannot be prepared without changing or broadening its scope."""


def clean_environment(environ: dict[str, str]) -> dict[str, str]:
    # Git discovery must depend on cwd, not an inherited GIT_DIR/config override.
    return {key: value for key, value in environ.items() if not key.startswith("GIT_")}


def git(directory: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "--literal-pathspecs", "-C", str(directory), *args],
        env=clean_environment(os.environ), capture_output=True, timeout=30,
    )


def source_files(report: Path, source_root: Path, suite: str) -> dict[str, Path]:
    """Validate every SF record before copying any source or invoking Codecov."""
    source_root = source_root.resolve(strict=True)
    checkout = git(source_root, "rev-parse", "--show-toplevel")
    if checkout.returncode or Path(os.fsdecode(checkout.stdout).strip()).resolve() != source_root:
        raise UploadError("source root must be the Git checkout root")

    files = {}
    with report.open(encoding="utf-8") as stream:
        for line in stream:
            if not line.startswith("SF:"):
                continue
            name = line[3:].rstrip("\r\n")
            if (not name or "\\" in name or any(ord(char) < 32 or ord(char) == 127 for char in name)
                    or any(part == ".." or part.casefold() == ".git" for part in name.split("/"))):
                raise UploadError("LCOV contains an invalid source path")
            path = Path(name)
            if not path.is_absolute():
                path = source_root / path
            try:
                relative = path.relative_to(source_root).as_posix()
            except ValueError as exc:
                raise UploadError("LCOV source is outside the checkout") from exc
            if (not any(relative.startswith(prefix) for prefix in SUITES[suite]["lcov_prefixes"])
                    or re.search(COVERAGE_EXCLUDE_REGEXP, relative)):
                raise UploadError(f"LCOV source is outside suite {suite}: {relative}")
            if not path.is_file() or path.resolve(strict=True) != path:
                raise UploadError(f"LCOV source is missing, not regular, or uses a symlink: {relative}")
            files[relative] = path
    if not files:
        raise UploadError("LCOV contains no source files")

    # Check only the selected paths; never publish the full Git index/network.
    names = sorted(files)
    tracked = set()
    for start in range(0, len(names), 128):
        result = git(source_root, "ls-files", "--stage", "-z", "--", *names[start:start + 128])
        if result.returncode:
            raise UploadError("could not validate tracked LCOV sources")
        for record in result.stdout.split(b"\0"):
            if not record:
                continue
            metadata, name = record.split(b"\t", 1)
            mode, _, stage = metadata.split()
            if mode not in {b"100644", b"100755"} or stage != b"0":
                raise UploadError("LCOV sources must be regular tracked files without conflicts")
            tracked.add(os.fsdecode(name))
    if tracked != set(files):
        raise UploadError("LCOV contains an untracked source file")
    return {name: files[name] for name in names}


def stage_sources(files: dict[str, Path], directory: Path) -> None:
    for name, source in files.items():
        destination = directory / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)


def safe_output(text: str) -> str:
    # Codecov exceptions can contain signed storage URLs. Keep the host/path,
    # but never echo a query signature or the repository upload token.
    text = re.sub(r"(https?://[^\s<>\"'?]+)\?[^\s<>\"']+", r"\1?[REDACTED]", text)
    token = os.environ.get("CODECOV_TOKEN", "")
    return text.replace(token, "[REDACTED]") if token else text


def annotation(message: str) -> None:
    text = safe_output(message).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::error title=Codecov publication::{text}", flush=True)


def upload(args: argparse.Namespace) -> int:
    source_root = args.source_root.resolve(strict=True)
    report = args.report.resolve(strict=True)
    config = (args.config or source_root / ".github/codecov.yml").resolve(strict=True)
    if not config.is_file():
        raise UploadError("Codecov config is not a regular file")
    files = source_files(report, source_root, args.suite)
    executable = shutil.which(args.cli)
    if not executable:
        raise UploadError("Codecov CLI is unavailable")
    executable = str(Path(executable).resolve(strict=True))
    cli_args = args.upload_args
    if cli_args[:1] == ["--"]:
        cli_args = cli_args[1:]
    if not cli_args:
        raise UploadError("Codecov commit/flag metadata arguments are missing")
    # These options are owned here, so another report/root cannot be appended.
    reserved = {"--file", "-f", "--files-search-direct-file", "--coverage-files-search-direct-file",
                "--network-root-folder", "--network-filter", "--network-prefix", "--plugin",
                "--disable-file-fixes", "--disable-search", "--recurse-submodules"}
    if any(arg.split("=", 1)[0] in reserved or arg.startswith("-f") for arg in cli_args):
        raise UploadError("report selection and source metadata options are managed by this helper")

    with tempfile.TemporaryDirectory(prefix="codecov-upload-", dir=args.temp_dir) as temporary:
        directory = Path(temporary).resolve()
        if git(directory, "rev-parse", "--git-dir").returncode == 0:
            raise UploadError("upload temporary directory must be outside every Git repository")
        stage_sources(files, directory)
        print(f"Codecov upload: {len(files)} measured source files only; "
              f"LCOV unchanged ({report.stat().st_size} bytes); file fixes enabled.", flush=True)
        command = [
            executable, "--disable-telem", "--codecov-yml-path", str(config),
            "upload-coverage", *cli_args,
            "--file", str(report), "--disable-search", "--plugin", "noop",
            "--network-root-folder", str(directory),
        ]
        started = time.monotonic()
        expired = False
        payload_bytes = None
        timed_out = threading.Event()
        with subprocess.Popen(
            command, cwd=directory, env=clean_environment(os.environ),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            encoding="utf-8", errors="replace", start_new_session=True,
        ) as process:
            def stop_upload_processes() -> None:
                try:
                    # Kill descendants too: they may keep stdout open after
                    # the CLI exits and prevent the reader from reaching EOF.
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

            def expire() -> None:
                timed_out.set()
                stop_upload_processes()

            # A timeout on wait() alone cannot interrupt a blocked stdout read.
            watchdog = threading.Timer(UPLOAD_TIMEOUT_SECONDS, expire)
            watchdog.daemon = True
            watchdog.start()
            try:
                for line in process.stdout:
                    expired = expired or "ExpiredToken" in line
                    size = re.search(r"Sending upload \((\d+) bytes\)", line)
                    if size:
                        payload_bytes = int(size[1])
                    # A killed CLI can leave a partial last line. The GitHub
                    # error annotation below must start on its own line.
                    print(safe_output(line), end="" if line.endswith("\n") else "\n", flush=True)
                code = process.wait()
            except BaseException:
                # Popen.__exit__ waits for the child, including when streaming
                # fails. Stop it before cancelling the watchdog below.
                stop_upload_processes()
                raise
            finally:
                watchdog.cancel()
                watchdog.join()
        elapsed = time.monotonic() - started
        if timed_out.is_set():
            code = 124
            reason = f"Codecov CLI timed out after {UPLOAD_TIMEOUT_SECONDS:g}s; upload processes stopped"
        else:
            reason = "ExpiredToken: Codecov storage rejected an expired upload URL" if expired else f"CLI exit code {code}"
        if code:
            annotation(f"Coverage report is ready, but publication failed: {reason}. Tests were not rerun.")
        summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary, "a", encoding="utf-8") as stream:
                stream.write(f"### Codecov publication: {args.suite}\n\n"
                             f"- Sources: {len(files)} measured files; no repository-wide scan.\n"
                             f"- LCOV: {report.stat().st_size} bytes, unchanged; file fixes enabled.\n"
                             f"- Upload payload: {payload_bytes if payload_bytes is not None else 'not reported'} bytes.\n"
                             f"- Duration: {elapsed:.1f}s.\n"
                             f"- Result: {'failed — ' + reason if code else 'success'}.\n\n")
        return code if code >= 0 else 128 - code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path.cwd())
    parser.add_argument("--suite", required=True, choices=sorted(SUITES))
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--temp-dir", type=Path)
    parser.add_argument("--cli", default="codecovcli")
    parser.add_argument("upload_args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        return upload(args)
    except (UploadError, OSError, ValueError, subprocess.SubprocessError) as exc:
        annotation(f"Could not prepare or send the unchanged coverage report: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
