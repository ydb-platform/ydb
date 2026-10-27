#!/usr/bin/env python3
"""Check the original PR patch against the exact LCOV sent by its suite job.

Do not use Codecov's cached per-flag comparison totals here: those can use a
different base from the PR patch (including after Codecov adjusts its project
baseline). Commit/flag annotations also merge earlier uploads at the same SHA.
Use the receipt-bound LCOV instead, retaining Codecov's LCOV branch/partial
semantics without combining uploads or trusting an adjusted comparison.

``collect`` runs in the trusted detect job, where the complete Git history is
already available. ``check`` runs offline against trusted CI artifacts, without
checking out PR code. Neither Codecov processing nor a main baseline is needed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from codecov_suites import COVERAGE_EXCLUDE_REGEXP, SUITES

MAX_FILES = 1000
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_RECEIPT_BYTES = 4096
MAX_LCOV_BYTES = 64 * 1024 * 1024
MAX_POLICY_BYTES = 128 * 1024
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
EXCLUDE_RE = re.compile(COVERAGE_EXCLUDE_REGEXP)


class CoverageError(Exception):
    """A report or input cannot safely be used for a coverage verdict."""


def sha(value: str) -> str:
    if not SHA_RE.fullmatch(value):
        raise argparse.ArgumentTypeError("expected a full lowercase 40-character SHA")
    return value


def positive(value: str) -> int:
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return result


def percentage(value: str) -> Decimal:
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("expected a percentage between 0 and 100") from exc
    if not result.is_finite() or not 0 <= result <= 100:
        raise argparse.ArgumentTypeError("expected a percentage between 0 and 100")
    return result


@dataclass(frozen=True)
class PatchPolicy:
    target: Decimal
    tolerance: Decimal

    @property
    def minimum(self) -> Decimal:
        return max(Decimal(0), self.target - self.tolerance)

    def description(self) -> str:
        return (
            f"Policy from .github/codecov.yml: target {self.target}%, "
            f"tolerance {self.tolerance} percentage points; minimum {self.minimum}%"
        )


def load_patch_policy(path: Path, flag: str) -> PatchPolicy:
    """Read the trusted Codecov YAML's JSON form, never a second threshold.

    The workflow uses Ruby's safe YAML loader (already used by helper CI) so
    this offline checker needs no additional Python packages. Missing, auto or
    ambiguous patch targets are errors, not a fallback to a hardcoded value.
    """
    try:
        with path.open("rb") as source:
            raw = source.read(MAX_POLICY_BYTES + 1)
        if len(raw) > MAX_POLICY_BYTES:
            raise CoverageError("patch coverage policy exceeds the safety limit")
        config = json.loads(raw)
        flags = config["flag_management"]["individual_flags"]
        if not isinstance(flags, list) or any(not isinstance(item, dict) for item in flags):
            raise ValueError("invalid flags")
        matches = [item for item in flags if item.get("name") == flag]
        if len(matches) != 1:
            raise ValueError("missing or duplicate flag")
        statuses = matches[0]["statuses"]
        if not isinstance(statuses, list) or any(not isinstance(item, dict) for item in statuses):
            raise ValueError("invalid statuses")
        patches = [item for item in statuses if item.get("type") == "patch"]
        if len(patches) != 1:
            raise ValueError("missing or duplicate patch status")

        def configured_percentage(name: str) -> Decimal:
            value = patches[0][name]
            if type(value) not in (str, int, float):
                raise ValueError("invalid percentage type")
            text = str(value).strip()
            return percentage(text[:-1] if text.endswith("%") else text)

        return PatchPolicy(configured_percentage("target"), configured_percentage("threshold"))
    except (OSError, ValueError, KeyError, TypeError, argparse.ArgumentTypeError) as exc:
        raise CoverageError(f"could not read an unambiguous numeric patch policy for {flag}") from exc


def owned_path(path: str, flag: str | None = None) -> bool:
    if not isinstance(path, str) or "\x00" in path:
        return False
    if path.startswith("/") or any(part in (".", "..") for part in path.split("/")):
        return False
    suites = [SUITES[flag]] if flag else SUITES.values()
    return not EXCLUDE_RE.search(path) and any(
        path.startswith(prefix) for suite in suites for prefix in suite["lcov_prefixes"]
    )


def git(*args: str) -> bytes:
    try:
        return subprocess.run(
            ["git", "--no-pager", *args], check=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=120,
        ).stdout
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
        raise CoverageError("could not collect the pinned Git patch") from exc


def collect(base: str, head: str) -> dict:
    # Never invoke a repository-defined external diff/textconv command.
    common = ["diff", "--no-ext-diff", "--no-textconv", "--find-renames", "-l0", "--no-color"]
    names = git(*common, "--name-status", "--diff-filter=AMRT", "-z", f"{base}...{head}", "--")
    tokens = names.split(b"\0")
    if tokens[-1] != b"":
        raise CoverageError("Git returned a malformed NUL-delimited path list")
    tokens.pop()
    files = {}
    merge_base = None
    index = 0
    while index < len(tokens):
        status = tokens[index]
        count = 2 if status.startswith(b"R") else 1
        raw_paths = tokens[index + 1 : index + 1 + count]
        if len(raw_paths) != count or not re.fullmatch(rb"[AMT]|R\d+", status):
            raise CoverageError("Git returned an unexpected change status")
        index += count + 1
        try:
            paths = [raw_path.decode("utf-8") for raw_path in raw_paths]
        except UnicodeDecodeError as exc:
            raise CoverageError("the PR contains a non-UTF-8 path") from exc
        path = paths[-1]
        if not owned_path(path):
            continue
        # Isolate renamed blobs: a path-scoped diff including both names can
        # also include a different file newly occupying the previous name.
        # Pure renames yield no hunks; edited renames measure only new lines.
        if status.startswith(b"R"):
            if merge_base is None:
                merge_base = git("merge-base", base, head).decode("ascii").strip()
                if not SHA_RE.fullmatch(merge_base):
                    raise CoverageError("Git returned an invalid merge base")
            patch_args = [f"{merge_base}:{paths[0]}", f"{head}:{path}", "--"]
        else:
            patch_args = [f"{base}...{head}", "--", f":(literal){path}"]
        patch = git(
            *common, "--unified=0", "--inter-hunk-context=0", *patch_args,
        ).decode("utf-8", errors="replace")
        ranges = []
        for line in patch.splitlines():
            match = HUNK_RE.match(line)
            if match:
                start = int(match.group(1))
                count = int(match.group(2) or "1")
                if count:
                    ranges.append([start, start + count - 1])
        if ranges:
            files[path] = ranges
    if len(files) > MAX_FILES:
        raise CoverageError(f"patch contains more than {MAX_FILES} owned files; refusing to truncate it")
    return {"version": 1, "base_sha": base, "head_sha": head, "files": files}


def load_changed_lines(path: Path, base: str, head: str, flag: str) -> dict[str, list[list[int]]]:
    try:
        with path.open("rb") as source:
            raw = source.read(MAX_MANIFEST_BYTES + 1)
        if len(raw) > MAX_MANIFEST_BYTES:
            raise CoverageError("patch manifest exceeds the safety limit")
        data = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise CoverageError("could not read the trusted patch manifest") from exc
    if not isinstance(data, dict) or data.get("version") != 1:
        raise CoverageError("unsupported patch manifest")
    if data.get("head_sha") != head or data.get("base_sha") != base:
        raise CoverageError("patch manifest does not match the pinned base and HEAD")
    files = data.get("files")
    if not isinstance(files, dict) or len(files) > MAX_FILES:
        raise CoverageError("invalid or oversized patch manifest")
    result = {}
    for name, ranges in files.items():
        if not owned_path(name) or not isinstance(ranges, list) or not ranges:
            raise CoverageError("invalid file/ranges in patch manifest")
        previous_end = 0
        for bounds in ranges:
            if (
                not isinstance(bounds, list) or len(bounds) != 2
                or any(type(value) is not int for value in bounds)
                or not previous_end < bounds[0] <= bounds[1] <= 2**31 - 1
            ):
                raise CoverageError("invalid or overlapping line ranges in patch manifest")
            previous_end = bounds[1]
        if owned_path(name, flag):
            result[name] = ranges
    return result


@dataclass(frozen=True)
class UploadReceipt:
    name: str
    source_root: str
    report_sha256: str


def load_upload_receipt(path: Path, head: str, flag: str, run_id: int) -> UploadReceipt:
    """Bind the local report to the successful suite job, not a merged API view.

    The trusted artifact records the upload identity and preserved LCOV digest.
    Successful suites can retain their previous-attempt artifact when GitHub
    reruns only failed jobs. Codecov processing is deliberately not consulted.
    """
    try:
        with path.open("rb") as source:
            raw = source.read(MAX_RECEIPT_BYTES + 1)
        if len(raw) > MAX_RECEIPT_BYTES:
            raise CoverageError("upload receipt exceeds the safety limit")
        receipt = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise CoverageError("could not read the trusted upload receipt") from exc
    if not isinstance(receipt, dict) or type(receipt.get("version")) is not int or receipt["version"] != 2:
        raise CoverageError("unsupported upload receipt")
    if receipt.get("head_sha") != head or receipt.get("flag") != flag or receipt.get("run_id") != str(run_id):
        raise CoverageError("upload receipt does not match the pinned HEAD, flag and workflow run")
    attempt = receipt.get("run_attempt")
    if not isinstance(attempt, str) or not re.fullmatch(r"[1-9][0-9]*", attempt):
        raise CoverageError("invalid attempt in upload receipt")
    expected = f"{flag}-{run_id}-{attempt}"
    if receipt.get("upload_name") != expected:
        raise CoverageError("upload receipt contains an inconsistent upload name")
    root, digest = receipt.get("source_root"), receipt.get("report_sha256")
    if (
        not isinstance(root, str) or not root.startswith("/") or root.endswith("/")
        or any(part in ("", ".", "..") for part in root.split("/")[1:])
        or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
    ):
        raise CoverageError("upload receipt has an invalid source root or report digest")
    return UploadReceipt(expected, root, digest)


def load_lcov(path: Path, receipt: UploadReceipt, flag: str) -> dict:
    """Read our LLVM LCOV dialect; malformed/duplicate records fail closed.

    Match Codecov's services/report/languages/lcov.py: BRDA replaces DA on a
    branch line; zero/some/all taken branches mean M/P/H. Mangled function
    starts in .cpp files keep DA instead of branch data, as in that parser.
    No session, attempt or other file is ever merged into this report.
    """
    try:
        with path.open("rb") as source:
            raw = source.read(MAX_LCOV_BYTES + 1)
        if len(raw) > MAX_LCOV_BYTES:
            raise CoverageError("LCOV report exceeds the safety limit")
        if hashlib.sha256(raw).hexdigest() != receipt.report_sha256:
            raise CoverageError("LCOV report does not match the upload receipt digest")
        text = raw.decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise CoverageError("could not read the exact uploaded LCOV report") from exc

    def number(value: str) -> int:
        if not re.fullmatch(r"[0-9]+", value):
            raise CoverageError("invalid LLVM LCOV integer")
        return int(value)

    def line_number(value: str) -> int:
        result = number(value)
        if not 0 < result <= 2**31 - 1:
            raise CoverageError("invalid LLVM LCOV line number")
        return result

    reports = {}
    name = None
    lines, branches, skipped = {}, {}, set()
    found = None
    for row in text.splitlines():
        if row == "end_of_record":
            if name is None or found is None:
                raise CoverageError("incomplete LLVM LCOV record")
            kinds = {line: "H" if hits else "M" for line, hits in lines.items()}
            for line, outcomes in branches.items():
                if line not in skipped:
                    hits = sum(outcomes.values())
                    kinds[line] = "M" if hits == 0 else "H" if hits == len(outcomes) else "P"
            reports[name] = {
                "totals": {"lineCount": len(kinds)},
                "coverage": [{"line": line, "coverage": kind} for line, kind in sorted(kinds.items())],
            }
            name = None
            lines, branches, skipped = {}, {}, set()
            found = None
            continue
        if not row or row.startswith("TN:"):
            continue
        tag, separator, value = row.partition(":")
        if not separator:
            raise CoverageError("malformed LLVM LCOV row")
        if tag == "SF":
            prefix = receipt.source_root + "/"
            relative = value[len(prefix):] if value.startswith(prefix) else value
            if name is not None or not owned_path(relative, flag) or relative in reports:
                raise CoverageError("unexpected or duplicate source in LLVM LCOV")
            name = relative
            continue
        if name is None:
            raise CoverageError("LLVM LCOV data outside a source record")
        if tag == "DA":
            values = value.split(",")
            if len(values) not in (2, 3):
                raise CoverageError("malformed LLVM LCOV DA row")
            line, hits = line_number(values[0]), number(values[1])
            if line in lines:
                raise CoverageError("duplicate LLVM LCOV DA line")
            lines[line] = hits
        elif tag == "BRDA":
            values = value.split(",")
            if len(values) != 4:
                raise CoverageError("malformed LLVM LCOV BRDA row")
            line = line_number(values[0])
            key = (number(values[1]), number(values[2]))
            hits = 0 if values[3] == "-" else number(values[3])
            outcomes = branches.setdefault(line, {})
            if key in outcomes:
                raise CoverageError("duplicate LLVM LCOV branch")
            outcomes[key] = hits > 0
        elif tag == "FN":
            start, comma, function = value.partition(",")
            if not comma or not function:
                raise CoverageError("malformed LLVM LCOV FN row")
            line = line_number(start)
            if name.endswith(".cpp") and function.startswith(("_Z", "_G")):
                skipped.add(line)
        elif tag == "LF":
            if found is not None:
                raise CoverageError("duplicate LLVM LCOV LF total")
            # LLVM's summary can count template instantiations differently
            # from its DA rows. Codecov also ignores LF when classifying lines.
            found = number(value)
        elif tag not in ("FNDA", "FNF", "FNH", "LH", "BRF", "BRH"):
            raise CoverageError("unsupported LLVM LCOV row")
    if name is not None or not reports or not any(r["coverage"] for r in reports.values()):
        raise CoverageError("empty or truncated LLVM LCOV report")
    return reports


@dataclass
class Totals:
    hits: int = 0
    misses: int = 0
    partials: int = 0

    @property
    def lines(self) -> int:
        return self.hits + self.misses + self.partials

    def add_file(self, ranges: list[list[int]], report: object) -> bool:
        if not isinstance(report, dict) or not isinstance(report.get("coverage"), list):
            raise CoverageError("malformed LCOV file coverage")
        annotations = report["coverage"]
        if report.get("totals") is None:
            if annotations:
                raise CoverageError("LCOV file has annotations but no totals")
            return False
        if not isinstance(report["totals"], dict):
            raise CoverageError("invalid LCOV file totals")
        lines = report["totals"].get("lineCount")
        if type(lines) is not int or lines != len(annotations):
            raise CoverageError("LCOV file annotations are incomplete or inconsistent with its totals")
        seen = set()
        for annotation in annotations:
            if not isinstance(annotation, dict):
                raise CoverageError("invalid LCOV line annotation")
            line, kind = annotation.get("line"), annotation.get("coverage")
            if type(line) is not int or line <= 0 or line in seen or kind not in ("H", "M", "P"):
                raise CoverageError("invalid or duplicate LCOV line annotation")
            seen.add(line)
            if any(start <= line <= end for start, end in ranges):
                if kind == "H":
                    self.hits += 1
                elif kind == "M":
                    self.misses += 1
                else:
                    self.partials += 1
        return True

    def passes(self, threshold: Decimal) -> bool:
        return self.lines == 0 or Decimal(100 * self.hits) >= threshold * self.lines

    def description(self, flag: str, threshold: Decimal) -> str:
        if not self.lines:
            return f"{flag}: not applicable (no changed lines measured in the uploaded LCOV)"
        percent = Decimal(100 * self.hits) / self.lines
        verdict = "PASS" if self.passes(threshold) else "FAIL"
        return (
            f"{flag}: {verdict}, patch coverage {percent:.5f}% (target {threshold}%), "
            f"{self.hits}/{self.lines} covered; {self.misses} missed, {self.partials} partial"
        )


def check(args: argparse.Namespace) -> int:
    policy = load_patch_policy(args.policy_file, args.flag)
    files = load_changed_lines(args.changed_lines_file, args.base_sha, args.head_sha, args.flag)
    receipt = load_upload_receipt(args.upload_receipt, args.head_sha, args.flag, args.run_id)
    reports = load_lcov(args.coverage_report, receipt, args.flag)
    totals = Totals()
    absent = []
    for path in sorted(files):
        if not totals.add_file(files[path], reports.get(path, {"totals": None, "coverage": []})):
            absent.append(path)
    description = totals.description(args.flag, policy.minimum)
    print(description)
    print(policy.description())
    print(f"Measured HEAD {args.head_sha}; original PR base {args.base_sha}; upload {receipt.name}")
    print(f"Exact uploaded LCOV SHA-256: {receipt.report_sha256}")
    if absent:
        print("Files absent from the exact uploaded LCOV (not counted as covered):")
        for path in absent:
            print(json.dumps(path))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as output:
            output.write(f"### Patch coverage: {args.flag}\n\n{description}\n\n")
            output.write(policy.description() + "\n\n")
            output.write(f"Base `{args.base_sha}` → HEAD `{args.head_sha}`.\n\n")
            output.write(f"Upload `{receipt.name}`; LCOV SHA-256 `{receipt.report_sha256}`.\n\n")
            output.write("Uses the exact uploaded LCOV and Codecov's branch/partial semantics for the original PR diff; partial lines are not hits.\n\n")
            output.write("Calculated offline; Codecov processing and main baseline availability do not affect this verdict.\n\n")
            if absent:
                output.write("Files absent from that LCOV (excluded from the denominator, not assumed covered):\n\n")
                for path in absent:
                    output.write(f"- {json.dumps(path).replace('<', '&lt;').replace('>', '&gt;')}\n")
                output.write("\n")
    return 0 if totals.passes(policy.minimum) else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect", help="collect the pinned PR patch in the trusted detect job")
    collect_parser.add_argument("--base-sha", type=sha, required=True)
    collect_parser.add_argument("--head-sha", type=sha, required=True)
    collect_parser.add_argument("--output", type=Path, required=True)
    check_parser = subparsers.add_parser("check", help="check one flag offline using the exact uploaded LCOV")
    check_parser.add_argument("--base-sha", type=sha, required=True)
    check_parser.add_argument("--head-sha", type=sha, required=True)
    check_parser.add_argument("--flag", choices=sorted(SUITES), required=True)
    check_parser.add_argument("--upload-receipt", type=Path, required=True, help="trusted artifact recording this suite job's exact upload")
    check_parser.add_argument("--coverage-report", type=Path, required=True, help="exact uploaded LCOV bound to the receipt digest")
    check_parser.add_argument("--run-id", type=positive, required=True, help="expected workflow run ID")
    check_parser.add_argument("--changed-lines-file", type=Path, required=True)
    check_parser.add_argument("--policy-file", type=Path, required=True, help="trusted .github/codecov.yml converted to JSON")
    args = parser.parse_args(argv)
    try:
        if args.command == "collect":
            result = collect(args.base_sha, args.head_sha)
            args.output.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
            print(f"Collected changed-line ranges for {len(result['files'])} owned files")
            return 0
        return check(args)
    except CoverageError as exc:
        reason = str(exc)[:500]
        print(f"Patch coverage measurement failed: {reason}", file=sys.stderr)
        summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if args.command == "check" and summary:
            with open(summary, "a", encoding="utf-8") as output:
                output.write(f"### Patch coverage: {args.flag}\n\nMeasurement error: {reason}\n\n")
                output.write("No coverage verdict was produced; this is not a below-threshold result.\n\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
