#!/usr/bin/env python3
"""Check the original PR patch against the exact LCOV sent by its suite job.

Do not use Codecov's cached per-flag comparison totals here: those can use a
different base from the PR patch (including after Codecov adjusts its project
baseline). Commit/flag annotations also merge earlier uploads at the same SHA.
Use the receipt-bound LCOV instead, retaining Codecov's LCOV branch/partial
semantics without combining uploads or trusting an adjusted comparison.

``collect`` runs in the trusted detect job, where the complete Git history is
already available. ``check`` runs after upload, without checking out PR code.
No Codecov notification/status or CI completion is needed to read this API.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable

from codecov_suites import COVERAGE_EXCLUDE_REGEXP, SUITES

API_URL = "https://api.codecov.io/graphql/gh"
MAX_FILES = 1000
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_RECEIPT_BYTES = 4096
MAX_LCOV_BYTES = 64 * 1024 * 1024
MAX_RESPONSE_BYTES = 32 * 1024 * 1024
MAX_UPLOAD_PAGES = 20
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
EXCLUDE_RE = re.compile(COVERAGE_EXCLUDE_REGEXP)


class CoverageError(Exception):
    """A report or input cannot safely be used for a coverage verdict."""


class NotReady(CoverageError):
    """A transient API error or an upload that is not merged yet."""


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
    """Bind polling to the exact upload produced by the successful suite job.

    Looking for the newest upload visible in Codecov is insufficient: during
    eventual consistency an older merged upload can be visible while the new
    attempt has not appeared yet. A trusted CI artifact records the expected
    identity and digest of the preserved LCOV. Successful suites can retain
    their previous-attempt artifact when GitHub reruns only failed jobs.
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


def request(query: str, variables: dict, timeout: float) -> dict:
    req = urllib.request.Request(
        API_URL,
        data=json.dumps({"query": query, "variables": variables}).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "ydb-patch-coverage/1"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        exc.close()
        if exc.code in (408, 429) or exc.code >= 500:
            raise NotReady(f"Codecov API returned HTTP {exc.code}") from exc
        raise CoverageError(f"Codecov API returned HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise NotReady("Codecov API request failed or timed out") from exc
    if len(raw) > MAX_RESPONSE_BYTES:
        raise CoverageError("Codecov API response exceeds the safety limit")
    try:
        result = json.loads(raw)
    except ValueError as exc:
        raise NotReady("Codecov API returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise CoverageError("Codecov API returned an invalid response")
    if result.get("errors"):
        # GraphQL returns HTTP 200 for processing/provider errors. Never accept
        # partial data from a response containing any such errors.
        errors = result["errors"]
        if not isinstance(errors, list) or not isinstance(errors[0], dict):
            raise CoverageError("Codecov GraphQL returned a malformed error list")
        error = errors[0]
        message = error.get("message")
        detail = json.dumps(message[:300]) if isinstance(message, str) else "unspecified error"
        extensions = error.get("extensions")
        code = extensions.get("code") if isinstance(extensions, dict) else None
        if isinstance(code, str):
            detail += f" (code {json.dumps(code[:80])})"
        raise NotReady(f"Codecov GraphQL: {detail}; partial response rejected")
    return result


def commit_from_response(response: dict, head: str) -> dict:
    try:
        repo = response["data"]["owner"]["repository"]
        if repo["__typename"] != "Repository":
            raise CoverageError("Codecov repository is not accessible")
        commit = repo["commit"]
    except (TypeError, KeyError) as exc:
        raise NotReady("Codecov has not exposed the expected commit report") from exc
    if commit is None:
        raise NotReady("Codecov has not exposed the expected commit report")
    if not isinstance(commit, dict) or commit.get("commitid") != head:
        raise CoverageError("Codecov returned a different HEAD SHA")
    return commit


UPLOAD_QUERY = """
query PatchUpload($owner: String!, $repo: String!, $head: String!, $after: String) {
  owner(username: $owner) {
    repository(name: $repo) {
      __typename
      ... on Repository {
        commit(id: $head) {
          commitid
          coverageAnalytics { flagNames totals { lineCount } }
          uploads(first: 100, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges { node { name state flags errors { totalCount } } }
          }
        }
      }
    }
  }
}
"""


class Client:
    def __init__(self, repository: str, head: str, timeout: int = 300):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise CoverageError("repository must be owner/name")
        owner, repo = repository.split("/")
        self.variables = {"owner": owner, "repo": repo, "head": head}
        self.head = head
        self.deadline = time.monotonic() + timeout

    def query(self, query: str, extra: dict | None = None) -> dict:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise CoverageError("timed out waiting for a complete Codecov report")
        return commit_from_response(
            request(query, {**self.variables, **(extra or {})}, min(30, remaining)), self.head,
        )

    def wait(self, operation: Callable[[], object]) -> object:
        while True:
            try:
                return operation()
            except NotReady as exc:
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    raise CoverageError(f"timed out: {exc}") from exc
                print(f"Waiting for Codecov: {exc}", flush=True)
                time.sleep(min(15, remaining))

    def upload_ready(self, flag: str, name: str) -> str:
        cursor = None
        seen_cursors = set()
        found = []
        for _ in range(MAX_UPLOAD_PAGES):
            commit = self.query(UPLOAD_QUERY, {"after": cursor})
            analytics = commit.get("coverageAnalytics") or {}
            if flag not in (analytics.get("flagNames") or []):
                raise NotReady(f"no processed report for flag {flag}")
            lines = (analytics.get("totals") or {}).get("lineCount")
            if type(lines) is not int or lines <= 0:
                raise NotReady("HEAD report has no measured lines")
            try:
                connection = commit["uploads"]
                for edge in connection["edges"]:
                    upload = edge["node"]
                    if upload["name"] == name:
                        found.append(upload)
                page = connection["pageInfo"]
                if not page["hasNextPage"]:
                    break
                cursor = page["endCursor"]
                if not cursor or cursor in seen_cursors:
                    raise CoverageError("invalid Codecov upload pagination")
                seen_cursors.add(cursor)
            except (TypeError, KeyError) as exc:
                raise CoverageError("malformed Codecov upload list") from exc
        else:
            raise CoverageError("too many Codecov upload pages; refusing to truncate them")
        if not found:
            raise NotReady(f"upload {name} has not appeared yet")
        for upload in found:
            if upload.get("flags") != [flag]:
                raise CoverageError(f"upload {name} does not belong exclusively to {flag}")
            if upload.get("state") == "ERROR" or (upload.get("errors") or {}).get("totalCount", 0):
                raise CoverageError(f"upload {name} has Codecov processing errors")
            if upload.get("state") != "MERGED":
                raise NotReady(f"upload {name} is {upload.get('state')}, not merged")
        return name


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
    files = load_changed_lines(args.changed_lines_file, args.base_sha, args.head_sha, args.flag)
    receipt = load_upload_receipt(args.upload_receipt, args.head_sha, args.flag, args.run_id)
    reports = load_lcov(args.coverage_report, receipt, args.flag)
    client = Client(args.repository, args.head_sha, args.timeout)
    client.wait(lambda: client.upload_ready(args.flag, receipt.name))
    totals = Totals()
    absent = []
    for path in sorted(files):
        if not totals.add_file(files[path], reports.get(path, {"totals": None, "coverage": []})):
            absent.append(path)
    description = totals.description(args.flag, args.threshold)
    print(description)
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
            output.write(f"Base `{args.base_sha}` → HEAD `{args.head_sha}`.\n\n")
            output.write(f"Upload `{receipt.name}`; LCOV SHA-256 `{receipt.report_sha256}`.\n\n")
            output.write("Uses the exact uploaded LCOV and Codecov's branch/partial semantics for the original PR diff; partial lines are not hits.\n\n")
            if absent:
                output.write("Files absent from that LCOV (excluded from the denominator, not assumed covered):\n\n")
                for path in absent:
                    output.write(f"- {json.dumps(path).replace('<', '&lt;').replace('>', '&gt;')}\n")
                output.write("\n")
    return 0 if totals.passes(args.threshold) else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect", help="collect the pinned PR patch in the trusted detect job")
    collect_parser.add_argument("--base-sha", type=sha, required=True)
    collect_parser.add_argument("--head-sha", type=sha, required=True)
    collect_parser.add_argument("--output", type=Path, required=True)
    check_parser = subparsers.add_parser("check", help="check one flag after its uniquely named upload has merged")
    check_parser.add_argument("--repository", required=True)
    check_parser.add_argument("--base-sha", type=sha, required=True)
    check_parser.add_argument("--head-sha", type=sha, required=True)
    check_parser.add_argument("--flag", choices=sorted(SUITES), required=True)
    check_parser.add_argument("--upload-receipt", type=Path, required=True, help="trusted artifact recording this suite job's exact upload")
    check_parser.add_argument("--coverage-report", type=Path, required=True, help="exact uploaded LCOV bound to the receipt digest")
    check_parser.add_argument("--run-id", type=positive, required=True, help="expected workflow run ID")
    check_parser.add_argument("--changed-lines-file", type=Path, required=True)
    check_parser.add_argument("--threshold", type=percentage, default=Decimal(80))
    check_parser.add_argument("--timeout", type=positive, default=300)
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
