#!/usr/bin/env python3
"""
Fetch CI artifacts from a Run-tests index URL and build the tests resource dashboard.

Usage:
  python fetch_and_build_dashboard.py --url "https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb/Run-tests/22821461353/ya-main-x86-64/index.html"
  python fetch_and_build_dashboard.py --url "..." --repo-root /path/to/ydb --try 1

- Parses the index page to find try_1, try_2, ... directories.
- For each try_N, fetches the try index and downloads report.json and ya_evlog.jsonl.
- Saves artifacts and generated dashboard to ~/<job_type>/<run_id>/try_N/
  (job_type from URL: Run-tests, PR-check, etc.; e.g. ~/PR-check/22803868282/try_1/)
- Runs tests_resource_dashboard.py to produce dashboard.html and related outputs.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

if sys.version_info >= (3, 9):
    from urllib.request import Request, urlopen
else:
    from urllib.request import Request, urlopen

# Try to use certifi for HTTPS on older Pythons; optional
try:
    import ssl
    import certifi
    _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except Exception:
    _SSL_CTX = None

SCRIPT_DIR = Path(__file__).resolve().parent
DASHBOARD_SCRIPT = SCRIPT_DIR / "tests_resource_dashboard.py"


def _default_repo_root() -> Path:
    """Infer YDB repo root from script location so ya.make (REQUIREMENTS/SPLIT_FACTOR) is found when run from any cwd."""
    p = SCRIPT_DIR.resolve()
    for _ in range(10):
        if (p / "ydb").is_dir() and (p / ".github").is_dir():
            return p
        if p.parent == p:
            break
        p = p.parent
    return Path.cwd()

# Markdown-style: [ try_1 ](try_1/index.html) or [try_1](https://.../try_1/index.html)
LINK_RE = re.compile(r'\]\s*\(\s*([^)\s]+)\s*\)')
# HTML-style: <a href="try_1/"> or href='try_1/index.html'
HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)
TRY_DIR_RE = re.compile(r'^try_(\d+)/?$')
# Fallback: find try_N in page text (e.g. "try_1", "try_2")
TRY_IN_TEXT_RE = re.compile(r'try_(\d+)')


def fetch_url(url: str, timeout: int = 120) -> bytes:
    req = Request(url, headers={"User-Agent": "YDB-Dashboard-Fetcher/1.0"})
    ctx = _SSL_CTX
    with urlopen(req, timeout=timeout, context=ctx) as resp:
        return resp.read()


def fetch_text(url: str, timeout: int = 120) -> str:
    return fetch_url(url, timeout=timeout).decode("utf-8", errors="replace")


def parse_index_links(html: str, base_url: str) -> list[str]:
    """Extract hrefs from markdown [ text ](href) and HTML href="...". Returns absolute URLs."""
    seen: set[str] = set()
    links: list[str] = []
    base = base_url.rstrip("/") + "/"
    for m in LINK_RE.finditer(html):
        href = m.group(1).strip()
        if not href or href == ".." or href.endswith("/../") or href == "index.html":
            continue
        full = urljoin(base, href)
        if full not in seen:
            seen.add(full)
            links.append(full)
    for m in HREF_RE.finditer(html):
        href = m.group(1).strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        if href == ".." or href == "../" or href == "index.html":
            continue
        full = urljoin(base, href)
        if full not in seen:
            seen.add(full)
            links.append(full)
    return links


def extract_try_dirs(links: list[str], base_url: str, html: str = "") -> list[tuple[int, str]]:
    """From list of absolute URLs, return (N, try_N_base_url) for each try_N directory."""
    out: list[tuple[int, str]] = []
    seen: set[str] = set()
    base = base_url.rstrip("/") + "/"
    for u in links:
        parsed = urlparse(u)
        path = unquote(parsed.path).rstrip("/")
        parts = [p for p in path.split("/") if p]
        if not parts:
            continue
        # URL can end with try_1/index.html, try_1/, or try_1
        name = parts[-2] if len(parts) >= 2 and parts[-1].startswith("index.") else parts[-1]
        if name.startswith("index."):
            continue
        m = TRY_DIR_RE.match(name)
        if m and name not in seen:
            seen.add(name)
            n = int(m.group(1))
            out.append((n, base + name))
    if out:
        return sorted(out, key=lambda x: x[0])
    # Fallback: find try_N in page text (e.g. listing without parseable links)
    for m in TRY_IN_TEXT_RE.finditer(html):
        n = int(m.group(1))
        name = f"try_{n}"
        if name not in seen:
            seen.add(name)
            out.append((n, base + name))
    return sorted(out, key=lambda x: x[0])


def find_report_evlog_resources(html: str, try_base_url: str) -> tuple[str | None, str | None, str | None]:
    """From try index HTML, return (report_url, evlog_url, resources_url) if found. resources_url may be None."""
    links = parse_index_links(html, try_base_url)
    report_url = None
    evlog_url = None
    resources_url = None
    for u in links:
        name = urlparse(u).path.split("/")[-1].lower()
        if name == "report.json":
            report_url = u
        if name == "ya_evlog.jsonl" or name == "ya_evlog.jsonl.json":
            if name == "ya_evlog.jsonl":
                evlog_url = u
            elif evlog_url is None:
                evlog_url = u
        if name == "resources_monitor.jsonl":
            resources_url = u
    return report_url, evlog_url, resources_url


def download_file(url: str, dest: Path, timeout: int = 300) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    data = fetch_url(url, timeout=timeout)
    dest.write_bytes(data)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch CI Run-tests artifacts by index URL and build tests resource dashboard.",
    )
    ap.add_argument(
        "url",
        nargs="?",
        default=None,
        help="URL of the Run-tests index.html (e.g. .../Run-tests/22821461353/ya-main-x86-64/index.html)",
    )
    ap.add_argument(
        "--url",
        dest="url_flag",
        default=None,
        help="Same as positional URL.",
    )
    ap.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Path to YDB repo root (for ya.make REQUIREMENTS / SPLIT_FACTOR). Default: inferred from script path.",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Base output directory. Default: ~/<job_type>/<run_id> (e.g. ~/PR-check/22803868282).",
    )
    ap.add_argument(
        "--try",
        dest="try_num",
        type=int,
        default=None,
        metavar="N",
        help="Process only try_N (e.g. 1). Default: all try_* found.",
    )
    ap.add_argument(
        "--sanitizer",
        type=str,
        default=None,
        help="SANITIZER_TYPE for ya.make (e.g. address, thread). Auto-detected from URL (asan/tsan/msan in path) or report if omitted.",
    )
    ap.add_argument(
        "--top-n",
        type=int,
        default=500,
        help="Top N suites for dashboard. Passed to tests_resource_dashboard.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be downloaded and run, do not fetch or run.",
    )
    args = ap.parse_args()

    url = args.url or args.url_flag
    if not url:
        ap.error("Provide index URL as argument or --url")

    url = url.strip().rstrip("/")
    if not url.endswith("index.html"):
        url = url + "/index.html" if not url.endswith("/") else url + "index.html"

    # Extract job_type, run_id, and optional sanitizer from path: .../<job_type>/<run_id>/<config>/...
    # e.g. PR-check/22803868282/ya-x86-64-asan -> sanitizer "address" from "asan"
    parsed = urlparse(url)
    path_parts = [p for p in parsed.path.split("/") if p]
    run_id = None
    job_type = "Run-tests"
    run_id_index = None
    for i, p in enumerate(path_parts):
        if p.isdigit() and i > 0:
            run_id = p
            job_type = path_parts[i - 1]
            run_id_index = i
            break
    if not run_id:
        run_id = "unknown_run"

    sanitizer_from_url = None
    if run_id_index is not None and run_id_index + 1 < len(path_parts):
        config_segment = path_parts[run_id_index + 1].lower()
        if "asan" in config_segment:
            sanitizer_from_url = "address"
        elif "tsan" in config_segment:
            sanitizer_from_url = "thread"
        elif "msan" in config_segment:
            sanitizer_from_url = "memory"

    base_url = url.rsplit("/", 1)[0]

    repo_root = (args.repo_root.resolve() if args.repo_root else _default_repo_root())
    out_base = args.out_dir or (Path.home() / job_type / run_id)
    out_base = out_base.resolve()

    effective_sanitizer = args.sanitizer or sanitizer_from_url
    if args.dry_run:
        print(f"URL: {url}")
        print(f"Job type: {job_type}")
        print(f"Run ID: {run_id}")
        if sanitizer_from_url:
            print(f"Sanitizer (from URL): {sanitizer_from_url}")
        print(f"Base URL: {base_url}")
        print(f"Output base: {out_base}")
        print(f"Repo root: {repo_root}")

    # Fetch main index and find try_* dirs
    try:
        index_html = fetch_text(url)
    except Exception as e:
        print(f"Failed to fetch index: {e}", file=sys.stderr)
        sys.exit(1)

    links = parse_index_links(index_html, base_url)
    try_dirs = extract_try_dirs(links, base_url, index_html)

    if args.try_num is not None:
        try_dirs = [(n, u) for n, u in try_dirs if n == args.try_num]
        if not try_dirs:
            print(f"No try_{args.try_num} found.", file=sys.stderr)
            sys.exit(1)

    if not try_dirs:
        print("No try_* directories found in index.", file=sys.stderr)
        sys.exit(1)

    for try_n, try_base in try_dirs:
        try_name = f"try_{try_n}"
        try_index_url = urljoin(try_base + "/", "index.html")
        if args.dry_run:
            print(f"\nWould process {try_name}: {try_index_url}")
            continue

        try:
            try_html = fetch_text(try_index_url)
        except Exception as e:
            print(f"Failed to fetch {try_name} index: {e}", file=sys.stderr)
            continue

        report_url, evlog_url, resources_url = find_report_evlog_resources(try_html, try_base)
        if not report_url or not evlog_url:
            print(f"{try_name}: missing report.json or ya_evlog.jsonl (report={report_url!s}, evlog={evlog_url!s})", file=sys.stderr)
            continue

        out_dir = out_base / try_name
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "report.json"
        evlog_filename = "ya_evlog.jsonl.json" if evlog_url.rstrip("/").endswith(".json") else "ya_evlog.jsonl"
        evlog_path = out_dir / evlog_filename

        print(f"Downloading {try_name} report.json ...")
        download_file(report_url, report_path)
        print(f"Downloading {try_name} evlog ...")
        download_file(evlog_url, evlog_path)

        resources_path = None
        if resources_url:
            resources_path = out_dir / "resources_monitor.jsonl"
            print(f"Downloading {try_name} resources_monitor.jsonl ...")
            download_file(resources_url, resources_path)

        out_html = out_dir / "dashboard.html"
        cmd = [
            sys.executable,
            str(DASHBOARD_SCRIPT),
            "--report", str(report_path),
            "--evlog", str(evlog_path),
            "--out-html", str(out_html),
            "--repo-root", str(repo_root),
            "--top-n", str(args.top_n),
        ]
        if resources_path is not None:
            cmd += ["--resources-jsonl", str(resources_path)]
        if effective_sanitizer:
            cmd += ["--sanitizer", effective_sanitizer]

        print(f"Running dashboard for {try_name} ...")
        rc = subprocess.run(cmd, cwd=repo_root)
        if rc.returncode != 0:
            print(f"Dashboard script exited with {rc.returncode} for {try_name}", file=sys.stderr)
            continue
        print(f"Done: {out_html}")

    if not args.dry_run:
        print(f"\nOutputs under: {out_base}")


if __name__ == "__main__":
    main()
