#!/usr/bin/env python3
"""
Collect step summaries from all matrix jobs and generate a combined markdown table.

This script:
1. Collects GITHUB_STEP_SUMMARY files from all matrix jobs (via artifacts)
2. Parses test statistics from each summary
3. Generates a combined markdown table with branches as rows and build_presets as columns
4. Writes the combined summary to GITHUB_STEP_SUMMARY
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


TABLE_HEADER_PATTERN = r'\|\s*TESTS\s*\|\s*PASSED\s*\|\s*ERRORS\s*\|\s*FAILED\s*\|\s*SKIPPED\s*\|\s*MUTED'
URL_PATTERN = r'https://storage\.yandexcloud\.net/[^\s\)]+ya-test\.html[^\s\)]*'
DEFAULT_SUMMARY_DIR = "summary_artifacts"


def _extract_number(value: str) -> int:
    match = re.search(r'\[?(\d+)\]?', value)
    return int(match.group(1)) if match else 0


def _parse_tables(summary_text: str) -> List[Dict]:
    if not summary_text:
        return []

    lines = summary_text.split('\n')
    table_starts = [i for i, line in enumerate(lines) if re.search(TABLE_HEADER_PATTERN, line, re.IGNORECASE)]
    if not table_starts:
        return []

    tables = []
    for start_idx in table_starts:
        i = start_idx + 1
        while i < len(lines):
            line = lines[i].strip()
            if '---' in line:
                i += 1
                continue
            if line.startswith('|') and line.count('|') >= 6:
                parts = [p.strip() for p in line.split('|') if p.strip()]
                if len(parts) >= 6:
                    stats = {
                        'tests': _extract_number(parts[0]),
                        'passed': _extract_number(parts[1]),
                        'errors': _extract_number(parts[2]),
                        'failed': _extract_number(parts[3]),
                        'skipped': _extract_number(parts[4]),
                        'muted': _extract_number(parts[5]),
                    }
                    failed_cell = parts[3] if len(parts) > 3 else ''
                    tests_cell = parts[0] if len(parts) > 0 else ''
                    match = re.search(URL_PATTERN, failed_cell) or re.search(URL_PATTERN, tests_cell)
                    tables.append({'stats': stats, 'url': match.group(0) if match else None})
                break
            if i + 1 < len(lines) and re.search(TABLE_HEADER_PATTERN, lines[i + 1], re.IGNORECASE):
                break
            i += 1
    return tables


def _safe_summary_name(raw_name: str) -> str:
    # Replace colon for artifact filesystem compatibility
    return raw_name.replace(":", "__")


def _build_summary_filename(job_name: str, branch: str, build_preset: str) -> str:
    if ":" in job_name:
        return f"summary-{_safe_summary_name(job_name)}.md"
    return f"summary-{_safe_summary_name(branch)}__{_safe_summary_name(build_preset)}.md"


def save_step_summary(
    summary_path: str,
    output_dir: str,
    job_name: str,
    branch: str,
    build_preset: str,
) -> Path:
    summary_dir = Path(output_dir)
    summary_dir.mkdir(parents=True, exist_ok=True)
    filename = _build_summary_filename(job_name, branch, build_preset)
    summary_file = summary_dir / filename

    if Path(summary_path).is_file():
        summary_file.write_text(Path(summary_path).read_text(encoding="utf-8"), encoding="utf-8")
    else:
        summary_file.touch()
    return summary_file


def parse_all_attempts(summary_text: str) -> List[Dict]:
    """
    Parse all attempt tables from markdown summary.
    
    Returns: list of dicts, each containing stats and URL for one attempt (try_1, try_2, try_3, etc.)
    """
    attempts = []
    tables = _parse_tables(summary_text)
    for idx, table in enumerate(tables):
        stats = table.get('stats', {})
        url = table.get('url')
        if stats.get('tests', 0) > 0 or stats.get('failed', 0) > 0 or stats.get('errors', 0) > 0 or url:
            attempts.append({'attempt': idx + 1, 'stats': stats, 'url': url})
    return attempts


def parse_summary_markdown(summary_text: str) -> Optional[Dict]:
    """
    Parse test statistics from markdown summary.
    
    Looks for markdown table format:
    | TESTS | PASSED | ERRORS | FAILED | SKIPPED | MUTED |
    
    There can be multiple tables (for retries: try_1, try_2, try_3).
    We only take the LAST table (last attempt) for combined summary.
    
    Returns: dict with test statistics from last attempt or None if not found
    """
    tables = _parse_tables(summary_text)
    if not tables:
        return None
    stats = tables[-1].get('stats', {})
    if stats.get('tests', 0) > 0 or stats.get('failed', 0) > 0 or stats.get('errors', 0) > 0:
        return stats
    return None


def extract_test_report_url(summary_text: str) -> Optional[str]:
    """
    Extract test report URL from summary.
    Takes URL from the LAST attempt (last table) - from FAILED column if available, otherwise from TESTS column.
    """
    tables = _parse_tables(summary_text)
    if tables:
        url = tables[-1].get('url')
        if url:
            return url
    all_matches = list(re.finditer(URL_PATTERN, summary_text or ""))
    return all_matches[-1].group(0) if all_matches else None


def extract_job_info_from_name(job_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract branch and build_preset from job name.
    
    Format: "Regression-run_... (build_preset) / branch:build_preset"
    Returns: (branch, build_preset)
    """
    branch = None
    build_preset = None
    
    if ':' in job_name:
        # Format: "branch:build_preset"
        parts = job_name.split(':')
        if len(parts) >= 2:
            branch = parts[0].strip()
            build_preset = parts[1].strip()
    elif '__' in job_name:
        # Format: "branch__build_preset" (colon-safe)
        parts = job_name.split('__')
        if len(parts) >= 2:
            branch = parts[0].strip()
            build_preset = parts[1].strip()
    elif ' / ' in job_name:
        # Format: "Regression-run_... (build_preset) / branch:build_preset"
        parts = job_name.split(' / ')
        if len(parts) >= 2:
            branch_preset = parts[-1].strip()
            if ':' in branch_preset:
                branch, build_preset = branch_preset.split(':', 1)
                branch = branch.strip()
                build_preset = build_preset.strip()
            elif '__' in branch_preset:
                branch, build_preset = branch_preset.split('__', 1)
                branch = branch.strip()
                build_preset = build_preset.strip()
    
    return branch, build_preset


def collect_summaries_from_artifacts(artifacts_dir: str) -> Dict[str, Dict]:
    """
    Collect summaries from artifact files.
    
    Artifacts are downloaded with pattern "job-summary-*" and contain files like:
    "summary-{branch}:{build_preset}.md"
    
    Returns: dict mapping "branch:build_preset" to summary data
    """
    summaries = {}
    artifacts_path = Path(artifacts_dir)
    
    if not artifacts_path.exists():
        print(f"Warning: Artifacts directory {artifacts_dir} does not exist", file=sys.stderr)
        return summaries
    
    # Look for summary files - they might be in subdirectories (one per artifact)
    # Files can be named: "summary-*.md" or "{branch}:{build_preset} summary"
    summary_files = list(artifacts_path.rglob("summary-*.md"))
    # Also look for files ending with " summary" (without .md extension)
    summary_files.extend(artifacts_path.rglob("* summary"))
    
    for summary_file in summary_files:
        try:
            with open(summary_file, 'r', encoding='utf-8') as f:
                summary_text = f.read()
            
            # Extract job name from filename
            # Format: "summary-{branch}:{build_preset}.md" or " {branch}:{build_preset} summary"
            filename = summary_file.name.strip()  # Remove leading/trailing whitespace
            
            # Handle format: "summary-{branch}:{build_preset}.md"
            if filename.startswith('summary-') and filename.endswith('.md'):
                job_name = filename[8:-3].strip()  # Remove "summary-" prefix and ".md" suffix
            # Handle format: "{branch}:{build_preset} summary" (without .md extension)
            elif filename.endswith(' summary'):
                job_name = filename[:-8].strip()  # Remove " summary" suffix
            else:
                job_name = summary_file.stem.replace('summary-', '').strip()
            
            branch, build_preset = extract_job_info_from_name(job_name)
            
            if not branch or not build_preset:
                # Try to extract from filename directly
                if ':' in job_name:
                    parts = job_name.split(':', 1)
                    branch = parts[0].strip()
                    build_preset = parts[1].strip()
                elif '__' in job_name:
                    parts = job_name.split('__', 1)
                    branch = parts[0].strip()
                    build_preset = parts[1].strip()
            
            if not branch or not build_preset:
                print(f"Warning: Could not extract branch/build_preset from {job_name} (file: {summary_file})", file=sys.stderr)
                continue
            
            key = f"{branch}:{build_preset}"
            
            error = None
            if not summary_text.strip():
                error = "empty summary"

            # Parse statistics from last attempt
            stats = parse_summary_markdown(summary_text)
            test_report_url = extract_test_report_url(summary_text)
            
            # Parse all attempts for detailed table
            all_attempts = parse_all_attempts(summary_text)

            if error is None and not stats and not test_report_url and not all_attempts:
                error = "no test tables"
            
            summaries[key] = {
                'branch': branch,
                'build_preset': build_preset,
                'job_name': job_name,
                'stats': stats or {},
                'test_report_url': test_report_url,
                'all_attempts': all_attempts,  # List of all attempts with stats and URLs
                'error': error,
            }
            print(f"Collected summary for {key}", file=sys.stderr)
            
        except Exception as e:
            print(f"Warning: Error processing {summary_file}: {e}", file=sys.stderr)
            continue
    
    return summaries


def _format_error_suffix(failed: int, errors: int) -> str:
    if failed > 0 and errors > 0:
        return f" ({failed} failed, {errors} errors)"
    if failed > 0:
        return f" ({failed} failed)"
    if errors > 0:
        return f" ({errors} errors)"
    return ""


def _format_result_cell(
    total_errors: int,
    failed: int,
    errors: int,
    url: str,
    prefix: str = "",
    ok_label: str = "✓",
) -> str:
    if url:
        base = f"[{total_errors}]({url})" if total_errors > 0 else f"[{ok_label}]({url})"
    else:
        base = str(total_errors) if total_errors > 0 else ok_label

    suffix = _format_error_suffix(failed, errors) if total_errors > 0 else ""
    return f"{prefix}{base}{suffix}"


def _format_error_cell(error: str, detailed: bool) -> str:
    return f"-({error})" if detailed else f"- (error: {error})"


def _format_no_results_cell() -> str:
    return "- (no results)"


def _stats_parts(stats: Dict[str, int]) -> Tuple[int, int, int]:
    failed = stats.get('failed', 0)
    errors = stats.get('errors', 0)
    total_errors = failed + errors
    return failed, errors, total_errors


def _build_table(
    title: str,
    branches: List[str],
    presets: List[str],
    cell_for: callable,
) -> List[str]:
    lines = [title, "| Branch | " + " | ".join(presets) + " |", "|" + "---|" * (len(presets) + 1)]
    for branch in branches:
        row = [branch]
        for preset in presets:
            row.append(cell_for(branch, preset))
        lines.append("| " + " | ".join(row) + " |")
    return lines


def generate_combined_markdown_table(summaries: Dict[str, Dict]) -> str:
    """
    Generate combined markdown table.
    
    Rows: branches
    Columns: build_presets
    Cells: error counts with links (always show link, even if no errors)
    Plus detailed table with all attempts below.
    """
    if not summaries:
        return "No job summaries available.\n"
    
    # Collect all branches and build_presets
    branches = sorted(set(s['branch'] for s in summaries.values()))
    presets = sorted(set(s['build_preset'] for s in summaries.values()))
    
    if not branches or not presets:
        return "No valid job summaries found.\n"
    
    def cell_last_attempt(branch: str, preset: str) -> str:
        summary = summaries.get(f"{branch}:{preset}")
        if not summary:
            return _format_no_results_cell()
        error = summary.get('error')
        if error:
            return _format_error_cell(error, detailed=False)
        stats = summary.get('stats', {})
        failed, errors, total_errors = _stats_parts(stats)
        return _format_result_cell(total_errors, failed, errors, summary.get('test_report_url', ''))

    def cell_detailed(branch: str, preset: str) -> str:
        summary = summaries.get(f"{branch}:{preset}")
        if not summary:
            return _format_no_results_cell()
        error = summary.get('error')
        if error:
            return _format_error_cell(error, detailed=True)
        all_attempts = summary.get('all_attempts', [])
        if all_attempts:
            attempt_parts = []
            for attempt in all_attempts:
                attempt_num = attempt.get('attempt', 0)
                attempt_stats = attempt.get('stats', {})
                failed, errors, total_errors = _stats_parts(attempt_stats)
                attempt_parts.append(
                    _format_result_cell(
                        total_errors,
                        failed,
                        errors,
                        attempt.get('url', ''),
                        prefix=f"try_{attempt_num}: ",
                    )
                )
            return "<br>".join(attempt_parts)
        stats = summary.get('stats', {})
        failed, errors, total_errors = _stats_parts(stats)
        return _format_result_cell(
            total_errors,
            failed,
            errors,
            summary.get('test_report_url', ''),
            prefix="last: ",
        )

    lines = []
    lines.extend(
        _build_table(
            "## 📊 Combined Test Summary (Last Attempt)\n",
            branches,
            presets,
            cell_last_attempt,
        )
    )
    lines.append("\n### Detailed Results (All Attempts)\n")
    lines.extend(_build_table("", branches, presets, cell_detailed)[1:])
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(
        description='Collect step summaries from matrix jobs or save current step summary'
    )
    parser.add_argument(
        '--artifacts-dir',
        help='Directory containing summary artifacts from matrix jobs'
    )
    parser.add_argument(
        '--output',
        help='Output file for combined summary (default: GITHUB_STEP_SUMMARY)'
    )
    parser.add_argument(
        '--save-summary',
        action='store_true',
        help='Save current GITHUB_STEP_SUMMARY to artifact directory'
    )
    parser.add_argument(
        '--summary-dir',
        default=DEFAULT_SUMMARY_DIR,
        help='Directory to write summary artifacts (default: summary_artifacts)'
    )
    parser.add_argument(
        '--summary-path',
        default=os.environ.get('GITHUB_STEP_SUMMARY', ''),
        help='Path to GITHUB_STEP_SUMMARY file (default: env GITHUB_STEP_SUMMARY)'
    )
    parser.add_argument(
        '--job-name',
        default=os.environ.get('GITHUB_JOB', 'unknown'),
        help='Job name used to derive summary filename'
    )
    parser.add_argument(
        '--branch',
        default=os.environ.get('MATRIX_BRANCH') or os.environ.get('GITHUB_REF_NAME', 'unknown'),
        help='Branch name for summary filename'
    )
    parser.add_argument(
        '--build-preset',
        default=os.environ.get('INPUTS_BUILD_PRESET') or os.environ.get('MATRIX_BUILD_PRESET', 'unknown'),
        help='Build preset for summary filename'
    )

    args = parser.parse_args()

    if args.save_summary:
        saved = save_step_summary(
            summary_path=args.summary_path,
            output_dir=args.summary_dir,
            job_name=args.job_name,
            branch=args.branch,
            build_preset=args.build_preset,
        )
        print(f"Saved summary to {saved}", file=sys.stderr)
        return 0

    if not args.artifacts_dir:
        parser.error('--artifacts-dir is required unless --save-summary is used')

    print("Collecting summaries from artifacts...", file=sys.stderr)
    summaries = collect_summaries_from_artifacts(args.artifacts_dir)
    print(f"Found {len(summaries)} job summaries", file=sys.stderr)

    combined_markdown = generate_combined_markdown_table(summaries)

    output_file = args.output or os.environ.get('GITHUB_STEP_SUMMARY')
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(combined_markdown)
        print(f"Combined summary written to {output_file}", file=sys.stderr)
    else:
        print(combined_markdown)

    return 0


if __name__ == '__main__':
    sys.exit(main())
