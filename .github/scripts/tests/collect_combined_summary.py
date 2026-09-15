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


def parse_all_attempts(summary_text: str) -> List[Dict]:
    """
    Parse all attempt tables from markdown summary.
    
    Returns: list of dicts, each containing stats and URL for one attempt (try_1, try_2, try_3, etc.)
    """
    if not summary_text:
        return []
    
    # Pattern to match markdown table with test statistics
    pattern = r'\|\s*TESTS\s*\|\s*PASSED\s*\|\s*ERRORS\s*\|\s*FAILED\s*\|\s*SKIPPED\s*\|\s*MUTED'
    url_pattern = r'https://storage\.yandexcloud\.net/[^\s\)]+ya-test\.html[^\s\)]*'
    
    lines = summary_text.split('\n')
    
    # Find all table headers
    table_starts = []
    for i, line in enumerate(lines):
        if re.search(pattern, line, re.IGNORECASE):
            table_starts.append(i)
    
    if not table_starts:
        return []
    
    attempts = []
    
    def extract_number(s):
        match = re.search(r'\[?(\d+)\]?', s)
        return int(match.group(1)) if match else 0
    
    # Parse each table
    for table_idx, start_idx in enumerate(table_starts):
        attempt_num = table_idx + 1
        stats = {
            'tests': 0,
            'passed': 0,
            'errors': 0,
            'failed': 0,
            'skipped': 0,
            'muted': 0
        }
        attempt_url = None
        
        # Find data row for this table
        i = start_idx + 1
        while i < len(lines):
            line = lines[i].strip()
            if '---' in line:
                i += 1
                continue
            if line.startswith('|') and line.count('|') >= 6:
                parts = [p.strip() for p in line.split('|')]
                parts = [p for p in parts if p]
                
                if len(parts) >= 6:
                    try:
                        stats['tests'] = extract_number(parts[0])
                        stats['passed'] = extract_number(parts[1])
                        stats['errors'] = extract_number(parts[2])
                        stats['failed'] = extract_number(parts[3])
                        stats['skipped'] = extract_number(parts[4])
                        stats['muted'] = extract_number(parts[5])
                        
                        # Extract URL from FAILED or TESTS column
                        failed_cell = parts[3] if len(parts) > 3 else ''
                        tests_cell = parts[0] if len(parts) > 0 else ''
                        
                        match = re.search(url_pattern, failed_cell)
                        if match:
                            attempt_url = match.group(0)
                        else:
                            match = re.search(url_pattern, tests_cell)
                            if match:
                                attempt_url = match.group(0)
                        
                        break
                    except (ValueError, IndexError):
                        pass
            # Check if next line is a new table header
            if i + 1 < len(lines) and re.search(pattern, lines[i + 1], re.IGNORECASE):
                break
            i += 1
        
        if stats['tests'] > 0 or stats['failed'] > 0 or stats['errors'] > 0 or attempt_url:
            attempts.append({
                'attempt': attempt_num,
                'stats': stats,
                'url': attempt_url
            })
    
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
    if not summary_text:
        return None
    
    # Pattern to match markdown table with test statistics
    pattern = r'\|\s*TESTS\s*\|\s*PASSED\s*\|\s*ERRORS\s*\|\s*FAILED\s*\|\s*SKIPPED\s*\|\s*MUTED'
    
    lines = summary_text.split('\n')
    
    # Find all table headers
    table_starts = []
    for i, line in enumerate(lines):
        if re.search(pattern, line, re.IGNORECASE):
            table_starts.append(i)
    
    if not table_starts:
        return None
    
    # Use the LAST table (last attempt)
    start_idx = table_starts[-1]
    
    stats = {
        'tests': 0,
        'passed': 0,
        'errors': 0,
        'failed': 0,
        'skipped': 0,
        'muted': 0
    }
    
    def extract_number(s):
        # Handle format: [624](url) or just 624
        match = re.search(r'\[?(\d+)\]?', s)
        return int(match.group(1)) if match else 0
    
    # Look for data row after the header (only from last table)
    i = start_idx + 1
    while i < len(lines):
        line = lines[i].strip()
        # Skip separator line
        if '---' in line:
            i += 1
            continue
        # Check if this is a data row
        if line.startswith('|') and line.count('|') >= 6:
            # Parse the row - this is the data from last attempt
            parts = [p.strip() for p in line.split('|')]
            parts = [p for p in parts if p]  # Remove empty
            
            if len(parts) >= 6:
                try:
                    # Take values from last attempt only (don't sum)
                    stats['tests'] = extract_number(parts[0])
                    stats['passed'] = extract_number(parts[1])
                    stats['errors'] = extract_number(parts[2])
                    stats['failed'] = extract_number(parts[3])
                    stats['skipped'] = extract_number(parts[4])
                    stats['muted'] = extract_number(parts[5])
                    # Found the data row, break
                    break
                except (ValueError, IndexError):
                    pass
        # Check if next line is a new table header (shouldn't happen for last table, but just in case)
        if i + 1 < len(lines) and re.search(pattern, lines[i + 1], re.IGNORECASE):
            # This shouldn't happen since we're using last table, but break anyway
            break
        i += 1
    
    # Only return stats if we found data
    if stats['tests'] > 0 or stats['failed'] > 0 or stats['errors'] > 0:
        return stats
    
    return None


def extract_test_report_url(summary_text: str) -> Optional[str]:
    """
    Extract test report URL from summary.
    Takes URL from the LAST attempt (last table) - from FAILED column if available, otherwise from TESTS column.
    """
    if not summary_text:
        return None
    
    # Pattern to match markdown table with test statistics
    table_pattern = r'\|\s*TESTS\s*\|\s*PASSED\s*\|\s*ERRORS\s*\|\s*FAILED\s*\|\s*SKIPPED\s*\|\s*MUTED'
    lines = summary_text.split('\n')
    
    # Find all table headers
    table_starts = []
    for i, line in enumerate(lines):
        if re.search(table_pattern, line, re.IGNORECASE):
            table_starts.append(i)
    
    if not table_starts:
        return None
    
    # Use the LAST table (last attempt)
    start_idx = table_starts[-1]
    
    # Pattern for storage.yandexcloud.net URLs with ya-test.html
    url_pattern = r'https://storage\.yandexcloud\.net/[^\s\)]+ya-test\.html[^\s\)]*'
    
    # Look for data row after the header and extract URL from FAILED column (preferred) or TESTS column
    i = start_idx + 1
    while i < len(lines):
        line = lines[i].strip()
        # Skip separator line
        if '---' in line:
            i += 1
            continue
        # Check if this is a data row
        if line.startswith('|') and line.count('|') >= 6:
            # Parse the row
            parts = [p.strip() for p in line.split('|')]
            parts = [p for p in parts if p]  # Remove empty
            
            if len(parts) >= 6:
                # Try FAILED column first (column index 3, but parts[0] is TESTS, parts[1] is PASSED, etc.)
                # Actually, parts array: [TESTS, PASSED, ERRORS, FAILED, SKIPPED, MUTED]
                # So FAILED is at index 3
                failed_cell = parts[3] if len(parts) > 3 else ''
                tests_cell = parts[0] if len(parts) > 0 else ''
                
                # Try to extract URL from FAILED column first
                match = re.search(url_pattern, failed_cell)
                if match:
                    return match.group(0)
                
                # Fallback to TESTS column
                match = re.search(url_pattern, tests_cell)
                if match:
                    return match.group(0)
                
                # Found the data row, break
                break
        i += 1
    
    # Fallback: search in entire summary (but prefer last attempt)
    # Search backwards from end to find last URL
    all_matches = list(re.finditer(url_pattern, summary_text))
    if all_matches:
        return all_matches[-1].group(0)
    
    return None


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
    elif ' / ' in job_name:
        # Format: "Regression-run_... (build_preset) / branch:build_preset"
        parts = job_name.split(' / ')
        if len(parts) >= 2:
            branch_preset = parts[-1].strip()
            if ':' in branch_preset:
                branch, build_preset = branch_preset.split(':', 1)
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
            
            if not branch or not build_preset:
                print(f"Warning: Could not extract branch/build_preset from {job_name} (file: {summary_file})", file=sys.stderr)
                continue
            
            key = f"{branch}:{build_preset}"
            
            # Parse statistics from last attempt
            stats = parse_summary_markdown(summary_text)
            test_report_url = extract_test_report_url(summary_text)
            
            # Parse all attempts for detailed table
            all_attempts = parse_all_attempts(summary_text)
            
            summaries[key] = {
                'branch': branch,
                'build_preset': build_preset,
                'job_name': job_name,
                'stats': stats or {},
                'test_report_url': test_report_url,
                'all_attempts': all_attempts,  # List of all attempts with stats and URLs
            }
            print(f"Collected summary for {key}", file=sys.stderr)
            
        except Exception as e:
            print(f"Warning: Error processing {summary_file}: {e}", file=sys.stderr)
            continue
    
    return summaries


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
    
    lines = []
    lines.append("## 📊 Combined Test Summary\n")
    lines.append("| Branch | " + " | ".join(presets) + " |")
    lines.append("|" + "---|" * (len(presets) + 1))
    
    for branch in branches:
        row = [branch]
        for preset in presets:
            key = f"{branch}:{preset}"
            if key in summaries:
                summary = summaries[key]
                stats = summary.get('stats', {})
                failed = stats.get('failed', 0)
                errors = stats.get('errors', 0)
                total_errors = failed + errors
                test_report_url = summary.get('test_report_url', '')
                
                if test_report_url:
                    if total_errors > 0:
                        cell = f"[{total_errors}]({test_report_url})"
                        if failed > 0 and errors > 0:
                            cell += f" ({failed} failed, {errors} errors)"
                        elif failed > 0:
                            cell += f" ({failed} failed)"
                        else:
                            cell += f" ({errors} errors)"
                    else:
                        # No errors, but still show link
                        cell = f"[✓]({test_report_url})"
                else:
                    # No URL available
                    if total_errors > 0:
                        cell = str(total_errors)
                        if failed > 0 and errors > 0:
                            cell += f" ({failed} failed, {errors} errors)"
                        elif failed > 0:
                            cell += f" ({failed} failed)"
                        else:
                            cell += f" ({errors} errors)"
                    else:
                        cell = "✓"
                row.append(cell)
            else:
                row.append("—")
        
        lines.append("| " + " | ".join(row) + " |")
    
    # Add detailed table with all attempts
    # Same structure as main table: branches as rows, build_presets as columns
    lines.append("\n### Detailed Results (All Attempts)\n")
    lines.append("| Branch | " + " | ".join(presets) + " |")
    lines.append("|" + "---|" * (len(presets) + 1))
    
    for branch in branches:
        row = [branch]
        for preset in presets:
            key = f"{branch}:{preset}"
            if key in summaries:
                summary = summaries[key]
                all_attempts = summary.get('all_attempts', [])
                
                if all_attempts:
                    # Format all attempts in this cell
                    attempt_parts = []
                    for attempt in all_attempts:
                        attempt_num = attempt.get('attempt', 0)
                        attempt_stats = attempt.get('stats', {})
                        failed = attempt_stats.get('failed', 0)
                        errors = attempt_stats.get('errors', 0)
                        attempt_url = attempt.get('url', '')
                        
                        total_errors = failed + errors
                        
                        if attempt_url:
                            if total_errors > 0:
                                attempt_text = f"try_{attempt_num}: [{total_errors}]({attempt_url})"
                                if failed > 0 and errors > 0:
                                    attempt_text += f" ({failed}f, {errors}e)"
                                elif failed > 0:
                                    attempt_text += f" ({failed}f)"
                                else:
                                    attempt_text += f" ({errors}e)"
                            else:
                                attempt_text = f"try_{attempt_num}: [✓]({attempt_url})"
                        else:
                            if total_errors > 0:
                                attempt_text = f"try_{attempt_num}: {total_errors}"
                                if failed > 0 and errors > 0:
                                    attempt_text += f" ({failed}f, {errors}e)"
                                elif failed > 0:
                                    attempt_text += f" ({failed}f)"
                                else:
                                    attempt_text += f" ({errors}e)"
                            else:
                                attempt_text = f"try_{attempt_num}: ✓"
                        
                        attempt_parts.append(attempt_text)
                    
                    cell = "<br>".join(attempt_parts)
                else:
                    # Fallback if all_attempts not parsed
                    stats = summary.get('stats', {})
                    failed = stats.get('failed', 0)
                    errors = stats.get('errors', 0)
                    test_report_url = summary.get('test_report_url', '')
                    
                    total_errors = failed + errors
                    if test_report_url:
                        if total_errors > 0:
                            cell = f"last: [{total_errors}]({test_report_url})"
                            if failed > 0 and errors > 0:
                                cell += f" ({failed}f, {errors}e)"
                            elif failed > 0:
                                cell += f" ({failed}f)"
                            else:
                                cell += f" ({errors}e)"
                        else:
                            cell = f"last: [✓]({test_report_url})"
                    else:
                        if total_errors > 0:
                            cell = f"last: {total_errors}"
                            if failed > 0 and errors > 0:
                                cell += f" ({failed}f, {errors}e)"
                            elif failed > 0:
                                cell += f" ({failed}f)"
                            else:
                                cell += f" ({errors}e)"
                        else:
                            cell = "last: ✓"
                
                row.append(cell)
            else:
                row.append("—")
        
        lines.append("| " + " | ".join(row) + " |")
    
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(
        description='Collect step summaries from matrix jobs and generate combined markdown table'
    )
    parser.add_argument(
        '--artifacts-dir',
        required=True,
        help='Directory containing summary artifacts from matrix jobs'
    )
    parser.add_argument(
        '--output',
        help='Output file for combined summary (default: GITHUB_STEP_SUMMARY)'
    )
    
    args = parser.parse_args()
    
    # Collect summaries
    print("Collecting summaries from artifacts...", file=sys.stderr)
    summaries = collect_summaries_from_artifacts(args.artifacts_dir)
    print(f"Found {len(summaries)} job summaries", file=sys.stderr)
    
    # Generate combined markdown table
    combined_markdown = generate_combined_markdown_table(summaries)
    
    # Write output
    output_file = args.output or os.environ.get('GITHUB_STEP_SUMMARY')
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(combined_markdown)
        print(f"Combined summary written to {output_file}", file=sys.stderr)
    else:
        # Write to stdout if no output file specified
        print(combined_markdown)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
