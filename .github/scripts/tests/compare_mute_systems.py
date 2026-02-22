#!/usr/bin/env python3
"""
Compare new mute system (v4) with legacy (no quarantine, no graduation).

Runs create_new_muted_ya twice:
  - Legacy: --legacy, output to comparison/legacy/
  - Current: output to comparison/current/

Produces diff report for to_mute, to_unmute, to_delete, final muted_ya.

Usage:
  compare_mute_systems.py --branch main [--build_type relwithdebinfo]
  # Requires: flaky_tests_history, tests_monitor, base_muted_ya.txt, quarantine.txt
"""

import argparse
import os
import subprocess
import sys


def _load_lines(path):
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return set(line.strip() for line in f if line.strip() and not line.strip().startswith("#"))


def _diff_sets(a, b, label_a, label_b):
    only_a = a - b
    only_b = b - a
    return only_a, only_b


def main():
    parser = argparse.ArgumentParser(description="Compare mute systems: legacy vs current (v4)")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--build_type", default="relwithdebinfo")
    parser.add_argument("--muted_ya_file", default="base_muted_ya.txt")
    parser.add_argument("--quarantine_file", default="quarantine.txt")
    parser.add_argument("--output_dir", default="comparison")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    create_script = os.path.join(script_dir, "create_new_muted_ya.py")
    legacy_dir = os.path.join(args.output_dir, "legacy")
    current_dir = os.path.join(args.output_dir, "current")

    os.makedirs(legacy_dir, exist_ok=True)
    os.makedirs(current_dir, exist_ok=True)

    # Run legacy
    print("=== Running LEGACY (no quarantine, no graduation) ===")
    rc = subprocess.run(
        [
            sys.executable,
            create_script,
            "update_muted_ya",
            "--branch", args.branch,
            "--build_type", args.build_type,
            "--muted_ya_file", args.muted_ya_file,
            "--quarantine_file", args.quarantine_file,
            "--output_folder", legacy_dir,
        ] + ["--legacy"],
        capture_output=False,
    )
    if rc.returncode != 0:
        print(f"Legacy run failed: {rc.returncode}")
        return rc.returncode

    # Run current (v4)
    print("\n=== Running CURRENT (v4 with quarantine, graduation) ===")
    rc = subprocess.run(
        [
            sys.executable,
            create_script,
            "update_muted_ya",
            "--branch", args.branch,
            "--build_type", args.build_type,
            "--muted_ya_file", args.muted_ya_file,
            "--quarantine_file", args.quarantine_file,
            "--output_folder", current_dir,
        ],
        capture_output=False,
    )
    if rc.returncode != 0:
        print(f"Current run failed: {rc.returncode}")
        return rc.returncode

    # Compare
    mute_update = "mute_update"
    legacy_base = os.path.join(legacy_dir, mute_update)
    current_base = os.path.join(current_dir, mute_update)

    def load_both(name):
        return _load_lines(os.path.join(legacy_base, name)), _load_lines(os.path.join(current_base, name))

    report = []
    report.append("# Mute System Comparison: Legacy vs Current (v4)")
    report.append(f"Branch: {args.branch}, Build type: {args.build_type}\n")

    for label, filename in [
        ("to_mute", "to_mute.txt"),
        ("to_unmute", "to_unmute.txt"),
        ("to_delete", "to_delete.txt"),
        ("final muted_ya", "new_muted_ya.txt"),
    ]:
        leg, cur = load_both(filename)
        only_legacy = leg - cur
        only_current = cur - leg
        report.append(f"## {label}")
        report.append(f"- Legacy: {len(leg)}, Current: {len(cur)}")
        if only_legacy:
            report.append(f"- Only in LEGACY ({len(only_legacy)}):")
            for t in sorted(only_legacy)[:20]:
                report.append(f"  - {t}")
            if len(only_legacy) > 20:
                report.append(f"  ... and {len(only_legacy) - 20} more")
        if only_current:
            report.append(f"- Only in CURRENT ({len(only_current)}):")
            for t in sorted(only_current)[:20]:
                report.append(f"  - {t}")
            if len(only_current) > 20:
                report.append(f"  ... and {len(only_current) - 20} more")
        if not only_legacy and not only_current:
            report.append("- No differences")
        report.append("")

    report_text = "\n".join(report)
    print("\n" + report_text)

    report_path = os.path.join(args.output_dir, "comparison_report.md")
    with open(report_path, "w") as f:
        f.write(report_text)
    print(f"\nReport saved to {report_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
