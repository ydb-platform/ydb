#!/usr/bin/env python3
"""
Compare mute systems: legacy vs current (v4) vs v4_direct.

Runs three modes:
  - Legacy: create_new_muted_ya --legacy (uses tests_monitor, no quarantine)
  - Current: create_new_muted_ya (uses tests_monitor, with quarantine)
  - v4_direct: create_new_muted_ya_v4 (uses test_results directly, NO flaky_tests_window/tests_monitor)

Legacy and Current require flaky_tests_history + tests_monitor. v4_direct does not.

Usage:
  compare_mute_systems.py --branch main [--build_type relwithdebinfo]
  # For legacy/current: run flaky_tests_history.py and tests_monitor.py first
  # v4_direct: only needs test_results (test_runs_column)
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


def main():
    parser = argparse.ArgumentParser(description="Compare mute systems: legacy vs current vs v4_direct")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--build_type", default="relwithdebinfo")
    parser.add_argument("--muted_ya_file", default="base_muted_ya.txt")
    parser.add_argument("--quarantine_file", default="quarantine.txt")
    parser.add_argument("--output_dir", default="comparison")
    parser.add_argument("--skip_legacy", action="store_true", help="Skip legacy (requires flaky+tests_monitor)")
    parser.add_argument("--skip_current", action="store_true", help="Skip current (requires flaky+tests_monitor)")
    parser.add_argument("--skip_v4_direct", action="store_true", help="Skip v4_direct (uses test_results only)")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    create_script = os.path.join(script_dir, "create_new_muted_ya.py")
    v4_direct_script = os.path.join(script_dir, "create_new_muted_ya_v4.py")
    legacy_dir = os.path.join(args.output_dir, "legacy")
    current_dir = os.path.join(args.output_dir, "current")
    v4_direct_dir = os.path.join(args.output_dir, "v4_direct")

    os.makedirs(args.output_dir, exist_ok=True)

    # Run legacy (needs flaky_tests_history + tests_monitor)
    if not args.skip_legacy:
        os.makedirs(legacy_dir, exist_ok=True)
        print("=== Running LEGACY (tests_monitor, no quarantine) ===")
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

    # Run current (needs flaky_tests_history + tests_monitor)
    if not args.skip_current:
        os.makedirs(current_dir, exist_ok=True)
        print("\n=== Running CURRENT (tests_monitor, with quarantine) ===")
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

    # Run v4_direct (NO flaky_tests_history, NO tests_monitor - read test_results only)
    if not args.skip_v4_direct:
        os.makedirs(v4_direct_dir, exist_ok=True)
        print("\n=== Running v4_direct (test_results only, no flaky/monitor) ===")
        rc = subprocess.run(
            [
                sys.executable,
                v4_direct_script,
                "--branch", args.branch,
                "--build_type", args.build_type,
                "--muted_ya_file", args.muted_ya_file,
                "--quarantine_file", args.quarantine_file,
                "--output_folder", v4_direct_dir,
            ],
            capture_output=False,
        )
        if rc.returncode != 0:
            print(f"v4_direct run failed: {rc.returncode}")
            return rc.returncode

    # Build comparison report
    mute_update = "mute_update"
    legacy_base = os.path.join(legacy_dir, mute_update)
    current_base = os.path.join(current_dir, mute_update)
    v4_direct_base = os.path.join(v4_direct_dir, mute_update)

    def load(name, base):
        return _load_lines(os.path.join(base, name)) if os.path.exists(base) else set()

    report = []
    report.append("# Mute System Comparison: Legacy vs Current vs v4_direct")
    report.append(f"Branch: {args.branch}, Build type: {args.build_type}\n")
    report.append("| Mode | Data source | flaky/monitor |")
    report.append("|------|-------------|---------------|")
    report.append("| Legacy | tests_monitor | required |")
    report.append("| Current | tests_monitor | required |")
    report.append("| v4_direct | test_results | **not used** |\n")

    modes = []
    if not args.skip_legacy:
        modes.append(("legacy", legacy_base))
    if not args.skip_current:
        modes.append(("current", current_base))
    if not args.skip_v4_direct:
        modes.append(("v4_direct", v4_direct_base))

    for label, filename in [
        ("to_mute", "to_mute.txt"),
        ("to_unmute", "to_unmute.txt"),
        ("to_delete", "to_delete.txt"),
        ("final muted_ya", "new_muted_ya.txt"),
    ]:
        report.append(f"## {label}")
        data = {m: load(filename, b) for m, b in modes}
        for mode, vals in data.items():
            report.append(f"- {mode}: {len(vals)}")
        if len(modes) >= 2:
            # Compare pairwise
            for i, (m1, b1) in enumerate(modes):
                for m2, b2 in modes[i + 1:]:
                    s1, s2 = data[m1], data[m2]
                    only_1 = s1 - s2
                    only_2 = s2 - s1
                    if only_1 or only_2:
                        report.append(f"\n**{m1} vs {m2}:**")
                        if only_1:
                            report.append(f"- Only in {m1} ({len(only_1)}):")
                            for t in sorted(only_1)[:20]:
                                report.append(f"  - {t}")
                            if len(only_1) > 20:
                                report.append(f"  ... and {len(only_1) - 20} more")
                        if only_2:
                            report.append(f"- Only in {m2} ({len(only_2)}):")
                            for t in sorted(only_2)[:20]:
                                report.append(f"  - {t}")
                            if len(only_2) > 20:
                                report.append(f"  ... and {len(only_2) - 20} more")
                    else:
                        report.append(f"\n**{m1} vs {m2}:** No differences")
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
