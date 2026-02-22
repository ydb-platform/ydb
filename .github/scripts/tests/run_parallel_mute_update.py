#!/usr/bin/env python3
"""
Run legacy and v4_direct in parallel.

- Legacy → mute_update_legacy/ (для .github/config/muted_ya.txt)
- v4 → mute_update_v4/ (для .github/config/mute/muted_ya.txt)

Workflow копирует оба в PR.
"""

import argparse
import os
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description="Run legacy and v4 in parallel, output to separate dirs")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--build_type", default="relwithdebinfo")
    parser.add_argument("--muted_ya_file", default="base_muted_ya.txt", help="Base for legacy")
    parser.add_argument("--mute_muted_ya_file", default=None, help="Base for v4 (default: same as muted_ya_file)")
    parser.add_argument("--quarantine_file", default="quarantine.txt")
    args = parser.parse_args()
    v4_muted_ya = args.mute_muted_ya_file or args.muted_ya_file

    script_dir = os.path.dirname(os.path.abspath(__file__))
    create_script = os.path.join(script_dir, "create_new_muted_ya.py")
    v4_script = os.path.join(script_dir, "create_new_muted_ya_v4.py")

    legacy_dir = "mute_update_legacy"
    v4_dir = "mute_update_v4"

    os.makedirs(legacy_dir, exist_ok=True)
    os.makedirs(v4_dir, exist_ok=True)

    # 1. Legacy (tests_monitor, no quarantine) → для .github/config/muted_ya.txt
    print("=== Running LEGACY (→ .github/config/muted_ya.txt) ===")
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
            "--legacy",
            "--system-version", "legacy",
        ],
        capture_output=False,
    )
    if rc.returncode != 0:
        print(f"Legacy run failed: {rc.returncode}", file=sys.stderr)
        return rc.returncode

    # 2. v4 (test_results only) → для .github/config/mute/muted_ya.txt
    print("\n=== Running v4 (→ .github/config/mute/muted_ya.txt) ===")
    rc = subprocess.run(
        [
            sys.executable,
            v4_script,
            "--branch", args.branch,
            "--build_type", args.build_type,
            "--muted_ya_file", v4_muted_ya,
            "--quarantine_file", args.quarantine_file,
            "--output_folder", v4_dir,
            "--system-version", "v4_direct",
        ],
        capture_output=False,
    )
    if rc.returncode != 0:
        print(f"v4 run failed: {rc.returncode}", file=sys.stderr)
        return rc.returncode

    print("\n=== Done: legacy in mute_update_legacy/, v4 in mute_update_v4/ ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
