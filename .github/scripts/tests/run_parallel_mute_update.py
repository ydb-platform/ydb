#!/usr/bin/env python3
"""
Run legacy and v4_direct in parallel, record both to mute_decisions, use active system's output.

ACTIVE_SYSTEM env: "legacy" | "v4_direct" (default: legacy)
- legacy: tests_monitor, no quarantine
- v4_direct: test_results only, with quarantine

Both systems' decisions are written to mute_decisions with system_version suffix.
The active system's output is copied to mute_update/ for the rest of the workflow.
"""

import argparse
import os
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description="Run parallel mute systems, record both, use active output")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--build_type", default="relwithdebinfo")
    parser.add_argument("--muted_ya_file", default="base_muted_ya.txt")
    parser.add_argument("--quarantine_file", default="quarantine.txt")
    parser.add_argument("--active_system", default=None, help="Override ACTIVE_SYSTEM env: legacy | v4_direct")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    create_script = os.path.join(script_dir, "create_new_muted_ya.py")
    v4_script = os.path.join(script_dir, "create_new_muted_ya_v4.py")

    # Priority: arg > env > config file > default
    active = args.active_system or os.environ.get("ACTIVE_SYSTEM")
    if not active:
        config_path = os.path.join(os.path.dirname(script_dir), "..", "config", "active_mute_system.txt")
        if os.path.exists(config_path):
            with open(config_path) as f:
                active = f.read().strip().lower() or "legacy"
        else:
            active = "legacy"
    if active not in ("legacy", "v4_direct"):
        print(f"Invalid ACTIVE_SYSTEM={active}, use legacy or v4_direct", file=sys.stderr)
        return 1

    legacy_dir = "mute_update_legacy"
    v4_dir = "mute_update_v4"
    final_dir = "mute_update"

    os.makedirs(legacy_dir, exist_ok=True)
    os.makedirs(v4_dir, exist_ok=True)

    # 1. Run legacy (tests_monitor, no quarantine)
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
            "--legacy",
            "--system-version", "legacy",
        ],
        capture_output=False,
    )
    if rc.returncode != 0:
        print(f"Legacy run failed: {rc.returncode}", file=sys.stderr)
        return rc.returncode

    # 2. Run v4_direct (test_results only)
    print("\n=== Running v4_direct (test_results only) ===")
    rc = subprocess.run(
        [
            sys.executable,
            v4_script,
            "--branch", args.branch,
            "--build_type", args.build_type,
            "--muted_ya_file", args.muted_ya_file,
            "--quarantine_file", args.quarantine_file,
            "--output_folder", v4_dir,
            "--system-version", "v4_direct",
        ],
        capture_output=False,
    )
    if rc.returncode != 0:
        print(f"v4_direct run failed: {rc.returncode}", file=sys.stderr)
        return rc.returncode

    # 3. Copy active system's output to mute_update/
    src = os.path.join(legacy_dir if active == "legacy" else v4_dir, "mute_update")
    os.makedirs(final_dir, exist_ok=True)
    for name in os.listdir(src):
        src_path = os.path.join(src, name)
        dst_path = os.path.join(final_dir, name)
        if os.path.isfile(src_path):
            shutil.copy2(src_path, dst_path)
        elif os.path.isdir(src_path):
            if os.path.exists(dst_path):
                shutil.rmtree(dst_path)
            shutil.copytree(src_path, dst_path)

    print(f"\n=== Active system: {active} (output in {final_dir}/) ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
