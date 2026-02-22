#!/usr/bin/env python3
"""
Detect manually unmuted tests (removed from muted_ya but not by our automation).
Add them to quarantine.txt for protection from re-mute.

Usage:
  detect_manual_unmutes.py --previous_base FILE --current_base FILE --to_unmute FILE --quarantine_file FILE [--output_quarantine FILE]
"""

import argparse
import os


def parse_mute_file(path):
    """Parse mute file, return set of test lines (suite test_name)."""
    if not path or not os.path.exists(path):
        return set()
    with open(path) as f:
        return set(
            l.strip()
            for l in f
            if l.strip() and not l.strip().startswith("#")
        )


def main():
    parser = argparse.ArgumentParser(description="Detect manual unmutes and add to quarantine")
    parser.add_argument("--previous_base", required=True, help="Path to previous base muted_ya (from cache)")
    parser.add_argument("--current_base", required=True, help="Path to current base muted_ya from main")
    parser.add_argument("--to_unmute", required=True, help="Path to our to_unmute.txt from this run")
    parser.add_argument("--quarantine_file", required=True, help="Path to quarantine.txt")
    parser.add_argument("--output_quarantine", help="Path to write updated quarantine (default: overwrite quarantine_file)")
    args = parser.parse_args()

    previous = parse_mute_file(args.previous_base)
    current = parse_mute_file(args.current_base)
    our_to_unmute = parse_mute_file(args.to_unmute)

    removed = previous - current
    manual_unmutes = removed - our_to_unmute

    if not manual_unmutes:
        print("No manual unmutes detected")
        return 0

    print(f"Detected {len(manual_unmutes)} manual unmutes, adding to quarantine:")
    for t in sorted(manual_unmutes):
        print(f"  - {t}")

    existing = parse_mute_file(args.quarantine_file)
    updated = existing | manual_unmutes
    output_path = args.output_quarantine or args.quarantine_file
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        f.write("\n".join(sorted(updated)) + "\n")
    print(f"Updated quarantine: {output_path} ({len(updated)} tests)")
    return 0


if __name__ == "__main__":
    exit(main())
