#!/usr/bin/env python3
"""
Detect manually unmuted tests and add to quarantine.txt.
"""

import argparse
import os

from .utils import parse_mute_file


def main():
    parser = argparse.ArgumentParser(description="Detect manual unmutes and add to quarantine")
    parser.add_argument("--previous_base", required=True)
    parser.add_argument("--current_base", required=True)
    parser.add_argument("--to_unmute", required=True)
    parser.add_argument("--quarantine_file", required=True)
    parser.add_argument("--output_quarantine", default=None)
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
