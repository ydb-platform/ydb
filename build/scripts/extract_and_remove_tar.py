#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys


def parse_args():
    parser = argparse.ArgumentParser(description='Extract a tarball and remove it upon successful extraction.')
    parser.add_argument('--input', required=True, help='Path to the input tarball')
    parser.add_argument('--output', required=True, help='Path to the output directory')
    return parser.parse_args()


def main():
    args = parse_args()

    # Validate input file
    if not os.path.isfile(args.input):
        sys.exit(f"Input file does not exist: {args.input}")

    # Create output directory (if it doesn't exist)
    try:
        os.makedirs(args.output, exist_ok=True)
    except OSError as e:
        sys.exit(f"Failed to create output directory: {e}")

    # Prepare tar command
    tar_cmd = ['/usr/bin/tar', '--extract', '--file', args.input, '-C', args.output, '--no-same-owner']
    if sys.platform == 'linux':
        tar_cmd.append('--delay-directory-restore')

    # Run tar command
    try:
        result = subprocess.run(tar_cmd, check=True)
    except subprocess.CalledProcessError as e:
        sys.exit(f"tar command failed with exit code {e.returncode}")

    # Remove input file only if extraction was successful
    try:
        os.remove(args.input)
    except OSError as e:
        sys.exit(f"Failed to remove input file: {e}")


if __name__ == '__main__':
    main()
