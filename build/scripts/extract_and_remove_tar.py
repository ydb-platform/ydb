#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import tarfile

def is_exe(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

def parse_args():
    parser = argparse.ArgumentParser(description='Extract a tarball and remove it upon successful extraction.')
    parser.add_argument('--input', required=True, help='Path to the input tarball')
    parser.add_argument('--output', required=True, help='Path to the output directory')
    return parser.parse_args()

def unpack_dir(tared_dir, dest_path):
    tared_dir = os.path.abspath(tared_dir)
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    for tar_exe in ('/usr/bin/tar', '/bin/tar'):
        if is_exe(tar_exe):
            tar_cmd = [tar_exe, '--extract', '--file', tared_dir, '-C', dest_path, '--no-same-owner']

            if sys.platform == 'linux':
                tar_cmd.append('--delay-directory-restore')

            subprocess.run(tar_cmd, check=True, text=True)
            break
    else:
        with tarfile.open(tared_dir, 'r') as tar_file:
            tar_file.extractall(dest_path)

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

    unpack_dir(args.input, args.output)

    # Remove input file only if extraction was successful
    try:
        os.remove(args.input)
    except OSError as e:
        sys.exit(f"Failed to remove input file: {e}")


if __name__ == '__main__':
    main()
