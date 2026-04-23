import argparse
import os
import shutil
import sys


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--result', required=True)
    return parser.parse_args(sys.argv[1:])


def main():
    args = parse_args()
    if os.path.basename(args.name) != args.name:
        print("Error: --name must be a base name without directory components", file=sys.stderr, flush=True)
        sys.exit(1)
    result_dir = os.path.dirname(args.result)
    src_path = os.path.join(result_dir, args.name)

    if not os.path.exists(src_path):
        print(f"Error: Source file '{src_path}' does not exist", file=sys.stderr, flush=True)
        sys.exit(1)
    if not os.path.isfile(src_path):
        print(f"Error: Source path '{src_path}' is not a file", file=sys.stderr, flush=True)
        sys.exit(1)
    if os.path.isdir(args.output):
        print(f"Error: Output path '{args.output}' is a directory", file=sys.stderr, flush=True)
        sys.exit(1)
    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    try:
        shutil.move(src_path, args.output)
    except Exception as e:
        print(f"Error moving file: {str(e)}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
