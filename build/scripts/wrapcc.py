import argparse
import os
import sys


WRAPCC_ARGS_END = '--wrapcc-end'


def fix(source_file: str, source_root: str, build_root: str) -> list[str]:
    flags = []
    return flags


def parse_args():
    delimiter = -1
    if WRAPCC_ARGS_END in sys.argv:
        delimiter = sys.argv.index(WRAPCC_ARGS_END)
    assert delimiter != -1, f"This wrapper should be called with {WRAPCC_ARGS_END} argument."

    parser = argparse.ArgumentParser()
    parser.add_argument('--source-file', required=True)
    parser.add_argument('--source-root', required=True)
    parser.add_argument('--build-root', required=True)
    cc_cmd = sys.argv[delimiter + 1:]
    return parser.parse_args(sys.argv[1:delimiter]), cc_cmd


if __name__ == '__main__':
    args, cc_cmd = parse_args()
    cmd = cc_cmd + fix(args.source_file, args.source_root, args.build_root)
    os.execv(cmd[0], cmd)
