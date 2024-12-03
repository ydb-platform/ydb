import argparse
import subprocess
import sys, os


WRAPCC_ARGS_END='--wrapcc-end'

def fix(source_file: str, source_root: str):
    flags = []
    return flags

def parse_args():
    delimer = -1
    if WRAPCC_ARGS_END in sys.argv:
        delimer = sys.argv.index(WRAPCC_ARGS_END)
    assert delimer != -1, f"This wrapper should be called with {WRAPCC_ARGS_END} argument."

    parser = argparse.ArgumentParser()
    parser.add_argument('--source-file', required=True)
    parser.add_argument('--source-root', required=True)
    cc_cmd = sys.argv[delimer+1:]
    return parser.parse_args(sys.argv[1:delimer]), cc_cmd

if __name__ == '__main__':
    args, cc_cmd = parse_args()
    cmd = cc_cmd + fix(args.source_file, args.source_root)
    os.execv(cmd[0], cmd)
