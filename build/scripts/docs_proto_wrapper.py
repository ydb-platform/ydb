import argparse
import subprocess
import sys
import pathlib


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--docs-output', required=True)
    parser.add_argument('--template', required=True)
    parser.add_argument('args', nargs='+')

    return parser.parse_args()


def main(args):
    cmd = list(args.args)
    # interface is like this:
    # --doc_out=TARGET_DIR
    # --doc_opt=markdon,TARGET_FILE_NAME

    target_file = pathlib.Path(args.docs_output)
    target_template = pathlib.Path(args.template)
    cmd.append(f'--doc_opt={target_template},{target_file.name}')
    cmd.append(f'--doc_out={target_file.parent}')

    try:
        subprocess.check_output(cmd, stdin=None, stderr=subprocess.STDOUT, text=True)
    except subprocess.CalledProcessError as e:
        sys.stderr.write(f'{e.cmd} returned non-zero exit code {e.returncode}.\n{e.output}\n')
        return e.returncode

    return 0


if __name__ == '__main__':
    sys.exit(main(parse_args()))
