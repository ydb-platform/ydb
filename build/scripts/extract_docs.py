import argparse
import os
import tarfile
import sys

# Explicitly enable local imports
# Don't forget to add imported scripts to inputs of the calling command!
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import process_command_files as pcf  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dest-dir', required=True)
    parser.add_argument('docs', nargs='*')
    return parser.parse_args(pcf.get_args(sys.argv[1:]))


def main():
    args = parse_args()

    def _valid_docslib(path):
        base = os.path.basename(path)
        return base.endswith(('.docslib', '.docslib.fake')) or base == '_preprocessed.tar.gz'

    dest_dir = args.dest_dir
    for src in [p for p in args.docs if _valid_docslib(p)]:
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        with tarfile.open(src, 'r') as tar_file:
            if sys.version_info >= (3, 12):
                tar_file.extractall(dest_dir, filter='data')
            else:
                tar_file.extractall(dest_dir)


if __name__ == '__main__':
    main()
