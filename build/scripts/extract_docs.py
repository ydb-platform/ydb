import argparse
import os
import tarfile
import sys

# Explicitly enable local imports
# Don't forget to add imported scripts to inputs of the calling command!
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import process_command_files as pcf


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dest-dir', required=True)
    parser.add_argument('--skip-prefix', dest='skip_prefixes', action='append', default=[])
    parser.add_argument('docs', nargs='*')
    return parser.parse_args(pcf.get_args(sys.argv[1:]))


def main():
    args = parse_args()

    prefixes = ['{}{}'.format(os.path.normpath(p), os.path.sep) for p in args.skip_prefixes]

    def _valid_docslib(path):
        base = os.path.basename(path)
        return base.endswith(('.docslib', '.docslib.fake')) or base == 'preprocessed.tar.gz'

    for src in [p for p in args.docs if _valid_docslib(p)]:
        if src == 'preprocessed.tar.gz':
            rel_dst = os.path.dirname(os.path.normpath(src))
            for prefix in prefixes:
                if src.startswith(prefix):
                    rel_dst = rel_dst[len(prefix) :]
                    continue
            assert not os.path.isabs(rel_dst)
            dest_dir = os.path.join(args.dest_dir, rel_dst)
        else:
            dest_dir = args.dest_dir
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        with tarfile.open(src, 'r') as tar_file:
            if sys.version_info >= (3, 12):
                tar_file.extractall(dest_dir, filter='data')
            else:
                tar_file.extractall(dest_dir)


if __name__ == '__main__':
    main()
