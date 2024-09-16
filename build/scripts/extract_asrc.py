import argparse
import os
import sys
import tarfile


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', nargs='*', required=True)
    parser.add_argument('--output', required=True)

    return parser.parse_args()


def main():
    args = parse_args()

    for asrc in [x for x in args.input if x.endswith('.asrc') and os.path.exists(x)]:
        with tarfile.open(asrc, 'r') as tar:
            if sys.version_info >= (3, 12):
                tar.extractall(path=args.output, filter='data')
            else:
                tar.extractall(path=args.output)



if __name__ == '__main__':
    main()
