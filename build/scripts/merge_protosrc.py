import argparse
import os
import tarfile
import sys


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('args', nargs='+')
    return parser.parse_args()


def main(args):
    peers = args.args

    compression_mode = ''
    if args.output.endswith(('.tar.gz', '.tgz')):
        compression_mode = 'gz'
    elif args.output.endswith('.bzip2'):
        compression_mode = 'bz2'

    files = set()
    with tarfile.open(args.output, f'w:{compression_mode}') as dest:
        for psrc in [p[:-len('.self.protodesc')]+'.protosrc' for p in peers if p.endswith('.self.protodesc')]:
            with tarfile.open(psrc, 'r') as src:
                for tarinfo in [m for m in src.getmembers() if m.name != '']:
                    if tarinfo.name in files:
                        continue
                    files.add(tarinfo.name)
                    if tarinfo.isdir():
                        dest.addfile(tarinfo)
                    else:
                        dest.addfile(tarinfo, src.extractfile(tarinfo))
    return 0


if __name__ == '__main__':
    sys.exit(main(parse_args()))
