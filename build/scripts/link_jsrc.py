import argparse
import tarfile


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', nargs='*')
    parser.add_argument('--output', required=True)

    return parser.parse_args()


def main():
    args = parse_args()

    with tarfile.open(args.output, 'w') as dest:
        for jsrc in [j for j in args.input if j.endswith('.jsrc')]:
            with tarfile.open(jsrc, 'r') as src:
                for item in [m for m in src.getmembers() if m.name != '']:
                    if item.isdir():
                        dest.addfile(item)
                    else:
                        dest.addfile(item, src.extractfile(item))


if __name__ == '__main__':
    main()
