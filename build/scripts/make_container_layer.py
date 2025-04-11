import os
import sys

# Explicitly enable local imports
# Don't forget to add imported scripts to inputs of the calling command!
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import container  # 1


class UserError(Exception):
    pass


def entry():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('-s', '--squashfs-path', required=True)
    parser.add_argument('input', nargs='*')

    args = parser.parse_args()

    return container.join_layers(args.input, args.output, args.squashfs_path)


if __name__ == '__main__':
    sys.exit(entry())
