import sys

from yql.essentials.tools.ysondiff.lib import compare_contents


def compare_files(left_path, right_path):
    with open(left_path, 'rb') as left_file:
        left = left_file.read()
    with open(right_path, 'rb') as right_file:
        right = right_file.read()
    return compare_contents(left, right, fromfile=left_path, tofile=right_path)


def main(argv=None):
    argv = argv if argv is not None else sys.argv
    if len(argv) != 3:
        sys.stderr.write('Usage: %s <fileone> <filetwo>\n' % argv[0])
        return 2

    try:
        diff = compare_files(argv[1], argv[2])
    except Exception as exc:
        sys.stderr.write('ysondiff failed: %s: %s\n' % (type(exc).__name__, exc))
        return 255

    if diff is None:
        return 0

    sys.stderr.write('Files differ after YSON/JSON normalization\n')
    sys.stderr.writelines(diff)
    return 1


if __name__ == '__main__':
    sys.exit(main())
