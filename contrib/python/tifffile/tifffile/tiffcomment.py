#!/usr/bin/env python3
# tifffile/tiffcomment.py

"""Print or replace ImageDescription in first page of TIFF file."""

from __future__ import annotations

import argparse
import contextlib
import sys

try:
    from .tifffile import tiffcomment
except ImportError:
    try:
        from tifffile.tifffile import tiffcomment
    except ImportError:
        from tifffile import tiffcomment  # noqa: PLW0406


def main(argv: list[str] | None = None) -> int:
    """Tiffcomment command line usage main function."""
    parser = argparse.ArgumentParser(
        prog='tiffcomment',
        description=(
            'Print or replace ImageDescription in first page of TIFF file.'
        ),
        epilog=(
            'Example: tiffcomment --set "my description" image.tif\n'
            'When multiple files are given with --set or --set-file,'
            ' the same comment is written to all of them.'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        'files',
        nargs='+',
        metavar='file',
        help='TIFF file(s) to read or modify',
    )
    comment_group = parser.add_mutually_exclusive_group()
    comment_group.add_argument(
        '--set',
        dest='comment',
        metavar='comment',
        help='replacement comment string',
    )
    comment_group.add_argument(
        '--set-file',
        dest='comment_file',
        type=argparse.FileType('rb'),
        metavar='file',
        help='path to a file whose raw bytes replace the comment',
    )
    parser.add_argument(
        '--page',
        dest='pageindex',
        type=int,
        metavar='N',
        help='index of page to read or modify (default: 0)',
    )
    parser.add_argument(
        '--tag',
        dest='tagcode',
        metavar='code',
        help='tag code or name to read or modify (default: ImageDescription)',
    )
    args = parser.parse_args(None if argv is None else argv[1:])

    comment: bytes | None
    if args.comment_file is not None:
        with args.comment_file:
            comment = args.comment_file.read()
    elif args.comment is not None:
        try:
            comment = args.comment.encode('ascii')
        except UnicodeEncodeError:
            parser.error(
                'comment contains non-ASCII characters;'
                ' use --set-file with a pre-encoded file'
            )
            # comment = b''  # unreachable; satisfies mypy
    else:
        comment = None

    tagcode: int | str | None = args.tagcode
    if tagcode is not None:
        with contextlib.suppress(ValueError):
            tagcode = int(tagcode)

    ret = 0
    for file in args.files:
        try:
            result = tiffcomment(
                file, comment, pageindex=args.pageindex, tagcode=tagcode
            )
        except Exception as exc:
            print(f'{file}: {exc}', file=sys.stderr)
            ret = 1
        else:
            if result:
                if isinstance(result, bytes):
                    result = result.decode(errors='replace')
                if len(args.files) > 1:
                    print(f'# {file}')
                print(result)
                if len(args.files) > 1:
                    print()
    return ret


if __name__ == '__main__':
    sys.exit(main())
