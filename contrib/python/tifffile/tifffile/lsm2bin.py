#!/usr/bin/env python3
# tifffile/lsm2bin.py

"""Convert [MP]TZCYX LSM file to series of BIN files."""

from __future__ import annotations

import argparse
import sys

try:
    from .tifffile import lsm2bin
except ImportError:
    try:
        from tifffile.tifffile import lsm2bin
    except ImportError:
        from tifffile import lsm2bin  # noqa: PLW0406


def main(argv: list[str] | None = None) -> int:
    """Lsm2bin command line usage main function."""
    parser = argparse.ArgumentParser(
        prog='lsm2bin',
        description='Convert [MP]TZCYX LSM file to series of BIN files.',
        epilog='Example: lsm2bin input.lsm output --tile 512 512',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('lsmfile', help='path to the LSM input file')
    parser.add_argument(
        'binfile',
        nargs='?',
        help='common name of output BIN files (default: lsmfile name)',
    )
    parser.add_argument(
        '--tile',
        nargs=2,
        type=int,
        metavar=('Y', 'X'),
        help='tile Y and X dimensions (default: 256 256)',
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='suppress progress output',
    )
    args = parser.parse_args(None if argv is None else argv[1:])

    if args.tile is not None and any(v <= 0 for v in args.tile):
        parser.error('--tile values must be positive integers')

    tile = (args.tile[0], args.tile[1]) if args.tile is not None else None

    try:
        lsm2bin(
            args.lsmfile,
            args.binfile,
            tile=tile,
            verbose=not args.quiet,
        )
    except Exception as exc:
        print(f'{args.lsmfile}: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
