#!/usr/bin/env python3
# tifffile/tiff2fsspec.py

"""Write fsspec ReferenceFileSystem for TIFF file."""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from typing import Any

try:
    from .tifffile import tiff2fsspec
except ImportError:
    try:
        from tifffile.tifffile import tiff2fsspec
    except ImportError:
        from tifffile import tiff2fsspec  # noqa: PLW0406


def main(argv: list[str] | None = None) -> int:
    """Tiff2fsspec command line usage main function."""
    parser = argparse.ArgumentParser(
        prog='tiff2fsspec',
        description='Write fsspec ReferenceFileSystem for TIFF file.',
        epilog='Example: tiff2fsspec ./test.ome.tif https://server.com/path/',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('tifffile', help='path to the local TIFF input file')
    parser.add_argument(
        'url', help='remote URL of TIFF file without file name'
    )
    parser.add_argument('--out', help='path to the JSON output file')
    parser.add_argument('--series', type=int, help='index of series in file')
    parser.add_argument('--level', type=int, help='index of level in series')
    parser.add_argument(
        '--key', type=int, help='index of page in file or series'
    )
    parser.add_argument(
        '--chunkmode',
        metavar='mode',
        help='mode used for chunking (int or string, e.g. "pages")',
    )
    parser.add_argument(
        '--fillvalue',
        type=float,
        help='fill value for missing data',
    )
    parser.add_argument(
        '--squeeze',
        action=argparse.BooleanOptionalAction,
        help='squeeze length-1 dimensions from zarr store',
    )
    parser.add_argument(
        '--groupname',
        help='name of the zarr group in the fsspec output',
    )
    parser.add_argument(
        '--zattrs',
        metavar='JSON',
        help='custom Zarr attributes as a JSON object string',
    )
    parser.add_argument(
        '--ref-version',
        dest='version',
        type=int,
        help='version of ReferenceFileSystem spec',
    )
    args = parser.parse_args(None if argv is None else argv[1:])

    chunkmode: int | str | None = args.chunkmode
    if chunkmode is not None:
        with contextlib.suppress(ValueError):
            chunkmode = int(chunkmode)

    zattrs: dict[str, Any] | None = None
    if args.zattrs is not None:
        try:
            zattrs = json.loads(args.zattrs)
        except json.JSONDecodeError as exc:
            parser.error(f'--zattrs is not valid JSON: {exc}')
        if not isinstance(zattrs, dict):
            parser.error(
                '--zattrs must be a JSON object, not an array or scalar'
            )

    try:
        tiff2fsspec(
            args.tifffile,
            args.url,
            out=args.out,
            key=args.key,
            series=args.series,
            level=args.level,
            chunkmode=chunkmode,
            fillvalue=args.fillvalue,
            squeeze=args.squeeze,
            groupname=args.groupname,
            zattrs=zattrs,
            version=args.version,
        )
    except Exception as exc:
        print(f'{args.tifffile}: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
