#!/usr/bin/env python3
#
#
# Library to extract Exif information from digital camera image files.
# https://github.com/ianare/exif-py
#
#
# Copyright (c) 2002-2007 Gene Cash
# Copyright (c) 2007-2025 Ianaré Sévi and contributors
#
# See LICENSE.txt file for licensing information
# See ChangeLog.rst file for all contributors and changes
#

"""
Runs Exif tag extraction in command line.
"""

import argparse
import sys
import timeit

from exifread import __version__, exif_log, process_file
from exifread.core.exceptions import ExifError
from exifread.tags.fields import FIELD_DEFINITIONS

logger = exif_log.get_logger()


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="EXIF.py", description="Extract EXIF information from digital image files."
    )
    parser.add_argument(
        "files",
        metavar="FILE",
        type=str,
        nargs="+",
        help="files to process",
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version="EXIF.py Version %s on Python%s" % (__version__, sys.version_info[0]),
        help="Display version information and exit",
    )
    parser.add_argument(
        "-q",
        "--quick",
        action="store_false",
        dest="detailed",
        help="Do not process MakerNotes and do not extract thumbnails",
    )
    parser.add_argument(
        "-t",
        "--tag",
        type=str,
        dest="stop_tag",
        help="Stop processing when this tag is retrieved.",
    )
    parser.add_argument(
        "-s",
        "--strict",
        action="store_true",
        dest="strict",
        help="Run in strict mode (stop on errors).",
    )
    parser.add_argument(
        "-b",
        "--builtin",
        action="store_true",
        dest="builtin_types",
        help="Convert IfdTag values to built-in Python variable types",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        dest="debug",
        help="Run in debug mode (display extra info).",
    )
    parser.add_argument(
        "-c",
        "--color",
        action="store_true",
        dest="color",
        help="Output in color (only works with debug on POSIX).",
    )
    args = parser.parse_args()
    return args


def run_cli(args: argparse.Namespace) -> None:
    """Extract tags based on options (args)."""

    exif_log.setup_logger(args.debug, args.color)

    # output info for each file
    for filename in args.files:
        # avoid errors when printing to console
        escaped_fn = filename.encode(
            sys.getfilesystemencoding(), "surrogateescape"
        ).decode()

        file_start = timeit.default_timer()
        try:
            with open(escaped_fn, "rb") as img_file:
                logger.info("Opening: %s", escaped_fn)

                tag_start = timeit.default_timer()
                # get the tags
                data = process_file(
                    img_file,
                    stop_tag=args.stop_tag,
                    details=args.detailed,
                    strict=args.strict,
                    debug=args.debug,
                    extract_thumbnail=args.detailed,
                    builtin_types=args.builtin_types,
                )
                tag_stop = timeit.default_timer()

        except IOError:
            logger.error("'%s' is unreadable", escaped_fn)
            continue

        if not data:
            logger.warning("No EXIF information found")
            print()
            continue

        if "JPEGThumbnail" in data:
            logger.info("File has JPEG thumbnail")
            del data["JPEGThumbnail"]
        if "TIFFThumbnail" in data:
            logger.info("File has TIFF thumbnail")
            del data["TIFFThumbnail"]

        for field in sorted(data):
            value = data[field]
            try:
                if args.builtin_types:
                    logger.info("%s (%s): %r", field, type(value).__name__, value)
                else:
                    logger.info(
                        "%s (%s): %s",
                        field,
                        FIELD_DEFINITIONS[value.field_type][1],
                        value.printable,
                    )
            except (ExifError, ValueError):
                logger.error("%s: %s", field, str(value))

        file_stop = timeit.default_timer()

        logger.debug("Tags processed in %s seconds", tag_stop - tag_start)
        logger.debug("File processed in %s seconds", file_stop - file_start)
        print()


def main() -> None:
    run_cli(get_args())


if __name__ == "__main__":
    main()
