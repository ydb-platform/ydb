"""A tool for converting SVG to PDF.

This module provides a high-level function for converting SVG files to PDF format
using the ReportLab graphics library. It also serves as the main entry point for
the svglib package.

The module includes:
- svg2pdf(): Convert SVG files to PDF programmatically.
- main(): Command-line interface for SVG to PDF conversion.
- Automatic file path handling and output generation.
"""

import argparse
import sys
import textwrap
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from os.path import basename, dirname, exists, splitext
from typing import Optional

from reportlab.graphics import renderPDF

from svglib import svglib

try:
    __version__ = version("svglib")
except PackageNotFoundError:
    __version__ = "unknown"


def svg2pdf(path: str, outputPat: Optional[str] = None) -> None:
    """Convert an SVG file to PDF format.

    High-level function that loads an SVG file, converts it to a ReportLab drawing,
    and saves it as a PDF file. Supports both .svg and .svgz (compressed SVG) files.

    Args:
        path: Path to the input SVG file (.svg or .svgz extension).
        outputPat: Optional output path pattern. Supports placeholders:
            - %(dirname)s: Directory of input file
            - %(basename)s: Full filename with extension
            - %(base)s: Filename without extension
            - %(ext)s: File extension
            - %(now)s: Current datetime object
            - %(format)s: Output format (always "pdf")
            Also supports {name} format strings.

    Returns:
        None. The PDF file is written to disk.

    Raises:
        FileNotFoundError: If the input SVG file does not exist.
        Exception: If SVG parsing or PDF generation fails.
        OSError: If the output directory is not writable.

    Examples:
        Basic conversion:
        >>> svg2pdf("input.svg")  # Creates "input.pdf"

        Custom output location:
        >>> svg2pdf("input.svg", "output.pdf")

        Using placeholders:
        >>> svg2pdf("path/file.svg", "%(dirname)s/converted/%(base)s.pdf")
        # Creates "path/converted/file.pdf"

        Timestamped output:
        >>> svg2pdf("file.svg", "backup/%(now.year)s-%(now.month)s-%(base)s.pdf")

    Note:
        The function will overwrite existing PDF files without warning.
        For compressed SVG files (.svgz), the file is automatically decompressed.
    """

    # derive output filename from output pattern
    file_info = {
        "dirname": dirname(path) or ".",
        "basename": basename(path),
        "base": basename(splitext(path)[0]),
        "ext": splitext(path)[1],
        "now": datetime.now(),
        "format": "pdf",
    }
    out_pattern = outputPat or "%(dirname)s/%(base)s.%(format)s"
    # allow classic %%(name)s notation
    out_path = out_pattern % file_info
    # allow also newer {name} notation
    out_path = out_path.format(**file_info)

    # generate a drawing from the SVG file
    try:
        drawing = svglib.svg2rlg(path)
    except:
        print("Rendering failed.")
        raise

    # save converted file
    if drawing:
        renderPDF.drawToFile(drawing, out_path, showBoundary=0)


# command-line usage stuff
def main() -> None:
    """Main entry point for the CLI."""
    ext = "pdf"
    ext_caps = ext.upper()
    format_args = dict(
        prog=basename(sys.argv[0]),
        version=__version__,
        ts_pattern="{{dirname}}/out-"
        "{{now.hour}}-{{now.minute}}-{{now.second}}-"
        "%(base)s",
        ext=ext,
        ext_caps=ext_caps,
    )
    format_args["ts_pattern"] += ".%s" % format_args["ext"]
    desc = "{prog} v. {version}\n".format(**format_args)
    desc += "A converter from SVG to {} (via ReportLab Graphics)\n".format(ext_caps)
    epilog = textwrap.dedent(
        """\
        examples:
          # convert path/file.svg to path/file.{ext}
          {prog} path/file.svg

          # convert file1.svg to file1.{ext} and file2.svgz to file2.{ext}
          {prog} file1.svg file2.svgz

          # convert file.svg to out.{ext}
          {prog} -o out.{ext} file.svg

          # convert all SVG files in path/ to PDF files with names like:
          # path/file1.svg -> file1.{ext}
          {prog} -o "%(base)s.{ext}" path/file*.svg

          # like before but with timestamp in the PDF files:
          # path/file1.svg -> path/out-12-58-36-file1.{ext}
          {prog} -o {ts_pattern} path/file*.svg

        issues/pull requests:
            https://github.com/deeplook/svglib
        """.format(**format_args)
    )
    p = argparse.ArgumentParser(
        description=desc,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument(
        "-v", "--version", help="Print version number and exit.", action="store_true"
    )

    p.add_argument(
        "-o",
        "--output",
        metavar="PATH_PAT",
        help="Set output path (incl. the placeholders: dirname, basename,"
        "base, ext, now) in both, %%(name)s and {name} notations.",
    )

    p.add_argument(
        "input",
        metavar="PATH",
        nargs="*",
        help="Input SVG file path with extension .svg or .svgz.",
    )

    args = p.parse_args()

    if args.version:
        print(__version__)
        sys.exit()

    if not args.input:
        p.print_usage()
        sys.exit()

    paths = [a for a in args.input if exists(a)]
    for path in paths:
        svg2pdf(path, outputPat=args.output)
