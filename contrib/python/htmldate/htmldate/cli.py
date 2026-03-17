"""
Implementing a basic command-line interface.
"""

import argparse
import logging
import sys

from platform import python_version
from typing import Any, Optional, Union

from lxml.html import HtmlElement

from . import __version__
from .core import find_date
from .utils import fetch_url, is_wrong_document


def cli_examine(
    htmlstring: Union[str, HtmlElement],
    args: Any,
) -> Optional[str]:
    """Generic safeguards and triggers"""
    # safety check
    if is_wrong_document(htmlstring):
        sys.stderr.write("# ERROR: document is empty or too large\n")
        return None
    return find_date(
        htmlstring,
        extensive_search=not args.fast,
        original_date=args.original,
        verbose=args.verbose,
        min_date=args.mindate,
        max_date=args.maxdate,
    )


def parse_args(args: Any) -> Any:
    """Define parser for command-line arguments"""
    argsparser = argparse.ArgumentParser()
    argsparser.add_argument(
        "-f", "--fast", help="fast mode: disable extensive search", action="store_false"
    )
    argsparser.add_argument(
        "-i",
        "--inputfile",
        help="""name of input file for batch processing
                            (similar to wget -i)""",
        type=str,
    )
    argsparser.add_argument(
        "--original", help="original date prioritized", action="store_true"
    )
    argsparser.add_argument(
        "-min", "--mindate", help="earliest acceptable date (ISO 8601 YMD)", type=str
    )
    argsparser.add_argument(
        "-max", "--maxdate", help="latest acceptable date (ISO 8601 YMD)", type=str
    )
    argsparser.add_argument("-u", "--URL", help="custom URL download", type=str)
    argsparser.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true"
    )
    argsparser.add_argument(
        "--version",
        help="show version information and exit",
        action="version",
        version=f"Htmldate {__version__} - Python {python_version()}",
    )
    return argsparser.parse_args()


def process_args(args: Any) -> None:
    """Process the arguments passed on the command-line."""
    # verbosity
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    # input type
    if not args.inputfile:
        # URL as input
        if args.URL:
            htmlstring = fetch_url(args.URL)
            if htmlstring is None:
                sys.exit(f"No data for URL: {args.URL}" + "\n")
        # unicode check
        else:
            try:
                htmlstring = sys.stdin.read()
            except UnicodeDecodeError as err:
                sys.exit(f"Wrong buffer encoding: {str(err)}" + "\n")
        result = cli_examine(htmlstring, args)
        if result is not None:
            sys.stdout.write(result + "\n")

    # process input file line by line
    else:
        with open(args.inputfile, mode="r", encoding="utf-8") as inputfile:
            for line in inputfile:
                htmltext = fetch_url(line.strip())
                result = cli_examine(htmltext, args)  # type: ignore[arg-type]
                sys.stdout.write(f"{line.strip()}\t{result or 'None'}\n")


def main() -> None:
    """Run as a command-line utility."""
    # arguments
    args = parse_args(sys.argv[1:])
    # process input on STDIN
    process_args(args)


if __name__ == "__main__":
    main()
