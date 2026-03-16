# -*- coding: utf-8 -*-

"""Command line interface

"""

import argparse
import json
import sys

from .__version__ import __version__
from .simple_json import simple_json_from_html_string, have_node


def main():
    parser = argparse.ArgumentParser(
        description="Extract article data from a HTML file using either Mozilla's Readability.js package or a simplified python-only alternative.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--input-file",
        default="-",
        help="Path to input file containing HTML, use '-' for stdin.",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        default="-",
        help="Path to file to output the article data to as JSON, use '-' for stdout.",
    )
    parser.add_argument(
        "-c",
        "--content-digests",
        action="store_true",
        help="Add a 'data-content-digest' attribute containing a SHA256-based digest of the element's contents to each HTML element in the plain_content output.",
    )
    parser.add_argument(
        "-n",
        "--node-indexes",
        action="store_true",
        help="Add a 'data-node-index' attribute containing a hierarchical representation of the element's position in the HTML structure each HTML element in the plain_content output.",
    )
    parser.add_argument(
        "-p",
        "--use-python-parser",
        action="store_true",
        help="Use the pure-python 'plain_html' parser included in this project rather than Mozilla's Readability.js.",
    )
    parser.add_argument(
        "-V",
        "--version",
        help="Show version and exit",
        action="version",
        version=f"{__version__} (Readability.js supported: {'yes' if have_node() else 'no'})",
    )

    args = parser.parse_args()

    # Open input file or stream
    if args.input_file == "-":
        if hasattr(sys.stdin, "reconfigure"):
            sys.stdin.reconfigure(encoding="utf-8", errors="replace")
        input_file = sys.stdin
    else:
        input_file = open(args.input_file, encoding="utf-8", errors="replace")  # pylint: disable=consider-using-with

    # Read from input then close if appropriate
    html = input_file.read()
    if not input_file.isatty():
        input_file.close()

    article = simple_json_from_html_string(
        html,
        content_digests=args.content_digests,
        node_indexes=args.node_indexes,
        use_readability=(not args.use_python_parser),
    )

    # Open output file or stream
    if args.output_file == "-":
        output_file = sys.stdout
    else:
        output_file = open(args.output_file, "w", encoding="utf-8")  # pylint: disable=consider-using-with

    # Write to output then close if appropriate
    json.dump(article, output_file, ensure_ascii=False)
    if not output_file.isatty():
        output_file.close()


if __name__ == "__main__":
    main()
