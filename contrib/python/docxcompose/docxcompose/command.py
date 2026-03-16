import os.path
import sys
from argparse import ArgumentParser

from docx import Document

from docxcompose.composer import Composer


def setup_parser():
    parser = ArgumentParser(description="compose multiple docx files into one file.")
    parser.add_argument(
        "master",
        help="path to master template that defines styles, " "headings and so on",
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="path to one or more word-files to be appended " "to the master template",
        metavar="file",
    )
    parser.add_argument(
        "-o",
        "--output-document",
        dest="ouput_document",
        default="composed.docx",
        help="path to the output file",
        metavar="file",
    )
    return parser


def require_valid_file(parser, path):
    if not os.path.isfile(path):
        parser.error(message="file not found {}".format(path))


def parse_args(parser, args):
    parsed_args = parser.parse_args(args=args)

    require_valid_file(parser, parsed_args.master)
    for file_path in parsed_args.files:
        require_valid_file(parser, file_path)

    return parsed_args


def compose_files(parser, parsed_args):
    composer = Composer(Document(parsed_args.master))
    for slave_path in parsed_args.files:
        composer.append(Document(slave_path))

    composer.save(parsed_args.ouput_document)
    parser.exit(
        message="successfully composed file at {}\n".format(parsed_args.ouput_document)
    )


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = setup_parser()
    parsed_args = parse_args(parser, args)
    compose_files(parser, parsed_args)
