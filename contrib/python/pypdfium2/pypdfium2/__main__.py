# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import sys
import argparse
import importlib
from pypdfium2.version import PYPDFIUM_INFO, PDFIUM_INFO
from pypdfium2._cli._parsers import setup_logging

from pypdfium2_raw.bindings import _libs

SubCommands = {
    "arrange":        "Rearrange/merge documents",
    "attachments":    "List/extract/edit embedded files",
    "extract-images": "Extract images",
    "extract-text":   "Extract text",
    "imgtopdf":       "Convert images to PDF",
    "pageobjects":    "Print info on pageobjects",
    "pdfinfo":        "Print info on document and pages",
    "render":         "Rasterize pages",
    "tile":           "Tile pages (N-up)",
    "toc":            "Print table of contents",
}

CmdToModule = {n: importlib.import_module(f"pypdfium2._cli.{n.replace('-', '_')}") for n in SubCommands}


def get_parser():
    
    main_parser = argparse.ArgumentParser(
        prog = "pypdfium2",
        formatter_class = argparse.RawTextHelpFormatter,
        description = "Command line interface to the pypdfium2 library (Python binding to PDFium)",
    )
    main_parser.add_argument(
        "--version", "-v",
        action = "version",
        version = f"pypdfium2 {PYPDFIUM_INFO}\n" f"pdfium {PDFIUM_INFO} at {_libs['pdfium']._name}"
    )
    subparsers = main_parser.add_subparsers(dest="subcommand")
    
    for name, help in SubCommands.items():
        mod = CmdToModule[name]
        desc = getattr(mod, "PARSER_DESC", None)
        desc = (help + "\n\n" + desc) if desc else help
        subparser = subparsers.add_parser(
            name, help=help, description=desc,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        mod.attach(subparser)
    
    return main_parser


def api_main(raw_args=sys.argv[1:]):
    
    parser = get_parser()
    args = parser.parse_args(raw_args)
    
    if not args.subcommand:
        parser.print_help()
        return
    
    CmdToModule[args.subcommand].main(args)


def cli_main():
    setup_logging()
    api_main()


if __name__ == "__main__":
    cli_main()
