from __future__ import annotations

import os
from argparse import ArgumentParser

import barcode
from barcode.version import version
from barcode.writer import BaseWriter
from barcode.writer import ImageWriter
from barcode.writer import SVGWriter

IMG_FORMATS = ("BMP", "GIF", "JPEG", "MSP", "PCX", "PNG", "TIFF", "XBM")


def list_types(args, parser=None) -> None:
    print("\npython-barcode available barcode formats:")
    print(", ".join(barcode.PROVIDED_BARCODES))
    print("\n")
    print("Available image formats")
    print("Standard: svg")
    if ImageWriter is not None:
        print("Pillow:", ", ".join(IMG_FORMATS))
    else:
        print("Pillow: disabled")
    print("\n")


def create_barcode(args, parser) -> None:
    args.type = args.type.upper()
    if args.type != "SVG" and args.type not in IMG_FORMATS:
        parser.error(f"Unknown type {args.type}. Try list action for available types.")
    args.barcode = args.barcode.lower()
    if args.barcode not in barcode.PROVIDED_BARCODES:
        parser.error(
            f"Unknown barcode {args.barcode}. Try list action for available barcodes."
        )
    if args.type != "SVG":
        assert ImageWriter is not None
        opts = {"format": args.type}
        writer: BaseWriter = ImageWriter()
    else:
        opts = {"compress": args.compress}
        writer = SVGWriter()
    out = os.path.normpath(os.path.abspath(args.output))
    name = barcode.generate(args.barcode, args.code, writer, out, opts, args.text)
    print(f"New barcode saved as {name}.")


def main() -> None:
    msg = []
    if ImageWriter is None:
        msg.append("Image output disabled (Pillow not found), --type option disabled.")
    else:
        msg.append(
            "Image output enabled, use --type option to give image "
            "format (png, jpeg, ...)."
        )
    parser = ArgumentParser(
        description="Create standard barcodes via cli.", epilog=" ".join(msg)
    )
    parser.add_argument(
        "-v", "--version", action="version", version="%(prog)s " + version
    )
    subparsers = parser.add_subparsers(title="Actions")
    create_parser = subparsers.add_parser(
        "create", help="Create a barcode with the given options."
    )
    create_parser.add_argument("code", help="Code to render as barcode.")
    create_parser.add_argument(
        "output", help="Filename for output without extension, e. g. mybarcode."
    )
    create_parser.add_argument(
        "-c",
        "--compress",
        action="store_true",
        help="Compress output, only recognized if type is svg.",
    )
    create_parser.add_argument(
        "-b", "--barcode", help="Barcode to use [default: %(default)s]."
    )
    create_parser.add_argument("--text", help="Text to show under the barcode.")
    if ImageWriter is not None:
        create_parser.add_argument(
            "-t", "--type", help="Type of output [default: %(default)s]."
        )
    list_parser = subparsers.add_parser(
        "list", help="List available image and code types."
    )
    list_parser.set_defaults(func=list_types)
    create_parser.set_defaults(
        type="svg", compress=False, func=create_barcode, barcode="code39", text=None
    )
    args = parser.parse_args()
    try:
        func = args.func
    except AttributeError:
        parser.error("You need to tell me what to do.")
    else:
        func(args, parser)


if __name__ == "__main__":
    main()
