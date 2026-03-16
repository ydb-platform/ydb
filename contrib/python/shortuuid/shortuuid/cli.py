import argparse
import sys
from typing import Any
from uuid import UUID

from .main import decode
from .main import encode
from .main import uuid


def encode_cli(args: argparse.Namespace):
    print(encode(args.uuid))


def decode_cli(args: argparse.Namespace):
    print(str(decode(args.shortuuid, legacy=args.legacy)))


def cli(*args: Any) -> None:
    parser = argparse.ArgumentParser(
        description="Generate, encode and decode shortuuids",
        epilog="top-level command generates a random shortuuid",
    )

    subparsers = parser.add_subparsers(help="sub-command help")

    encode_parser = subparsers.add_parser(
        "encode", help="Encode a UUID into a short UUID", description=encode.__doc__
    )
    encode_parser.add_argument("uuid", type=UUID, help="UUID to be encoded")
    encode_parser.set_defaults(func=encode_cli)

    decode_parser = subparsers.add_parser(
        "decode", help="Decode a short UUID into a UUID", description=decode.__doc__
    )
    decode_parser.add_argument("shortuuid", type=str, help="Short UUID to be decoded")
    decode_parser.add_argument("--legacy", action="store_true")
    decode_parser.set_defaults(func=decode_cli)

    passed_args = parser.parse_args(*args)

    if hasattr(passed_args, "func"):
        passed_args.func(passed_args)
    else:
        # Maintain legacy behaviour
        print(uuid())


if __name__ == "__main__":
    cli(sys.argv[1:])
