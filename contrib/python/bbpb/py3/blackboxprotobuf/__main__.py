# Copyright (c) 2018-2024 NCC Group Plc
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys
import json
import base64
import argparse

import typing
from typing import Any, Dict, Optional, Tuple

from .lib.exceptions import BlackboxProtobufException
from .lib import api
from .lib import payloads
from .lib.pytypes import TypeDefDict, Message


def main():
    # type: () -> int
    parser = argparse.ArgumentParser(description="Decode/Encode Protobuf Messages")
    parser.add_argument(
        "-e",
        "--encode",
        action="store_true",
        help="Switch to encode mode. Command decodes by default",
    )
    parser.add_argument(
        "-j",
        "--json-protobuf",
        action="store_true",
        help='Use JSON objects which may contain base64\'d protobuf and typedefs instead of just raw protobuf bytes for encoding input and decoding output. The JSON may have the following keys: "message", "typedef", "payload_encoding".',
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Use compact/non-pretty JSON output",
    )
    parser.add_argument(
        "-pe",
        "--payload-encoding",
        action="store",
        help="Override the wrapper encoding for the payload, such as gzip or grpc",
    )
    parser.add_argument(
        "-it",
        "--input-type",
        action="store",
        help="File to read the typedef from instead of stdin",
    )
    parser.add_argument(
        "-ot",
        "--output-type",
        action="store",
        help='(Decoding) File to write the typedef to instead of stdout. This file may be a plain JSON typedef or a json object with "typedef" and "payload_encoding" fields.',
    )
    parser.add_argument(
        "-r",
        "--raw-decode",
        action="store_true",
        help="(Decoding) Output just the decoded JSON and no type information.",
    )

    args = parser.parse_args()

    message = None  # type:  str | bytes | Message | None
    typedef = None  # type: TypeDefDict | None
    payload_encoding = None  # type: str | None

    if args.input_type:
        typedef, payload_encoding = _read_input_typedef_arg(args)

    if args.payload_encoding:
        payload_encoding = args.payload_encoding

    input_data = _read_input(args)
    if args.encode:
        input_json = json.loads(input_data)
        message, typedef, payload_encoding = _read_input_json_encoding(
            args, input_json, typedef, payload_encoding
        )
    elif args.json_protobuf:
        input_json = json.loads(input_data)
        message, typedef, payload_encoding = _read_input_json_decoding(
            args, input_json, typedef, payload_encoding
        )
    else:
        message = input_data

    if payload_encoding is None:
        payload_encoding = "none"

    # Start basic, raw protobuf decoding
    if args.encode:
        if not isinstance(message, dict):
            sys.stderr.write("Error did not get a valid message to encode")
            return 1
        if typedef is None:
            sys.stderr.write("Encoding requires a valid type definition")
            return 1
        return _encode(args, message, typedef, payload_encoding)
    else:
        if isinstance(message, str):
            message = message.encode("utf-8")
        if not isinstance(message, bytes):
            sys.stderr.write("Error did not get a valid message to decode")
            return 1
        return _decode(args, message, typedef, payload_encoding)


# Reads input from the location from args
# Does not handle any JSON decoding
def _read_input(args):
    # type: (argparse.Namespace) -> str | bytes
    if args.encode or args.json_protobuf:
        # Text
        return sys.stdin.read()
    else:
        # Binary
        return sys.stdin.buffer.read()


# Writes output to the location from args
# Does not handle any JSON encoding
def _write_output(args, data):
    # type: (argparse.Namespace, str | bytes) -> None
    if isinstance(data, str):
        # Text
        sys.stdout.write(data)
    else:
        # Binary
        sys.stdout.buffer.write(data)


def _read_input_typedef_arg(args):
    # type: (argparse.Namespace) -> Tuple[TypeDefDict, Optional[str]]
    with open(args.input_type, "r") as f:
        input_json = json.load(f)
    if "typedef" in input_json:
        return input_json.get("typedef"), input_json.get("payload_encoding")
    else:
        # Return whole paylaod as typedef, no encoding
        return input_json, None


def _write_output_typedef_arg(args, typedef):
    # type: (argparse.Namespace, Dict[str, str | TypeDefDict]) -> None
    with open(args.output_type, "w") as f:
        f.write(_to_json(args, typedef))


def _to_json(args, data):
    # type: (argparse.Namespace, Dict[str, Any]) -> str
    if args.compact:
        return json.dumps(data)
    else:
        return json.dumps(data, indent=2)


def _read_input_json_encoding(args, input_json, typedef, payload_encoding):
    # type: (argparse.Namespace, Message | Dict[str, str | Message | TypeDefDict], Optional[TypeDefDict], Optional[str]) -> Tuple[Message, Optional[TypeDefDict], Optional[str]]
    if typedef is None and "typedef" not in input_json:
        sys.stderr.write(
            "Error: Did not get a typedef from --input-type or stdin. A typedef is required for encoding\n"
        )
        sys.exit(1)

    message = typing.cast(Message | None, input_json.get("message"))
    if message is None:
        # Whole input is message. We already checked to make sure we have a typedef, so we can ditch it
        return typing.cast(Message, input_json), typedef, None

    if typedef is None:
        typedef = typing.cast(TypeDefDict | None, input_json.get("typedef"))

    if payload_encoding is None:
        json_payload_encoding = input_json.get("payload_encoding")
        if isinstance(json_payload_encoding, str):
            payload_encoding = json_payload_encoding
        elif json_payload_encoding is None:
            payload_encoding = None
        else:
            sys.stderr.write(
                "Warn: Payload encoding must be a string value: %r" % payload_encoding
            )
            payload_encoding = None

    return message, typedef, payload_encoding


def _read_input_json_decoding(args, input_json, typedef, payload_encoding):
    # type: (argparse.Namespace, Dict[str, TypeDefDict | str], Optional[TypeDefDict], Optional[str]) -> Tuple[bytes, Optional[TypeDefDict], Optional[str]]
    # Return message, typedef, payload_encoding
    message = typing.cast(str | None, input_json.get("protobuf_data"))
    if message is None:
        sys.stderr.write('Error: Did not get a "protobuf_data" attribute in input JSON')
        sys.exit(1)

    if typedef is None:
        typedef = typing.cast(TypeDefDict | None, input_json.get("typedef"))

    if payload_encoding is None:
        payload_encoding = typing.cast(str | None, input_json.get("payload_encoding"))

    # base64 decode if protobuf_json when decoding
    protobuf_data = base64.b64decode(message)

    return protobuf_data, typedef, payload_encoding


def _encode(args, message, typedef, payload_encoding):
    # type: (argparse.Namespace, Message, TypeDefDict, Optional[str]) -> int
    if typedef is None:
        sys.stderr.write("Error: Cannot encode without a valid typedef")
        return 1

    if not payload_encoding:
        payload_encoding = "none"

    # Re jsonify so that bbpb can fix bytes
    message_json = json.dumps(message)

    protobuf_data = api.protobuf_from_json(message_json, typedef)

    data = payloads.encode_payload(protobuf_data, payload_encoding)

    if args.json_protobuf:
        json_out = {
            "protobuf_data": base64.b64encode(data).decode("ascii"),
            "typedef": typedef,  # Typedef is a bit redundant here
        }
        if payload_encoding != "none":
            json_out["payload_encoding"] = payload_encoding

        _write_output(args, _to_json(args, json_out))
    else:
        _write_output(args, data)
    return 0


def _decode(args, data, typedef, payload_encoding):
    # type: (argparse.Namespace, bytes, Optional[TypeDefDict], str) -> int
    if len(data) == 0:
        sys.stderr.write("Error: Input data cannot be empty\n")
        return 1

    # args.protobuf_json is already handled

    if payload_encoding:
        # Use provided payload encoding algorithm
        protobuf_data, payload_encoding = payloads.decode_payload(
            data, payload_encoding
        )
        message_json, output_typedef = api.protobuf_to_json(protobuf_data, typedef)
    else:
        # Have to guess the decoding algorithm
        decoders = payloads.find_decoders(data)

        for decode in decoders:
            try:
                protobuf_data, encoding_alg = decode(data)
            except BlackboxProtobufException:
                # The "none" algorithm should always succeed
                continue

            try:
                message_json, output_typedef = api.protobuf_to_json(
                    protobuf_data, typedef
                )
                break
            except BlackboxProtobufException as exc:
                if encoding_alg == "none":
                    raise exc

    message = json.loads(message_json)

    if args.output_type:
        output_typedef_data = {}  # type: Dict[str, TypeDefDict | str]
        output_typedef_data["typedef"] = output_typedef
        if payload_encoding != "none":
            output_typedef_data["payload_encoding"] = payload_encoding
        _write_output_typedef_arg(args, output_typedef_data)

    if args.raw_decode:
        output = message
    else:
        output = {
            "message": message,
            "typedef": output_typedef,
        }
        if payload_encoding != "none":
            output["payload_encoding"] = payload_encoding
    _write_output(args, _to_json(args, output))
    return 0


if __name__ == "__main__":
    sys.exit(main())
