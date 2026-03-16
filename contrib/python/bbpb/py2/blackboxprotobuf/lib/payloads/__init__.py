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

""" The payloads module is intended to handle different encodings of the
    protobuf data, such as compression and grpc header. """

from blackboxprotobuf.lib.exceptions import BlackboxProtobufException
from . import gzip, grpc

import six

if six.PY3:
    from typing import List, Callable, Tuple, Optional


# Returns an ordered list of potential decoders, from most specific to least specific
# The consumer should loop through each decoder, try to decode it and then try
# to decode as a protobuf. This should minimize the chance of a false positive
# on any decoders
def find_decoders(buf):
    # type: (bytes) -> List[Callable[[bytes], Tuple[bytes | list[bytes], str]]]
    # In the future, we can take into account content-type too, such as for
    # grpc, but we risk false negatives
    decoders = []  # type: List[Callable[[bytes], Tuple[bytes | list[bytes], str]]]

    if gzip.is_gzip(buf):
        decoders.append(gzip.decode_gzip)

    if grpc.is_grpc(buf):
        decoders.append(grpc.decode_grpc)

    decoders.append(_none_decoder)
    return decoders


def _none_decoder(buf):
    # type: (bytes) -> Tuple[bytes, str]
    return buf, "none"


# Decoder by name
def decode_payload(buf, decoder):
    # type: (bytes, Optional[str]) -> Tuple[bytes | list[bytes], str]
    if decoder is None:
        return buf, "none"
    decoder = decoder.lower()
    if decoder == "none":
        return buf, "none"
    elif decoder.startswith("grpc"):
        return grpc.decode_grpc(buf)
    elif decoder == "gzip":
        return gzip.decode_gzip(buf)
    else:
        raise BlackboxProtobufException("Unknown decoder: " + decoder)


# Encode by name, should pass in the results from the decode function
def encode_payload(buf, encoder):
    # type: (bytes | list[bytes], Optional[str]) -> bytes
    if encoder is None:
        encoder = "none"

    encoder = encoder.lower()
    if encoder == "none":
        if isinstance(buf, list):
            raise BlackboxProtobufException(
                "Cannot encode multiple buffers with none/missing encoding"
            )
        return buf
    elif encoder.startswith("grpc"):
        return grpc.encode_grpc(buf, encoder)
    elif encoder == "gzip":
        return gzip.encode_gzip(buf)
    else:
        raise BlackboxProtobufException("Unknown encoder: " + encoder)
