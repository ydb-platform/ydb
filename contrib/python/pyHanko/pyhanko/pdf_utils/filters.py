"""
Implementation of stream filters for PDF.

Taken from PyPDF2 with modifications. See :ref:`here <pypdf2-license>`
for the original license of the PyPDF2 project.

Note that not all decoders specified in the standard are supported.
In particular ``/LZWDecode`` and the various JPEG-based decoders are missing.
"""

import binascii
import re
import struct
import zlib
from io import BytesIO

from .crypt.api import SecurityHandler
from .misc import PdfStreamError, Singleton

__all__ = [
    'Decoder',
    'ASCII85Decode',
    'ASCIIHexDecode',
    'FlateDecode',
    'get_generic_decoder',
]

decompress = zlib.decompress
compress = zlib.compress


class Decoder:
    """
    General filter/decoder interface.
    """

    def decode(self, data: bytes, decode_params: dict) -> bytes:
        """
        Decode a stream.

        :param data:
            Data to decode.
        :param decode_params:
            Decoder parameters, sourced from the ``/DecoderParams`` entry
            associated with this filter.
        :return:
            Decoded data.
        """
        raise NotImplementedError

    def encode(self, data: bytes, decode_params: dict) -> bytes:
        """
        Encode a stream.

        :param data:
            Data to encode.
        :param decode_params:
            Encoder parameters, sourced from the ``/DecoderParams`` entry
            associated with this filter.
        :return:
            Encoded data.
        """
        raise NotImplementedError


def _png_decode(data: memoryview, columns):
    output = BytesIO()
    # PNG prediction can vary from row to row
    rowlength = columns + 1
    assert len(data) % rowlength == 0

    prev_result = bytes(rowlength - 1)
    for row in range(len(data) // rowlength):
        rowdata = data[(row * rowlength) : ((row + 1) * rowlength)]
        filter_byte = rowdata[0]
        result_row = bytearray(rowlength - 1)
        if filter_byte == 0:
            pass
        elif filter_byte == 1:
            pairs = zip(rowdata[2:], rowdata[1:])
            result_row[0] = rowdata[1]
            for i, (x, y) in enumerate(pairs):
                result_row[i + 1] = (x + y) % 256
        elif filter_byte == 2:
            pairs = zip(rowdata[1:], prev_result)
            for i, (x, y) in enumerate(pairs):
                result_row[i] = (x + y) % 256
        else:
            # unsupported PNG filter
            raise NotImplementedError("Unsupported PNG filter %r" % filter_byte)
        prev_result = result_row
        output.write(result_row)
    return output.getvalue()


class FlateDecode(Decoder, metaclass=Singleton):
    """
    Implementation of the ``/FlateDecode`` filter.

    .. warning::
        Currently not all predictor values are supported. This may cause
        problems when extracting image data from PDF files.
    """

    def decode(self, data: bytes, decode_params):
        # there's lots of slicing ahead, so let's reduce copying overhead
        data = memoryview(decompress(data))
        predictor = 1
        if decode_params:
            try:
                predictor = decode_params.get("/Predictor", 1)
            except AttributeError:
                pass  # usually an array with a null object was read

        # predictor 1 == no predictor
        if predictor == 1:
            return data

        columns = decode_params["/Columns"]
        # PNG prediction:
        if 10 <= predictor <= 15:
            return _png_decode(data, columns)
        else:
            # unsupported predictor
            raise NotImplementedError(
                "Unsupported flatedecode predictor %r" % predictor
            )

    def encode(self, data, decode_params=None):
        # TODO support the parameters in the spec
        return compress(data)


# TODO check boundary conditions in PDF spec

WS_REGEX = re.compile(b'\\s+')
ASCII_HEX_EOD_MARKER = b'>'


class ASCIIHexDecode(Decoder, metaclass=Singleton):
    """
    Wrapper around :func:`binascii.hexlify` that implements the
    :class:`.Decoder` interface.
    """

    def encode(self, data: bytes, decode_params=None) -> bytes:
        return binascii.hexlify(data) + b'>'

    def decode(self, data, decode_params=None):
        data, _ = data.split(ASCII_HEX_EOD_MARKER, 1)
        data = WS_REGEX.sub(b'', data)
        return binascii.unhexlify(data)


# TODO reimplement LZW decoder

ASCII_85_EOD_MARKER = b'~>'
POWS = tuple(85**p for p in (4, 3, 2, 1, 0))


class ASCII85Decode(Decoder, metaclass=Singleton):
    """
    Implementation of the base 85 encoding scheme specified in ISO 32000-1.
    """

    def encode(self, data: bytes, decode_params=None) -> bytes:
        # BytesIO is quite clever, in that it doesn't copy things until modified
        data_stm = BytesIO(data)
        out = BytesIO()

        while True:
            grp = data_stm.read(4)
            if not grp:
                break
            # This needs to happen before applying padding!
            # See § 7.4.3 in ISO 32000-1
            if grp == b'\0\0\0\0':
                out.write(b'z')
                continue

            bytes_read = len(grp)
            if bytes_read < 4:
                grp += b'\0' * (4 - bytes_read)
                pows = POWS[: bytes_read + 1]
            else:
                pows = POWS

            # write 5 chars in base85
            (grp_int,) = struct.unpack('>L', grp)
            for p in pows:
                digit, grp_int = divmod(grp_int, p)
                # use chars from 0x21 to 0x75
                out.write(bytes((digit + 0x21,)))
        out.write(ASCII_85_EOD_MARKER)
        return out.getvalue()

    def decode(self, data, decode_params=None):
        data, _ = data.split(ASCII_85_EOD_MARKER, 1)
        data_stm = BytesIO(WS_REGEX.sub(b'', data))
        out = BytesIO()
        while True:
            next_char = data_stm.read(1)
            if not next_char:
                break
            if next_char == b'z':
                out.write(b'\0\0\0\0')
                continue
            rest = data_stm.read(4)
            if not rest:  # pragma: nocover
                raise PdfStreamError(
                    'Nonzero ASCII85 group must have at least two digits.'
                )

            grp = next_char + rest
            grp_result = 0
            p = 0  # make the linter happy
            # convert back from base 85 to int
            for digit, p in zip(grp, POWS):
                digit -= 0x21
                if 0 <= digit < 85:
                    grp_result += p * digit
                else:  # pragma: nocover
                    raise PdfStreamError(
                        'Bytes in ASCII85 data must lie beteen 0x21 and 0x75.'
                    )
            # 85 and 256 are coprime, so the last digit will always be off by
            # one if we had to throw away a multiple of 256 in the encoding
            # step (due to padding).
            if len(grp) < 5:
                grp_result += p

            # Finally, pack the integer into a 4-byte unsigned int
            # (potentially need to cut off some excess digits)
            decoded = struct.pack('>L', grp_result)
            out.write(decoded[: len(grp) - 1])
        return out.getvalue()


class CryptFilterDecoder(Decoder):
    def __init__(self, handler: SecurityHandler):
        self.handler = handler

    def decode(self, data: bytes, decode_params: dict) -> bytes:
        from .crypt import IDENTITY

        cf_name = decode_params.get('/Name', IDENTITY)
        cf = self.handler.get_stream_filter(name=cf_name)
        # the spec explicitly tells us to use the global key here, go figure
        # (clause § 7.4.10 in both 32k-1 and 32k-2)
        return cf.decrypt(cf.shared_key, data, params=decode_params)

    def encode(self, data: bytes, decode_params: dict) -> bytes:
        from .crypt import IDENTITY

        cf_name = decode_params.get('/Name', IDENTITY)
        cf = self.handler.get_stream_filter(name=cf_name)
        return cf.encrypt(cf.shared_key, data, params=decode_params)


DECODERS = {
    '/FlateDecode': FlateDecode,
    '/Fl': FlateDecode,
    '/ASCIIHexDecode': ASCIIHexDecode,
    '/AHx': ASCIIHexDecode,
    '/ASCII85Decode': ASCII85Decode,
    '/A85': ASCII85Decode,
}


def get_generic_decoder(name: str) -> Decoder:
    """
    Instantiate a specific stream filter decoder type by (PDF) name.

    The following names are recognised:

    * ``/FlateDecode`` or ``/Fl`` for the decoder implementing Flate
       compression.
    * ``/ASCIIHexDecode`` or ``/AHx`` for the decoder that converts bytes to
      their hexadecimal representations.
    * ``/ASCII85Decode`` or ``/A85`` for the decoder that converts byte strings
      to a base-85 textual representation.

    .. warning::
        ``/Crypt`` is a special case because it requires access to the
        document's security handler.

    .. warning::
        LZW compression is currently unsupported, as are most compression
        methods that are used specifically for image data.

    :param name:
        Name of the decoder to instantiate.
    """

    try:
        cls = DECODERS[name]
    except KeyError:
        raise NotImplementedError(f"Stream filter '{name}' is not supported.")
    return cls()
