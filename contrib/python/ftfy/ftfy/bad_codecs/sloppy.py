r"""
`ftfy.bad_codecs.sloppy` provides character-map encodings that fill their "holes"
in a messy but common way: by outputting the Unicode codepoints with the same
numbers.

This is incredibly ugly, and it's also in the HTML5 standard.

A single-byte encoding maps each byte to a Unicode character, except that some
bytes are left unmapped. In the commonly-used Windows-1252 encoding, for
example, bytes 0x81 and 0x8D, among others, have no meaning.

Python, wanting to preserve some sense of decorum, will handle these bytes
as errors. But Windows knows that 0x81 and 0x8D are possible bytes and they're
different from each other. It just hasn't defined what they are in terms of
Unicode.

Software that has to interoperate with Windows-1252 and Unicode -- such as all
the common Web browsers -- will pick some Unicode characters for them to map
to, and the characters they pick are the Unicode characters with the same
numbers: U+0081 and U+008D. This is the same as what Latin-1 does, and the
resulting characters tend to fall into a range of Unicode that's set aside for
obsolete Latin-1 control characters anyway.

These sloppy codecs let Python do the same thing, thus interoperating with
other software that works this way. It defines a sloppy version of many
single-byte encodings with holes. (There is no need for a sloppy version of
an encoding without holes: for example, there is no such thing as
sloppy-iso-8859-2 or sloppy-macroman.)

The following encodings will become defined:

- sloppy-windows-1250 (Central European, sort of based on ISO-8859-2)
- sloppy-windows-1251 (Cyrillic)
- sloppy-windows-1252 (Western European, based on Latin-1)
- sloppy-windows-1253 (Greek, sort of based on ISO-8859-7)
- sloppy-windows-1254 (Turkish, based on ISO-8859-9)
- sloppy-windows-1255 (Hebrew, based on ISO-8859-8)
- sloppy-windows-1256 (Arabic)
- sloppy-windows-1257 (Baltic, based on ISO-8859-13)
- sloppy-windows-1258 (Vietnamese)
- sloppy-cp874 (Thai, based on ISO-8859-11)
- sloppy-iso-8859-3 (Maltese and Esperanto, I guess)
- sloppy-iso-8859-6 (different Arabic)
- sloppy-iso-8859-7 (Greek)
- sloppy-iso-8859-8 (Hebrew)
- sloppy-iso-8859-11 (Thai)

Aliases such as "sloppy-cp1252" for "sloppy-windows-1252" will also be
defined.

Five of these encodings (`sloppy-windows-1250` through `sloppy-windows-1254`)
are used within ftfy.

Here are some examples, using :func:`ftfy.explain_unicode` to illustrate how
sloppy-windows-1252 merges Windows-1252 with Latin-1:

    >>> from ftfy import explain_unicode
    >>> some_bytes = b'\x80\x81\x82'
    >>> explain_unicode(some_bytes.decode('latin-1'))
    U+0080  \x80    [Cc] <unknown>
    U+0081  \x81    [Cc] <unknown>
    U+0082  \x82    [Cc] <unknown>

    >>> explain_unicode(some_bytes.decode('windows-1252', 'replace'))
    U+20AC  €       [Sc] EURO SIGN
    U+FFFD  �       [So] REPLACEMENT CHARACTER
    U+201A  ‚       [Ps] SINGLE LOW-9 QUOTATION MARK

    >>> explain_unicode(some_bytes.decode('sloppy-windows-1252'))
    U+20AC  €       [Sc] EURO SIGN
    U+0081  \x81    [Cc] <unknown>
    U+201A  ‚       [Ps] SINGLE LOW-9 QUOTATION MARK
"""

from __future__ import annotations

import codecs
from encodings import normalize_encoding

REPLACEMENT_CHAR = "\ufffd"


def make_sloppy_codec(encoding: str) -> codecs.CodecInfo:
    """
    Take a codec name, and return a 'sloppy' version of that codec that can
    encode and decode the unassigned bytes in that encoding.

    Single-byte encodings in the standard library are defined using some
    boilerplate classes surrounding the functions that do the actual work,
    `codecs.charmap_decode` and `charmap_encode`. This function, given an
    encoding name, *defines* those boilerplate classes.
    """
    # Make a bytestring of all 256 possible bytes.
    all_bytes = bytes(range(256))

    # Get a list of what they would decode to in Latin-1.
    sloppy_chars = list(all_bytes.decode("latin-1"))

    # Get a list of what they decode to in the given encoding. Use the
    # replacement character for unassigned bytes.
    decoded_chars = all_bytes.decode(encoding, errors="replace")

    # Update the sloppy_chars list. Each byte that was successfully decoded
    # gets its decoded value in the list. The unassigned bytes are left as
    # they are, which gives their decoding in Latin-1.
    for i, char in enumerate(decoded_chars):
        if char != REPLACEMENT_CHAR:
            sloppy_chars[i] = char

    # For ftfy's own purposes, we're going to allow byte 1A, the "Substitute"
    # control code, to encode the Unicode replacement character U+FFFD.
    sloppy_chars[0x1A] = REPLACEMENT_CHAR

    # Create the data structures that tell the charmap methods how to encode
    # and decode in this sloppy encoding.
    decoding_table = "".join(sloppy_chars)
    encoding_table = codecs.charmap_build(decoding_table)

    # Now produce all the class boilerplate. Look at the Python source for
    # `encodings.cp1252` for comparison; this is almost exactly the same,
    # except I made it follow pep8.
    class Codec(codecs.Codec):
        def encode(self, input: str, errors: str | None = "strict") -> tuple[bytes, int]:
            return codecs.charmap_encode(input, errors, encoding_table)

        def decode(self, input: bytes, errors: str | None = "strict") -> tuple[str, int]:
            return codecs.charmap_decode(input, errors, decoding_table)  # type: ignore[arg-type]

    class IncrementalEncoder(codecs.IncrementalEncoder):
        def encode(self, input: str, final: bool = False) -> bytes:
            return codecs.charmap_encode(input, self.errors, encoding_table)[0]

    class IncrementalDecoder(codecs.IncrementalDecoder):
        def decode(self, input: bytes, final: bool = False) -> str:  # type: ignore[override]
            return codecs.charmap_decode(input, self.errors, decoding_table)[0]  # type: ignore[arg-type]

    class StreamWriter(Codec, codecs.StreamWriter):
        pass

    class StreamReader(Codec, codecs.StreamReader):
        pass

    return codecs.CodecInfo(
        name="sloppy-" + encoding,
        encode=Codec().encode,
        decode=Codec().decode,
        incrementalencoder=IncrementalEncoder,
        incrementaldecoder=IncrementalDecoder,
        streamreader=StreamReader,
        streamwriter=StreamWriter,
    )


# Define a codec for each incomplete encoding. The resulting CODECS dictionary
# can be used by the main module of ftfy.bad_codecs.
CODECS = {}
INCOMPLETE_ENCODINGS = (
    [f"windows-{num}" for num in range(1250, 1259)]
    + [f"iso-8859-{num}" for num in (3, 6, 7, 8, 11)]
    + [f"cp{num}" for num in range(1250, 1259)]
    + ["cp874"]
)

for _encoding in INCOMPLETE_ENCODINGS:
    _new_name = normalize_encoding("sloppy-" + _encoding)
    CODECS[_new_name] = make_sloppy_codec(_encoding)
