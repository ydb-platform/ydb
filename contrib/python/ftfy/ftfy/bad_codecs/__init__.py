r"""
The `ftfy.bad_codecs` module gives Python the ability to decode some common,
flawed encodings.

Python does not want you to be sloppy with your text. Its encoders and decoders
("codecs") follow the relevant standards whenever possible, which means that
when you get text that *doesn't* follow those standards, you'll probably fail
to decode it. Or you might succeed at decoding it for implementation-specific
reasons, which is perhaps worse.

There are some encodings out there that Python wishes didn't exist, which are
widely used outside of Python:

- "utf-8-variants", a family of not-quite-UTF-8 encodings, including the
  ever-popular CESU-8 and "Java modified UTF-8".
- "Sloppy" versions of character map encodings, where bytes that don't map to
  anything will instead map to the Unicode character with the same number.

Simply importing this module, or in fact any part of the `ftfy` package, will
make these new "bad codecs" available to Python through the standard Codecs
API. You never have to actually call any functions inside `ftfy.bad_codecs`.

However, if you want to call something because your code checker insists on it,
you can call ``ftfy.bad_codecs.ok()``.

A quick example of decoding text that's encoded in CESU-8:

    >>> import ftfy.bad_codecs
    >>> print(b'\xed\xa0\xbd\xed\xb8\x8d'.decode('utf-8-variants'))
    😍
"""

import codecs
from encodings import normalize_encoding
from typing import Optional

_CACHE: dict[str, codecs.CodecInfo] = {}

# Define some aliases for 'utf-8-variants'. All hyphens get turned into
# underscores, because of `normalize_encoding`.
UTF8_VAR_NAMES = (
    "utf_8_variants",
    "utf8_variants",
    "utf_8_variant",
    "utf8_variant",
    "utf_8_var",
    "utf8_var",
    "cesu_8",
    "cesu8",
    "java_utf_8",
    "java_utf8",
)


def search_function(encoding: str) -> Optional[codecs.CodecInfo]:
    """
    Register our "bad codecs" with Python's codecs API. This involves adding
    a search function that takes in an encoding name, and returns a codec
    for that encoding if it knows one, or None if it doesn't.

    The encodings this will match are:

    - Encodings of the form 'sloppy-windows-NNNN' or 'sloppy-iso-8859-N',
      where the non-sloppy version is an encoding that leaves some bytes
      unmapped to characters.
    - The 'utf-8-variants' encoding, which has the several aliases seen
      above.
    """
    if encoding in _CACHE:
        return _CACHE[encoding]

    norm_encoding = normalize_encoding(encoding)
    codec = None
    if norm_encoding in UTF8_VAR_NAMES:
        from ftfy.bad_codecs.utf8_variants import CODEC_INFO

        codec = CODEC_INFO
    elif norm_encoding.startswith("sloppy_"):
        from ftfy.bad_codecs.sloppy import CODECS

        codec = CODECS.get(norm_encoding)

    if codec is not None:
        _CACHE[encoding] = codec

    return codec


def ok() -> None:
    """
    A feel-good function that gives you something to call after importing
    this package.

    Why is this here? Pyflakes. Pyflakes gets upset when you import a module
    and appear not to use it. It doesn't know that you're using it when
    you use the ``unicode.encode`` and ``bytes.decode`` methods with certain
    encodings.
    """


codecs.register(search_function)
