r"""
This file defines a codec called "utf-8-variants" (or "utf-8-var"), which can
decode text that's been encoded with a popular non-standard version of UTF-8.
This includes CESU-8, the accidental encoding made by layering UTF-8 on top of
UTF-16, as well as Java's twist on CESU-8 that contains a two-byte encoding for
codepoint 0.

This is particularly relevant in Python 3, which provides no other way of
decoding CESU-8 [1]_.

The easiest way to use the codec is to simply import `ftfy.bad_codecs`:

    >>> import ftfy.bad_codecs
    >>> result = b'here comes a null! \xc0\x80'.decode('utf-8-var')
    >>> print(repr(result).lstrip('u'))
    'here comes a null! \x00'

The codec does not at all enforce "correct" CESU-8. For example, the Unicode
Consortium's not-quite-standard describing CESU-8 requires that there is only
one possible encoding of any character, so it does not allow mixing of valid
UTF-8 and CESU-8. This codec *does* allow that, just like Python 2's UTF-8
decoder does.

Characters in the Basic Multilingual Plane still have only one encoding. This
codec still enforces the rule, within the BMP, that characters must appear in
their shortest form. There is one exception: the sequence of bytes `0xc0 0x80`,
instead of just `0x00`, may be used to encode the null character `U+0000`, like
in Java.

If you encode with this codec, you get legitimate UTF-8. Decoding with this
codec and then re-encoding is not idempotent, although encoding and then
decoding is. So this module won't produce CESU-8 for you. Look for that
functionality in the sister module, "Breaks Text For You", coming approximately
never.

.. [1] In a pinch, you can decode CESU-8 in Python 2 using the UTF-8 codec:
   first decode the bytes (incorrectly), then encode them, then decode them
   again, using UTF-8 as the codec every time. But Python 2 is dead, so use
   ftfy instead.
"""

import codecs
import re
from encodings.utf_8 import (
    IncrementalDecoder as UTF8IncrementalDecoder,
)
from encodings.utf_8 import (
    IncrementalEncoder as UTF8IncrementalEncoder,
)
from typing import Callable, Optional

NAME = "utf-8-variants"

# This regular expression matches all possible six-byte CESU-8 sequences,
# plus truncations of them at the end of the string. (If any of the
# subgroups matches $, then all the subgroups after it also have to match $,
# as there are no more characters to match.)
CESU8_EXPR = (
    b"("
    b"\xed"
    b"([\xa0-\xaf]|$)"
    b"([\x80-\xbf]|$)"
    b"(\xed|$)"
    b"([\xb0-\xbf]|$)"
    b"([\x80-\xbf]|$)"
    b")"
)

CESU8_RE = re.compile(CESU8_EXPR)

# This expression matches isolated surrogate characters that aren't
# CESU-8, which have to be handled carefully on Python 2.
SURROGATE_EXPR = b"(\xed([\xa0-\xbf]|$)([\x80-\xbf]|$))"

# This expression matches the Java encoding of U+0, including if it's
# truncated and we need more bytes.
NULL_EXPR = b"(\xc0(\x80|$))"

# This regex matches cases that we need to decode differently from
# standard UTF-8.
SPECIAL_BYTES_RE = re.compile(b"|".join([NULL_EXPR, CESU8_EXPR, SURROGATE_EXPR]))


class IncrementalDecoder(UTF8IncrementalDecoder):
    """
    An incremental decoder that extends Python's built-in UTF-8 decoder.

    This encoder needs to take in bytes, possibly arriving in a stream, and
    output the correctly decoded text. The general strategy for doing this
    is to fall back on the real UTF-8 decoder whenever possible, because
    the real UTF-8 decoder is way optimized, but to call specialized methods
    we define here for the cases the real encoder isn't expecting.
    """

    @staticmethod
    def _buffer_decode(  # type: ignore[override]
        input: bytes, errors: Optional[str], final: bool
    ) -> tuple[str, int]:
        """
        Decode bytes that may be arriving in a stream, following the Codecs
        API.

        `input` is the incoming sequence of bytes. `errors` tells us how to
        handle errors, though we delegate all error-handling cases to the real
        UTF-8 decoder to ensure correct behavior. `final` indicates whether
        this is the end of the sequence, in which case we should raise an
        error given incomplete input.

        Returns as much decoded text as possible, and the number of bytes
        consumed.
        """
        # decoded_segments are the pieces of text we have decoded so far,
        # and position is our current position in the byte string. (Bytes
        # before this position have been consumed, and bytes after it have
        # yet to be decoded.)
        decoded_segments = []
        position = 0
        while True:
            # Use _buffer_decode_step to decode a segment of text.
            decoded, consumed = IncrementalDecoder._buffer_decode_step(
                input[position:], errors, final
            )
            if consumed == 0:
                # Either there's nothing left to decode, or we need to wait
                # for more input. Either way, we're done for now.
                break

            # Append the decoded text to the list, and update our position.
            decoded_segments.append(decoded)
            position += consumed

        if final:
            # _buffer_decode_step must consume all the bytes when `final` is
            # true.
            assert position == len(input)

        return "".join(decoded_segments), position

    @staticmethod
    def _buffer_decode_step(input: bytes, errors: Optional[str], final: bool) -> tuple[str, int]:
        """
        There are three possibilities for each decoding step:

        - Decode as much real UTF-8 as possible.
        - Decode a six-byte CESU-8 sequence at the current position.
        - Decode a Java-style null at the current position.

        This method figures out which step is appropriate, and does it.
        """
        # Get a reference to the superclass method that we'll be using for
        # most of the real work.
        sup = UTF8IncrementalDecoder._buffer_decode

        # Find the next byte position that indicates a variant of UTF-8.
        match = SPECIAL_BYTES_RE.search(input)
        if match is None:
            return sup(input, errors, final)

        cutoff = match.start()
        if cutoff > 0:
            return sup(input[:cutoff], errors, True)

        # Some byte sequence that we intend to handle specially matches
        # at the beginning of the input.
        if input.startswith(b"\xc0"):
            if len(input) > 1:
                # Decode the two-byte sequence 0xc0 0x80.
                return "\u0000", 2
            else:
                if final:
                    # We hit the end of the stream. Let the superclass method
                    # handle it.
                    return sup(input, errors, True)
                else:
                    # Wait to see another byte.
                    return "", 0
        else:
            # Decode a possible six-byte sequence starting with 0xed.
            return IncrementalDecoder._buffer_decode_surrogates(sup, input, errors, final)

    @staticmethod
    def _buffer_decode_surrogates(
        sup: Callable[[bytes, Optional[str], bool], tuple[str, int]],
        input: bytes,
        errors: Optional[str],
        final: bool,
    ) -> tuple[str, int]:
        """
        When we have improperly encoded surrogates, we can still see the
        bits that they were meant to represent.

        The surrogates were meant to encode a 20-bit number, to which we
        add 0x10000 to get a codepoint. That 20-bit number now appears in
        this form:

          11101101 1010abcd 10efghij 11101101 1011klmn 10opqrst

        The CESU8_RE above matches byte sequences of this form. Then we need
        to extract the bits and assemble a codepoint number from them.
        """
        if len(input) < 6:
            if final:
                # We found 0xed near the end of the stream, and there aren't
                # six bytes to decode. Delegate to the superclass method to
                # handle it as normal UTF-8. It might be a Hangul character
                # or an error.
                return sup(input, errors, final)
            else:
                # We found a surrogate, the stream isn't over yet, and we don't
                # know enough of the following bytes to decode anything, so
                # consume zero bytes and wait.
                return "", 0
        else:
            if CESU8_RE.match(input):
                # Given this is a CESU-8 sequence, do some math to pull out
                # the intended 20-bit value, and consume six bytes.
                codepoint = (
                    ((input[1] & 0x0F) << 16)
                    + ((input[2] & 0x3F) << 10)
                    + ((input[4] & 0x0F) << 6)
                    + (input[5] & 0x3F)
                    + 0x10000
                )
                return chr(codepoint), 6
            else:
                # This looked like a CESU-8 sequence, but it wasn't one.
                # 0xed indicates the start of a three-byte sequence, so give
                # three bytes to the superclass to decode as usual.
                return sup(input[:3], errors, False)


# The encoder is identical to UTF-8.
IncrementalEncoder = UTF8IncrementalEncoder


class StreamWriter(codecs.StreamWriter):
    @staticmethod
    def encode(input: str, errors: str = "strict") -> tuple[bytes, int]:
        return IncrementalEncoder(errors).encode(input, final=True), len(input)


class StreamReader(codecs.StreamReader):
    @staticmethod
    def decode(input: bytes, errors: str = "strict") -> tuple[str, int]:
        return IncrementalDecoder(errors).decode(input, final=True), len(input)


CODEC_INFO = codecs.CodecInfo(
    name=NAME,
    encode=StreamWriter.encode,
    decode=StreamReader.decode,
    incrementalencoder=IncrementalEncoder,
    incrementaldecoder=IncrementalDecoder,
    streamreader=StreamReader,
    streamwriter=StreamWriter,
)
