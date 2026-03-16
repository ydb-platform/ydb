"""
ATASCII (Atari 8-bit) encoding.

ATASCII is the character encoding used by Atari 8-bit computers (400, 800,
XL, XE series).  It shares the printable ASCII range 0x20-0x7A with standard
ASCII, but replaces control codes 0x00-0x1F with graphics characters and uses
0x80-0xFF as inverse-video variants.

Mapping sources:
- https://www.kreativekorp.com/charset/map/atascii/
- https://github.com/JSJvR/atari-8-bit-utils

The inverse-video range (0x80-0xFF) maps to the same Unicode characters as
the corresponding normal byte (byte & 0x7F), except where a distinct glyph
exists (e.g. complementary block elements).  This makes encoding lossy for
inverse bytes, which is the same trade-off as the PETSCII codec.

Notable: 0x9B is the ATASCII end-of-line character (mapped to U+000A LF).
"""

# std imports
import codecs
from typing import Dict, Tuple, Union

# Decoding Table -- ATASCII, 256 entries.
#
# 0x00-0x1F : graphics characters (heart, box drawing, triangles, etc.)
# 0x20-0x5F : standard ASCII (space, digits, uppercase, punctuation)
# 0x60      : diamond suit
# 0x61-0x7A : lowercase a-z
# 0x7B-0x7F : spade suit, pipe, clear-screen, backspace, tab glyphs
# 0x80-0xFF : inverse video (mostly same glyphs as 0x00-0x7F)

DECODING_TABLE = (
    # 0x00-0x1F: Graphics characters
    "\u2665"  # 0x00 BLACK HEART SUIT
    "\u251c"  # 0x01 BOX DRAWINGS LIGHT VERTICAL AND RIGHT
    "\u23b9"  # 0x02 RIGHT VERTICAL BOX LINE
    "\u2518"  # 0x03 BOX DRAWINGS LIGHT UP AND LEFT
    "\u2524"  # 0x04 BOX DRAWINGS LIGHT VERTICAL AND LEFT
    "\u2510"  # 0x05 BOX DRAWINGS LIGHT DOWN AND LEFT
    "\u2571"  # 0x06 BOX DRAWINGS LIGHT DIAGONAL UPPER RIGHT TO LOWER LEFT
    "\u2572"  # 0x07 BOX DRAWINGS LIGHT DIAGONAL UPPER LEFT TO LOWER RIGHT
    "\u25e2"  # 0x08 BLACK LOWER RIGHT TRIANGLE
    "\u2597"  # 0x09 QUADRANT LOWER RIGHT
    "\u25e3"  # 0x0A BLACK LOWER LEFT TRIANGLE
    "\u259d"  # 0x0B QUADRANT UPPER RIGHT
    "\u2598"  # 0x0C QUADRANT UPPER LEFT
    "\U0001fb82"  # 0x0D UPPER ONE QUARTER BLOCK
    "\u2582"  # 0x0E LOWER ONE QUARTER BLOCK
    "\u2596"  # 0x0F QUADRANT LOWER LEFT
    "\u2663"  # 0x10 BLACK CLUB SUIT
    "\u250c"  # 0x11 BOX DRAWINGS LIGHT DOWN AND RIGHT
    "\u2500"  # 0x12 BOX DRAWINGS LIGHT HORIZONTAL
    "\u253c"  # 0x13 BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL
    "\u25cf"  # 0x14 BLACK CIRCLE
    "\u2584"  # 0x15 LOWER HALF BLOCK
    "\u258e"  # 0x16 LEFT ONE QUARTER BLOCK
    "\u252c"  # 0x17 BOX DRAWINGS LIGHT DOWN AND HORIZONTAL
    "\u2534"  # 0x18 BOX DRAWINGS LIGHT UP AND HORIZONTAL
    "\u258c"  # 0x19 LEFT HALF BLOCK
    "\u2514"  # 0x1A BOX DRAWINGS LIGHT UP AND RIGHT
    "\u241b"  # 0x1B SYMBOL FOR ESCAPE
    "\u2191"  # 0x1C UPWARDS ARROW (cursor up)
    "\u2193"  # 0x1D DOWNWARDS ARROW (cursor down)
    "\u2190"  # 0x1E LEFTWARDS ARROW (cursor left)
    "\u2192"  # 0x1F RIGHTWARDS ARROW (cursor right)
    # 0x20-0x5F: Standard ASCII
    " "  # 0x20 SPACE
    "!"  # 0x21
    '"'  # 0x22
    "#"  # 0x23
    "$"  # 0x24
    "%"  # 0x25
    "&"  # 0x26
    "'"  # 0x27
    "("  # 0x28
    ")"  # 0x29
    "*"  # 0x2A
    "+"  # 0x2B
    ","  # 0x2C
    "-"  # 0x2D
    "."  # 0x2E
    "/"  # 0x2F
    "0"  # 0x30
    "1"  # 0x31
    "2"  # 0x32
    "3"  # 0x33
    "4"  # 0x34
    "5"  # 0x35
    "6"  # 0x36
    "7"  # 0x37
    "8"  # 0x38
    "9"  # 0x39
    ":"  # 0x3A
    ";"  # 0x3B
    "<"  # 0x3C
    "="  # 0x3D
    ">"  # 0x3E
    "?"  # 0x3F
    "@"  # 0x40
    "A"  # 0x41
    "B"  # 0x42
    "C"  # 0x43
    "D"  # 0x44
    "E"  # 0x45
    "F"  # 0x46
    "G"  # 0x47
    "H"  # 0x48
    "I"  # 0x49
    "J"  # 0x4A
    "K"  # 0x4B
    "L"  # 0x4C
    "M"  # 0x4D
    "N"  # 0x4E
    "O"  # 0x4F
    "P"  # 0x50
    "Q"  # 0x51
    "R"  # 0x52
    "S"  # 0x53
    "T"  # 0x54
    "U"  # 0x55
    "V"  # 0x56
    "W"  # 0x57
    "X"  # 0x58
    "Y"  # 0x59
    "Z"  # 0x5A
    "["  # 0x5B
    "\\"  # 0x5C
    "]"  # 0x5D
    "^"  # 0x5E
    "_"  # 0x5F
    # 0x60-0x7F: Lowercase + special glyphs
    "\u2666"  # 0x60 BLACK DIAMOND SUIT
    "a"  # 0x61
    "b"  # 0x62
    "c"  # 0x63
    "d"  # 0x64
    "e"  # 0x65
    "f"  # 0x66
    "g"  # 0x67
    "h"  # 0x68
    "i"  # 0x69
    "j"  # 0x6A
    "k"  # 0x6B
    "l"  # 0x6C
    "m"  # 0x6D
    "n"  # 0x6E
    "o"  # 0x6F
    "p"  # 0x70
    "q"  # 0x71
    "r"  # 0x72
    "s"  # 0x73
    "t"  # 0x74
    "u"  # 0x75
    "v"  # 0x76
    "w"  # 0x77
    "x"  # 0x78
    "y"  # 0x79
    "z"  # 0x7A
    "\u2660"  # 0x7B BLACK SPADE SUIT
    "|"  # 0x7C VERTICAL LINE
    "\u21b0"  # 0x7D UPWARDS ARROW WITH TIP LEFTWARDS (clear screen)
    "\u25c0"  # 0x7E BLACK LEFT-POINTING TRIANGLE (backspace)
    "\u25b6"  # 0x7F BLACK RIGHT-POINTING TRIANGLE (tab)
    # 0x80-0xFF: Inverse video range
    # Bytes with distinct glyphs get their own Unicode mapping;
    # the rest share the same character as (byte & 0x7F).
    "\u2665"  # 0x80 = inverse of 0x00 (heart)
    "\u251c"  # 0x81 = inverse of 0x01
    "\u258a"  # 0x82 LEFT THREE QUARTERS BLOCK (distinct)
    "\u2518"  # 0x83 = inverse of 0x03
    "\u2524"  # 0x84 = inverse of 0x04
    "\u2510"  # 0x85 = inverse of 0x05
    "\u2571"  # 0x86 = inverse of 0x06
    "\u2572"  # 0x87 = inverse of 0x07
    "\u25e4"  # 0x88 BLACK UPPER LEFT TRIANGLE (distinct)
    "\u259b"  # 0x89 QUADRANT UPPER LEFT AND UPPER RIGHT AND LOWER LEFT (distinct)
    "\u25e5"  # 0x8A BLACK UPPER RIGHT TRIANGLE (distinct)
    "\u2599"  # 0x8B QUADRANT UPPER LEFT AND LOWER LEFT AND LOWER RIGHT (distinct)
    "\u259f"  # 0x8C QUADRANT UPPER RIGHT AND LOWER LEFT AND LOWER RIGHT (distinct)
    "\u2586"  # 0x8D LOWER THREE QUARTERS BLOCK (distinct)
    "\U0001fb85"  # 0x8E UPPER THREE QUARTERS BLOCK (distinct)
    "\u259c"  # 0x8F QUADRANT UPPER LEFT AND UPPER RIGHT AND LOWER RIGHT (distinct)
    "\u2663"  # 0x90 = inverse of 0x10 (club)
    "\u250c"  # 0x91 = inverse of 0x11
    "\u2500"  # 0x92 = inverse of 0x12
    "\u253c"  # 0x93 = inverse of 0x13
    "\u25d8"  # 0x94 INVERSE BULLET (distinct)
    "\u2580"  # 0x95 UPPER HALF BLOCK (distinct)
    "\U0001fb8a"  # 0x96 RIGHT THREE QUARTERS BLOCK (distinct)
    "\u252c"  # 0x97 = inverse of 0x17
    "\u2534"  # 0x98 = inverse of 0x18
    "\u2590"  # 0x99 RIGHT HALF BLOCK (distinct)
    "\u2514"  # 0x9A = inverse of 0x1A
    "\n"  # 0x9B ATASCII END OF LINE
    "\u2191"  # 0x9C = inverse of 0x1C (up arrow)
    "\u2193"  # 0x9D = inverse of 0x1D (down arrow)
    "\u2190"  # 0x9E = inverse of 0x1E (left arrow)
    "\u2192"  # 0x9F = inverse of 0x1F (right arrow)
    "\u2588"  # 0xA0 FULL BLOCK (distinct)
    "!"  # 0xA1 = inverse of 0x21
    '"'  # 0xA2 = inverse of 0x22
    "#"  # 0xA3 = inverse of 0x23
    "$"  # 0xA4 = inverse of 0x24
    "%"  # 0xA5 = inverse of 0x25
    "&"  # 0xA6 = inverse of 0x26
    "'"  # 0xA7 = inverse of 0x27
    "("  # 0xA8 = inverse of 0x28
    ")"  # 0xA9 = inverse of 0x29
    "*"  # 0xAA = inverse of 0x2A
    "+"  # 0xAB = inverse of 0x2B
    ","  # 0xAC = inverse of 0x2C
    "-"  # 0xAD = inverse of 0x2D
    "."  # 0xAE = inverse of 0x2E
    "/"  # 0xAF = inverse of 0x2F
    "0"  # 0xB0 = inverse of 0x30
    "1"  # 0xB1 = inverse of 0x31
    "2"  # 0xB2 = inverse of 0x32
    "3"  # 0xB3 = inverse of 0x33
    "4"  # 0xB4 = inverse of 0x34
    "5"  # 0xB5 = inverse of 0x35
    "6"  # 0xB6 = inverse of 0x36
    "7"  # 0xB7 = inverse of 0x37
    "8"  # 0xB8 = inverse of 0x38
    "9"  # 0xB9 = inverse of 0x39
    ":"  # 0xBA = inverse of 0x3A
    ";"  # 0xBB = inverse of 0x3B
    "<"  # 0xBC = inverse of 0x3C
    "="  # 0xBD = inverse of 0x3D
    ">"  # 0xBE = inverse of 0x3E
    "?"  # 0xBF = inverse of 0x3F
    "@"  # 0xC0 = inverse of 0x40
    "A"  # 0xC1 = inverse of 0x41
    "B"  # 0xC2 = inverse of 0x42
    "C"  # 0xC3 = inverse of 0x43
    "D"  # 0xC4 = inverse of 0x44
    "E"  # 0xC5 = inverse of 0x45
    "F"  # 0xC6 = inverse of 0x46
    "G"  # 0xC7 = inverse of 0x47
    "H"  # 0xC8 = inverse of 0x48
    "I"  # 0xC9 = inverse of 0x49
    "J"  # 0xCA = inverse of 0x4A
    "K"  # 0xCB = inverse of 0x4B
    "L"  # 0xCC = inverse of 0x4C
    "M"  # 0xCD = inverse of 0x4D
    "N"  # 0xCE = inverse of 0x4E
    "O"  # 0xCF = inverse of 0x4F
    "P"  # 0xD0 = inverse of 0x50
    "Q"  # 0xD1 = inverse of 0x51
    "R"  # 0xD2 = inverse of 0x52
    "S"  # 0xD3 = inverse of 0x53
    "T"  # 0xD4 = inverse of 0x54
    "U"  # 0xD5 = inverse of 0x55
    "V"  # 0xD6 = inverse of 0x56
    "W"  # 0xD7 = inverse of 0x57
    "X"  # 0xD8 = inverse of 0x58
    "Y"  # 0xD9 = inverse of 0x59
    "Z"  # 0xDA = inverse of 0x5A
    "["  # 0xDB = inverse of 0x5B
    "\\"  # 0xDC = inverse of 0x5C
    "]"  # 0xDD = inverse of 0x5D
    "^"  # 0xDE = inverse of 0x5E
    "_"  # 0xDF = inverse of 0x5F
    "\u2666"  # 0xE0 = inverse of 0x60 (diamond)
    "a"  # 0xE1 = inverse of 0x61
    "b"  # 0xE2 = inverse of 0x62
    "c"  # 0xE3 = inverse of 0x63
    "d"  # 0xE4 = inverse of 0x64
    "e"  # 0xE5 = inverse of 0x65
    "f"  # 0xE6 = inverse of 0x66
    "g"  # 0xE7 = inverse of 0x67
    "h"  # 0xE8 = inverse of 0x68
    "i"  # 0xE9 = inverse of 0x69
    "j"  # 0xEA = inverse of 0x6A
    "k"  # 0xEB = inverse of 0x6B
    "l"  # 0xEC = inverse of 0x6C
    "m"  # 0xED = inverse of 0x6D
    "n"  # 0xEE = inverse of 0x6E
    "o"  # 0xEF = inverse of 0x6F
    "p"  # 0xF0 = inverse of 0x70
    "q"  # 0xF1 = inverse of 0x71
    "r"  # 0xF2 = inverse of 0x72
    "s"  # 0xF3 = inverse of 0x73
    "t"  # 0xF4 = inverse of 0x74
    "u"  # 0xF5 = inverse of 0x75
    "v"  # 0xF6 = inverse of 0x76
    "w"  # 0xF7 = inverse of 0x77
    "x"  # 0xF8 = inverse of 0x78
    "y"  # 0xF9 = inverse of 0x79
    "z"  # 0xFA = inverse of 0x7A
    "\u2660"  # 0xFB = inverse of 0x7B (spade)
    "|"  # 0xFC = inverse of 0x7C
    "\u21b0"  # 0xFD = inverse of 0x7D (clear screen)
    "\u25c0"  # 0xFE = inverse of 0x7E (backspace)
    "\u25b6"  # 0xFF = inverse of 0x7F (tab)
)


def _normalize_eol(text: str) -> str:
    r"""
    Normalize CR and CRLF to LF for ATASCII encoding.

    ATASCII uses byte 0x9B as its end-of-line character, mapped to ``\n`` (U+000A).  Standard CR
    (U+000D) has no native ATASCII representation (byte 0x0D is a graphics character), so CR and
    CRLF are both folded to LF before charmap encoding.
    """
    return text.replace("\r\n", "\n").replace("\r", "\n")


class Codec(codecs.Codec):
    """ATASCII character map codec."""

    def encode(self, input: str, errors: str = "strict") -> Tuple[bytes, int]:
        """Encode input string using ATASCII character map."""
        input = _normalize_eol(input)
        return codecs.charmap_encode(input, errors, ENCODING_TABLE)

    def decode(self, input: bytes, errors: str = "strict") -> Tuple[str, int]:
        """Decode input bytes using ATASCII character map."""
        return codecs.charmap_decode(input, errors, DECODING_TABLE)  # type: ignore[arg-type]


class IncrementalEncoder(codecs.IncrementalEncoder):
    """ATASCII incremental encoder with CR/CRLF -> LF normalization."""

    def __init__(self, errors: str = "strict") -> None:
        """Initialize encoder with pending CR state."""
        super().__init__(errors)
        self._pending_cr = False

    def encode(self, input: str, final: bool = False) -> bytes:
        """Encode input string incrementally."""
        if self._pending_cr:
            input = "\r" + input
            self._pending_cr = False
        if not final and input.endswith("\r"):
            input = input[:-1]
            self._pending_cr = True
        input = _normalize_eol(input)
        return codecs.charmap_encode(input, self.errors, ENCODING_TABLE)[0]

    def reset(self) -> None:
        """Reset encoder state."""
        self._pending_cr = False

    def getstate(self) -> int:
        """Return encoder state as integer."""
        return int(self._pending_cr)

    def setstate(self, state: Union[int, str]) -> None:
        """Restore encoder state from integer."""
        self._pending_cr = bool(state)


class IncrementalDecoder(codecs.IncrementalDecoder):
    """ATASCII incremental decoder."""

    def decode(self, input: bytes, final: bool = False) -> str:  # type: ignore[override]
        """Decode input bytes incrementally."""
        return codecs.charmap_decode(input, self.errors, DECODING_TABLE)[  # type: ignore[arg-type]
            0
        ]


class StreamWriter(Codec, codecs.StreamWriter):
    """ATASCII stream writer."""


class StreamReader(Codec, codecs.StreamReader):
    """ATASCII stream reader."""


def getregentry() -> codecs.CodecInfo:
    """Return the codec registry entry."""
    return codecs.CodecInfo(
        name="atascii",
        encode=Codec().encode,
        decode=Codec().decode,  # type: ignore[arg-type]
        incrementalencoder=IncrementalEncoder,
        incrementaldecoder=IncrementalDecoder,
        streamreader=StreamReader,
        streamwriter=StreamWriter,
    )


def getaliases() -> Tuple[str, ...]:
    """Return codec aliases."""
    return ("atari8bit", "atari_8bit")


# Build encoding table preferring normal bytes (0x00-0x7F) over inverse
# (0x80-0xFF).  codecs.charmap_build() picks the last occurrence, which
# maps printable characters to the inverse-video range.  We fix this by
# overwriting with normal-range entries so they take priority.
# Exception: '\n' must still encode to 0x9B (ATASCII EOL), not 0x0A.
def _build_encoding_table() -> Dict[int, int]:
    table: Dict[int, int] = codecs.charmap_build(DECODING_TABLE)  # type: ignore[assignment]
    for byte_val in range(0x80):
        table[ord(DECODING_TABLE[byte_val])] = byte_val
    # '\n' at 0x0A must encode to 0x9B (ATASCII EOL), not 0x0A
    table[ord("\n")] = 0x9B
    return table


ENCODING_TABLE = _build_encoding_table()
