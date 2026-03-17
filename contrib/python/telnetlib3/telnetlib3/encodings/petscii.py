"""
PETSCII (Commodore 64/128) encoding -- shifted (lowercase) mode.

PETSCII is the character encoding used by Commodore computers (C64, C128,
VIC-20, Plus/4, etc.).  This codec implements the "shifted" character set
(mixed case with lowercase at 0x41-0x5A and uppercase at 0xC1-0xDA),
which is the standard mode for BBS operation.

Mapping sources:
- Commodore 64 Programmer's Reference Guide
- https://sta.c64.org/cbm64pet.html
- Unicode Consortium Legacy Computing Supplement (U+1FB00-U+1FBFF)

Control codes (0x00-0x1F, 0x80-0x9F) are mapped to their ASCII control
code equivalents where sensible, otherwise to U+FFFD with 'replace' errors.
Graphics characters use the closest available Unicode approximations from
the Box Drawing, Block Elements, and Geometric Shapes blocks.
"""

# std imports
import codecs

# Decoding Table -- PETSCII shifted (lowercase) mode, 256 entries.
#
# 0x00-0x1F : C64 control codes
# 0x20-0x3F : ASCII compatible (digits, punctuation)
# 0x40-0x5F : @, lowercase a-z, [, pound, ], up-arrow, left-arrow
# 0x60-0x7F : graphics characters
# 0x80-0x9F : C64 control codes (colors, function keys)
# 0xA0-0xBF : graphics characters (shifted)
# 0xC0-0xDF : graphics (0xC0) + uppercase A-Z (0xC1-0xDA) + graphics
# 0xE0-0xFE : graphics characters (same as 0xA0-0xBE)
# 0xFF      : pi

DECODING_TABLE = (
    # 0x00-0x1F: Control codes
    "\x00"  # 0x00 NUL
    "\x01"  # 0x01 (unused)
    "\x02"  # 0x02 (unused)
    "\x03"  # 0x03 RUN/STOP
    "\x04"  # 0x04 (unused)
    "\x05"  # 0x05 WHT (white)
    "\x06"  # 0x06 (unused)
    "\x07"  # 0x07 BEL
    "\x08"  # 0x08 shift-disable
    "\x09"  # 0x09 shift-enable
    "\n"  # 0x0A LF
    "\x0b"  # 0x0B (unused)
    "\x0c"  # 0x0C (unused)
    "\r"  # 0x0D RETURN
    "\x0e"  # 0x0E lowercase charset
    "\x0f"  # 0x0F (unused)
    "\x10"  # 0x10 (unused)
    "\x11"  # 0x11 cursor down
    "\x12"  # 0x12 RVS ON
    "\x13"  # 0x13 HOME
    "\x14"  # 0x14 DEL
    "\x15"  # 0x15 (unused)
    "\x16"  # 0x16 (unused)
    "\x17"  # 0x17 (unused)
    "\x18"  # 0x18 (unused)
    "\x19"  # 0x19 (unused)
    "\x1a"  # 0x1A (unused)
    "\x1b"  # 0x1B ESC
    "\x1c"  # 0x1C RED
    "\x1d"  # 0x1D cursor right
    "\x1e"  # 0x1E GRN
    "\x1f"  # 0x1F BLU
    # 0x20-0x3F: ASCII compatible
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
    # 0x40-0x5F: Letters and symbols
    "@"  # 0x40
    "a"  # 0x41 lowercase (PETSCII shifted mode)
    "b"  # 0x42
    "c"  # 0x43
    "d"  # 0x44
    "e"  # 0x45
    "f"  # 0x46
    "g"  # 0x47
    "h"  # 0x48
    "i"  # 0x49
    "j"  # 0x4A
    "k"  # 0x4B
    "l"  # 0x4C
    "m"  # 0x4D
    "n"  # 0x4E
    "o"  # 0x4F
    "p"  # 0x50
    "q"  # 0x51
    "r"  # 0x52
    "s"  # 0x53
    "t"  # 0x54
    "u"  # 0x55
    "v"  # 0x56
    "w"  # 0x57
    "x"  # 0x58
    "y"  # 0x59
    "z"  # 0x5A
    "["  # 0x5B
    "\u00a3"  # 0x5C POUND SIGN
    "]"  # 0x5D
    "\u2191"  # 0x5E UP ARROW
    "\u2190"  # 0x5F LEFT ARROW
    # 0x60-0x7F: Graphics characters (shifted mode)
    "\u2500"  # 0x60 HORIZONTAL LINE
    "\u2660"  # 0x61 BLACK SPADE SUIT
    "\u2502"  # 0x62 VERTICAL LINE
    "\u2500"  # 0x63 HORIZONTAL LINE
    "\u2597"  # 0x64 QUADRANT LOWER RIGHT
    "\u2596"  # 0x65 QUADRANT LOWER LEFT
    "\u2598"  # 0x66 QUADRANT UPPER LEFT
    "\u259d"  # 0x67 QUADRANT UPPER RIGHT
    "\u2599"  # 0x68 QUADRANT UPPER LEFT AND LOWER LEFT AND LOWER RIGHT
    "\u259f"  # 0x69 QUADRANT UPPER RIGHT AND LOWER LEFT AND LOWER RIGHT
    "\u259e"  # 0x6A QUADRANT UPPER RIGHT AND LOWER LEFT
    "\u2595"  # 0x6B RIGHT ONE EIGHTH BLOCK
    "\u258f"  # 0x6C LEFT ONE EIGHTH BLOCK
    "\u2584"  # 0x6D LOWER HALF BLOCK
    "\u2580"  # 0x6E UPPER HALF BLOCK
    "\u2588"  # 0x6F FULL BLOCK
    "\u2584"  # 0x70 LOWER HALF BLOCK (variant)
    "\u259b"  # 0x71 QUADRANT UPPER LEFT AND UPPER RIGHT AND LOWER LEFT
    "\u2583"  # 0x72 LOWER THREE EIGHTHS BLOCK
    "\u2665"  # 0x73 BLACK HEART SUIT
    "\u259c"  # 0x74 QUADRANT UPPER LEFT AND UPPER RIGHT AND LOWER RIGHT
    "\u256d"  # 0x75 BOX DRAWINGS LIGHT ARC DOWN AND RIGHT
    "\u2573"  # 0x76 BOX DRAWINGS LIGHT DIAGONAL CROSS
    "\u25cb"  # 0x77 WHITE CIRCLE
    "\u2663"  # 0x78 BLACK CLUB SUIT
    "\u259a"  # 0x79 QUADRANT UPPER LEFT AND LOWER RIGHT
    "\u2666"  # 0x7A BLACK DIAMOND SUIT
    "\u253c"  # 0x7B BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL
    "\u2502"  # 0x7C VERTICAL LINE (with serif, approx)
    "\u2571"  # 0x7D BOX DRAWINGS LIGHT DIAGONAL UPPER RIGHT TO LOWER LEFT
    "\u03c0"  # 0x7E GREEK SMALL LETTER PI
    "\u25e5"  # 0x7F BLACK UPPER RIGHT TRIANGLE
    # 0x80-0x9F: Control codes (colors, function keys, cursor)
    "\x80"  # 0x80 (unused)
    "\x81"  # 0x81 ORN (orange)
    "\x82"  # 0x82 (unused)
    "\x83"  # 0x83 (unused)
    "\x84"  # 0x84 (unused)
    "\x85"  # 0x85 F1
    "\x86"  # 0x86 F3
    "\x87"  # 0x87 F5
    "\x88"  # 0x88 F7
    "\x89"  # 0x89 F2
    "\x8a"  # 0x8A F4
    "\x8b"  # 0x8B F6
    "\x8c"  # 0x8C F8
    "\r"  # 0x8D SHIFT-RETURN
    "\x8e"  # 0x8E uppercase charset
    "\x8f"  # 0x8F (unused)
    "\x90"  # 0x90 BLK (black)
    "\x91"  # 0x91 cursor up
    "\x92"  # 0x92 RVS OFF
    "\x93"  # 0x93 CLR (clear screen)
    "\x94"  # 0x94 INS (insert)
    "\x95"  # 0x95 BRN (brown)
    "\x96"  # 0x96 LRD (light red)
    "\x97"  # 0x97 GR1 (dark grey)
    "\x98"  # 0x98 GR2 (medium grey)
    "\x99"  # 0x99 LGR (light green)
    "\x9a"  # 0x9A LBL (light blue)
    "\x9b"  # 0x9B GR3 (light grey)
    "\x9c"  # 0x9C PUR (purple)
    "\x9d"  # 0x9D cursor left
    "\x9e"  # 0x9E YEL (yellow)
    "\x9f"  # 0x9F CYN (cyan)
    # 0xA0-0xBF: Shifted graphics
    "\xa0"  # 0xA0 SHIFTED SPACE (non-breaking)
    "\u2584"  # 0xA1 LOWER HALF BLOCK
    "\u2580"  # 0xA2 UPPER HALF BLOCK
    "\u2500"  # 0xA3 HORIZONTAL LINE
    "\u2500"  # 0xA4 HORIZONTAL LINE (lower)
    "\u2500"  # 0xA5 HORIZONTAL LINE (upper)
    "\u2502"  # 0xA6 VERTICAL LINE (right shifted)
    "\u2502"  # 0xA7 VERTICAL LINE (left shifted)
    "\u2502"  # 0xA8 VERTICAL LINE
    "\u256e"  # 0xA9 BOX DRAWINGS LIGHT ARC DOWN AND LEFT
    "\u2570"  # 0xAA BOX DRAWINGS LIGHT ARC UP AND RIGHT
    "\u256f"  # 0xAB BOX DRAWINGS LIGHT ARC UP AND LEFT
    "\u2572"  # 0xAC BOX DRAWINGS LIGHT DIAGONAL UPPER LEFT TO LOWER RIGHT
    "\u2571"  # 0xAD BOX DRAWINGS LIGHT DIAGONAL UPPER RIGHT TO LOWER LEFT
    "\u2573"  # 0xAE BOX DRAWINGS LIGHT DIAGONAL CROSS (small)
    "\u2022"  # 0xAF BULLET
    "\u25e4"  # 0xB0 BLACK UPPER LEFT TRIANGLE
    "\u258c"  # 0xB1 LEFT HALF BLOCK
    "\u2597"  # 0xB2 QUADRANT LOWER RIGHT
    "\u2514"  # 0xB3 BOX DRAWINGS LIGHT UP AND RIGHT
    "\u2510"  # 0xB4 BOX DRAWINGS LIGHT DOWN AND LEFT
    "\u2582"  # 0xB5 LOWER ONE QUARTER BLOCK
    "\u250c"  # 0xB6 BOX DRAWINGS LIGHT DOWN AND RIGHT
    "\u2534"  # 0xB7 BOX DRAWINGS LIGHT UP AND HORIZONTAL
    "\u252c"  # 0xB8 BOX DRAWINGS LIGHT DOWN AND HORIZONTAL
    "\u2524"  # 0xB9 BOX DRAWINGS LIGHT VERTICAL AND LEFT
    "\u251c"  # 0xBA BOX DRAWINGS LIGHT VERTICAL AND RIGHT
    "\u2586"  # 0xBB LOWER THREE QUARTERS BLOCK
    "\u2585"  # 0xBC LOWER FIVE EIGHTHS BLOCK
    "\u2590"  # 0xBD RIGHT HALF BLOCK
    "\u2588"  # 0xBE FULL BLOCK (variant)
    "\u2572"  # 0xBF DIAGONAL (variant)
    # 0xC0-0xDF: Horizontal line + uppercase A-Z + graphics
    "\u2500"  # 0xC0 HORIZONTAL LINE (same as 0x60)
    "A"  # 0xC1 LATIN CAPITAL LETTER A
    "B"  # 0xC2 LATIN CAPITAL LETTER B
    "C"  # 0xC3 LATIN CAPITAL LETTER C
    "D"  # 0xC4 LATIN CAPITAL LETTER D
    "E"  # 0xC5 LATIN CAPITAL LETTER E
    "F"  # 0xC6 LATIN CAPITAL LETTER F
    "G"  # 0xC7 LATIN CAPITAL LETTER G
    "H"  # 0xC8 LATIN CAPITAL LETTER H
    "I"  # 0xC9 LATIN CAPITAL LETTER I
    "J"  # 0xCA LATIN CAPITAL LETTER J
    "K"  # 0xCB LATIN CAPITAL LETTER K
    "L"  # 0xCC LATIN CAPITAL LETTER L
    "M"  # 0xCD LATIN CAPITAL LETTER M
    "N"  # 0xCE LATIN CAPITAL LETTER N
    "O"  # 0xCF LATIN CAPITAL LETTER O
    "P"  # 0xD0 LATIN CAPITAL LETTER P
    "Q"  # 0xD1 LATIN CAPITAL LETTER Q
    "R"  # 0xD2 LATIN CAPITAL LETTER R
    "S"  # 0xD3 LATIN CAPITAL LETTER S
    "T"  # 0xD4 LATIN CAPITAL LETTER T
    "U"  # 0xD5 LATIN CAPITAL LETTER U
    "V"  # 0xD6 LATIN CAPITAL LETTER V
    "W"  # 0xD7 LATIN CAPITAL LETTER W
    "X"  # 0xD8 LATIN CAPITAL LETTER X
    "Y"  # 0xD9 LATIN CAPITAL LETTER Y
    "Z"  # 0xDA LATIN CAPITAL LETTER Z
    "\u253c"  # 0xDB BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL
    "\u2502"  # 0xDC VERTICAL LINE (with tick)
    "\u2571"  # 0xDD DIAGONAL
    "\u03c0"  # 0xDE GREEK SMALL LETTER PI
    "\u25e5"  # 0xDF BLACK UPPER RIGHT TRIANGLE
    # 0xE0-0xFE: Graphics (same as 0xA0-0xBE)
    "\xa0"  # 0xE0 SHIFTED SPACE
    "\u2584"  # 0xE1 LOWER HALF BLOCK
    "\u2580"  # 0xE2 UPPER HALF BLOCK
    "\u2500"  # 0xE3 HORIZONTAL LINE
    "\u2500"  # 0xE4 HORIZONTAL LINE (lower)
    "\u2500"  # 0xE5 HORIZONTAL LINE (upper)
    "\u2502"  # 0xE6 VERTICAL LINE (right shifted)
    "\u2502"  # 0xE7 VERTICAL LINE (left shifted)
    "\u2502"  # 0xE8 VERTICAL LINE
    "\u256e"  # 0xE9 BOX DRAWINGS LIGHT ARC DOWN AND LEFT
    "\u2570"  # 0xEA BOX DRAWINGS LIGHT ARC UP AND RIGHT
    "\u256f"  # 0xEB BOX DRAWINGS LIGHT ARC UP AND LEFT
    "\u2572"  # 0xEC DIAGONAL
    "\u2571"  # 0xED DIAGONAL
    "\u2573"  # 0xEE BOX DRAWINGS LIGHT DIAGONAL CROSS
    "\u2022"  # 0xEF BULLET
    "\u25e4"  # 0xF0 BLACK UPPER LEFT TRIANGLE
    "\u258c"  # 0xF1 LEFT HALF BLOCK
    "\u2597"  # 0xF2 QUADRANT LOWER RIGHT
    "\u2514"  # 0xF3 BOX DRAWINGS LIGHT UP AND RIGHT
    "\u2510"  # 0xF4 BOX DRAWINGS LIGHT DOWN AND LEFT
    "\u2582"  # 0xF5 LOWER ONE QUARTER BLOCK
    "\u250c"  # 0xF6 BOX DRAWINGS LIGHT DOWN AND RIGHT
    "\u2534"  # 0xF7 BOX DRAWINGS LIGHT UP AND HORIZONTAL
    "\u252c"  # 0xF8 BOX DRAWINGS LIGHT DOWN AND HORIZONTAL
    "\u2524"  # 0xF9 BOX DRAWINGS LIGHT VERTICAL AND LEFT
    "\u251c"  # 0xFA BOX DRAWINGS LIGHT VERTICAL AND RIGHT
    "\u2586"  # 0xFB LOWER THREE QUARTERS BLOCK
    "\u2585"  # 0xFC LOWER FIVE EIGHTHS BLOCK
    "\u2590"  # 0xFD RIGHT HALF BLOCK
    "\u2588"  # 0xFE FULL BLOCK
    "\u03c0"  # 0xFF PI
)


class Codec(codecs.Codec):
    """PETSCII character map codec."""

    def encode(self, input: str, errors: str = "strict") -> tuple[bytes, int]:
        """Encode input string using PETSCII character map."""
        return codecs.charmap_encode(input, errors, ENCODING_TABLE)

    def decode(self, input: bytes, errors: str = "strict") -> tuple[str, int]:
        """Decode input bytes using PETSCII character map."""
        return codecs.charmap_decode(input, errors, DECODING_TABLE)  # type: ignore[arg-type]


class IncrementalEncoder(codecs.IncrementalEncoder):
    """PETSCII incremental encoder."""

    def encode(self, input: str, final: bool = False) -> bytes:
        """Encode input string incrementally."""
        return codecs.charmap_encode(input, self.errors, ENCODING_TABLE)[0]


class IncrementalDecoder(codecs.IncrementalDecoder):
    """PETSCII incremental decoder."""

    def decode(self, input: bytes, final: bool = False) -> str:  # type: ignore[override]
        """Decode input bytes incrementally."""
        return codecs.charmap_decode(input, self.errors, DECODING_TABLE)[  # type: ignore[arg-type]
            0
        ]


class StreamWriter(Codec, codecs.StreamWriter):
    """PETSCII stream writer."""


class StreamReader(Codec, codecs.StreamReader):
    """PETSCII stream reader."""


def getregentry() -> codecs.CodecInfo:
    """Return the codec registry entry."""
    return codecs.CodecInfo(
        name="petscii",
        encode=Codec().encode,
        decode=Codec().decode,  # type: ignore[arg-type]
        incrementalencoder=IncrementalEncoder,
        incrementaldecoder=IncrementalDecoder,
        streamreader=StreamReader,
        streamwriter=StreamWriter,
    )


def getaliases() -> tuple[str, ...]:
    """Return codec aliases."""
    return ("cbm", "commodore", "c64", "c128")


ENCODING_TABLE = codecs.charmap_build(DECODING_TABLE)
