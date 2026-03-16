"""
Atari ST codec.

Generated from ftp://ftp.unicode.org/Public/MAPPINGS/VENDORS/MISC/ATARIST.TXT
"""

# std imports
import codecs


class Codec(codecs.Codec):
    """Atari ST character map codec."""

    def encode(self, input: str, errors: str = "strict") -> tuple[bytes, int]:
        """Encode input string using Atari ST character map."""
        return codecs.charmap_encode(input, errors, ENCODING_TABLE)

    def decode(self, input: bytes, errors: str = "strict") -> tuple[str, int]:
        """Decode input bytes using Atari ST character map."""
        return codecs.charmap_decode(input, errors, DECODING_TABLE)  # type: ignore[arg-type]


class IncrementalEncoder(codecs.IncrementalEncoder):
    """Atari ST incremental encoder."""

    def encode(self, input: str, final: bool = False) -> bytes:
        """Encode input string incrementally."""
        return codecs.charmap_encode(input, self.errors, ENCODING_TABLE)[0]


class IncrementalDecoder(codecs.IncrementalDecoder):
    """Atari ST incremental decoder."""

    def decode(self, input: bytes, final: bool = False) -> str:  # type: ignore[override]
        """Decode input bytes incrementally."""
        return codecs.charmap_decode(input, self.errors, DECODING_TABLE)[  # type: ignore[arg-type]
            0
        ]


class StreamWriter(Codec, codecs.StreamWriter):
    """Atari ST stream writer."""


class StreamReader(Codec, codecs.StreamReader):
    """Atari ST stream reader."""


def getaliases() -> tuple[str, ...]:
    """Return codec aliases."""
    return ("atari",)


def getregentry() -> codecs.CodecInfo:
    """Return the codec registry entry."""
    return codecs.CodecInfo(
        name="atarist",
        encode=Codec().encode,
        decode=Codec().decode,  # type: ignore[arg-type]
        incrementalencoder=IncrementalEncoder,
        incrementaldecoder=IncrementalDecoder,
        streamreader=StreamReader,
        streamwriter=StreamWriter,
    )


# Decoding Table

DECODING_TABLE = (
    "\x00"  # 0x00 -> NULL
    "\x01"  # 0x01 -> START OF HEADING
    "\x02"  # 0x02 -> START OF TEXT
    "\x03"  # 0x03 -> END OF TEXT
    "\x04"  # 0x04 -> END OF TRANSMISSION
    "\x05"  # 0x05 -> ENQUIRY
    "\x06"  # 0x06 -> ACKNOWLEDGE
    "\x07"  # 0x07 -> BELL
    "\x08"  # 0x08 -> BACKSPACE
    "\t"  # 0x09 -> HORIZONTAL TABULATION
    "\n"  # 0x0A -> LINE FEED
    "\x0b"  # 0x0B -> VERTICAL TABULATION
    "\x0c"  # 0x0C -> FORM FEED
    "\r"  # 0x0D -> CARRIAGE RETURN
    "\x0e"  # 0x0E -> SHIFT OUT
    "\x0f"  # 0x0F -> SHIFT IN
    "\x10"  # 0x10 -> DATA LINK ESCAPE
    "\x11"  # 0x11 -> DEVICE CONTROL ONE
    "\x12"  # 0x12 -> DEVICE CONTROL TWO
    "\x13"  # 0x13 -> DEVICE CONTROL THREE
    "\x14"  # 0x14 -> DEVICE CONTROL FOUR
    "\x15"  # 0x15 -> NEGATIVE ACKNOWLEDGE
    "\x16"  # 0x16 -> SYNCHRONOUS IDLE
    "\x17"  # 0x17 -> END OF TRANSMISSION BLOCK
    "\x18"  # 0x18 -> CANCEL
    "\x19"  # 0x19 -> END OF MEDIUM
    "\x1a"  # 0x1A -> SUBSTITUTE
    "\x1b"  # 0x1B -> ESCAPE
    "\x1c"  # 0x1C -> FILE SEPARATOR
    "\x1d"  # 0x1D -> GROUP SEPARATOR
    "\x1e"  # 0x1E -> RECORD SEPARATOR
    "\x1f"  # 0x1F -> UNIT SEPARATOR
    " "  # 0x20 -> SPACE
    "!"  # 0x21 -> EXCLAMATION MARK
    '"'  # 0x22 -> QUOTATION MARK
    "#"  # 0x23 -> NUMBER SIGN
    "$"  # 0x24 -> DOLLAR SIGN
    "%"  # 0x25 -> PERCENT SIGN
    "&"  # 0x26 -> AMPERSAND
    "'"  # 0x27 -> APOSTROPHE
    "("  # 0x28 -> LEFT PARENTHESIS
    ")"  # 0x29 -> RIGHT PARENTHESIS
    "*"  # 0x2A -> ASTERISK
    "+"  # 0x2B -> PLUS SIGN
    ","  # 0x2C -> COMMA
    "-"  # 0x2D -> HYPHEN-MINUS
    "."  # 0x2E -> FULL STOP
    "/"  # 0x2F -> SOLIDUS
    "0"  # 0x30 -> DIGIT ZERO
    "1"  # 0x31 -> DIGIT ONE
    "2"  # 0x32 -> DIGIT TWO
    "3"  # 0x33 -> DIGIT THREE
    "4"  # 0x34 -> DIGIT FOUR
    "5"  # 0x35 -> DIGIT FIVE
    "6"  # 0x36 -> DIGIT SIX
    "7"  # 0x37 -> DIGIT SEVEN
    "8"  # 0x38 -> DIGIT EIGHT
    "9"  # 0x39 -> DIGIT NINE
    ":"  # 0x3A -> COLON
    ";"  # 0x3B -> SEMICOLON
    "<"  # 0x3C -> LESS-THAN SIGN
    "="  # 0x3D -> EQUALS SIGN
    ">"  # 0x3E -> GREATER-THAN SIGN
    "?"  # 0x3F -> QUESTION MARK
    "@"  # 0x40 -> COMMERCIAL AT
    "A"  # 0x41 -> LATIN CAPITAL LETTER A
    "B"  # 0x42 -> LATIN CAPITAL LETTER B
    "C"  # 0x43 -> LATIN CAPITAL LETTER C
    "D"  # 0x44 -> LATIN CAPITAL LETTER D
    "E"  # 0x45 -> LATIN CAPITAL LETTER E
    "F"  # 0x46 -> LATIN CAPITAL LETTER F
    "G"  # 0x47 -> LATIN CAPITAL LETTER G
    "H"  # 0x48 -> LATIN CAPITAL LETTER H
    "I"  # 0x49 -> LATIN CAPITAL LETTER I
    "J"  # 0x4A -> LATIN CAPITAL LETTER J
    "K"  # 0x4B -> LATIN CAPITAL LETTER K
    "L"  # 0x4C -> LATIN CAPITAL LETTER L
    "M"  # 0x4D -> LATIN CAPITAL LETTER M
    "N"  # 0x4E -> LATIN CAPITAL LETTER N
    "O"  # 0x4F -> LATIN CAPITAL LETTER O
    "P"  # 0x50 -> LATIN CAPITAL LETTER P
    "Q"  # 0x51 -> LATIN CAPITAL LETTER Q
    "R"  # 0x52 -> LATIN CAPITAL LETTER R
    "S"  # 0x53 -> LATIN CAPITAL LETTER S
    "T"  # 0x54 -> LATIN CAPITAL LETTER T
    "U"  # 0x55 -> LATIN CAPITAL LETTER U
    "V"  # 0x56 -> LATIN CAPITAL LETTER V
    "W"  # 0x57 -> LATIN CAPITAL LETTER W
    "X"  # 0x58 -> LATIN CAPITAL LETTER X
    "Y"  # 0x59 -> LATIN CAPITAL LETTER Y
    "Z"  # 0x5A -> LATIN CAPITAL LETTER Z
    "["  # 0x5B -> LEFT SQUARE BRACKET
    "\\"  # 0x5C -> REVERSE SOLIDUS
    "]"  # 0x5D -> RIGHT SQUARE BRACKET
    "^"  # 0x5E -> CIRCUMFLEX ACCENT
    "_"  # 0x5F -> LOW LINE
    "`"  # 0x60 -> GRAVE ACCENT
    "a"  # 0x61 -> LATIN SMALL LETTER A
    "b"  # 0x62 -> LATIN SMALL LETTER B
    "c"  # 0x63 -> LATIN SMALL LETTER C
    "d"  # 0x64 -> LATIN SMALL LETTER D
    "e"  # 0x65 -> LATIN SMALL LETTER E
    "f"  # 0x66 -> LATIN SMALL LETTER F
    "g"  # 0x67 -> LATIN SMALL LETTER G
    "h"  # 0x68 -> LATIN SMALL LETTER H
    "i"  # 0x69 -> LATIN SMALL LETTER I
    "j"  # 0x6A -> LATIN SMALL LETTER J
    "k"  # 0x6B -> LATIN SMALL LETTER K
    "l"  # 0x6C -> LATIN SMALL LETTER L
    "m"  # 0x6D -> LATIN SMALL LETTER M
    "n"  # 0x6E -> LATIN SMALL LETTER N
    "o"  # 0x6F -> LATIN SMALL LETTER O
    "p"  # 0x70 -> LATIN SMALL LETTER P
    "q"  # 0x71 -> LATIN SMALL LETTER Q
    "r"  # 0x72 -> LATIN SMALL LETTER R
    "s"  # 0x73 -> LATIN SMALL LETTER S
    "t"  # 0x74 -> LATIN SMALL LETTER T
    "u"  # 0x75 -> LATIN SMALL LETTER U
    "v"  # 0x76 -> LATIN SMALL LETTER V
    "w"  # 0x77 -> LATIN SMALL LETTER W
    "x"  # 0x78 -> LATIN SMALL LETTER X
    "y"  # 0x79 -> LATIN SMALL LETTER Y
    "z"  # 0x7A -> LATIN SMALL LETTER Z
    "{"  # 0x7B -> LEFT CURLY BRACKET
    "|"  # 0x7C -> VERTICAL LINE
    "}"  # 0x7D -> RIGHT CURLY BRACKET
    "~"  # 0x7E -> TILDE
    "\x7f"  # 0x7F -> DELETE
    "\xc7"  # 0x80 -> LATIN CAPITAL LETTER C WITH CEDILLA
    "\xfc"  # 0x81 -> LATIN SMALL LETTER U WITH DIAERESIS
    "\xe9"  # 0x82 -> LATIN SMALL LETTER E WITH ACUTE
    "\xe2"  # 0x83 -> LATIN SMALL LETTER A WITH CIRCUMFLEX
    "\xe4"  # 0x84 -> LATIN SMALL LETTER A WITH DIAERESIS
    "\xe0"  # 0x85 -> LATIN SMALL LETTER A WITH GRAVE
    "\xe5"  # 0x86 -> LATIN SMALL LETTER A WITH RING ABOVE
    "\xe7"  # 0x87 -> LATIN SMALL LETTER C WITH CEDILLA
    "\xea"  # 0x88 -> LATIN SMALL LETTER E WITH CIRCUMFLEX
    "\xeb"  # 0x89 -> LATIN SMALL LETTER E WITH DIAERESIS
    "\xe8"  # 0x8A -> LATIN SMALL LETTER E WITH GRAVE
    "\xef"  # 0x8B -> LATIN SMALL LETTER I WITH DIAERESIS
    "\xee"  # 0x8C -> LATIN SMALL LETTER I WITH CIRCUMFLEX
    "\xec"  # 0x8D -> LATIN SMALL LETTER I WITH GRAVE
    "\xc4"  # 0x8E -> LATIN CAPITAL LETTER A WITH DIAERESIS
    "\xc5"  # 0x8F -> LATIN CAPITAL LETTER A WITH RING ABOVE
    "\xc9"  # 0x90 -> LATIN CAPITAL LETTER E WITH ACUTE
    "\xe6"  # 0x91 -> LATIN SMALL LETTER AE
    "\xc6"  # 0x92 -> LATIN CAPITAL LETTER AE
    "\xf4"  # 0x93 -> LATIN SMALL LETTER O WITH CIRCUMFLEX
    "\xf6"  # 0x94 -> LATIN SMALL LETTER O WITH DIAERESIS
    "\xf2"  # 0x95 -> LATIN SMALL LETTER O WITH GRAVE
    "\xfb"  # 0x96 -> LATIN SMALL LETTER U WITH CIRCUMFLEX
    "\xf9"  # 0x97 -> LATIN SMALL LETTER U WITH GRAVE
    "\xff"  # 0x98 -> LATIN SMALL LETTER Y WITH DIAERESIS
    "\xd6"  # 0x99 -> LATIN CAPITAL LETTER O WITH DIAERESIS
    "\xdc"  # 0x9A -> LATIN CAPITAL LETTER U WITH DIAERESIS
    "\xa2"  # 0x9B -> CENT SIGN
    "\xa3"  # 0x9C -> POUND SIGN
    "\xa5"  # 0x9D -> YEN SIGN
    "\xdf"  # 0x9E -> LATIN SMALL LETTER SHARP S
    "\u0192"  # 0x9F -> LATIN SMALL LETTER F WITH HOOK
    "\xe1"  # 0xA0 -> LATIN SMALL LETTER A WITH ACUTE
    "\xed"  # 0xA1 -> LATIN SMALL LETTER I WITH ACUTE
    "\xf3"  # 0xA2 -> LATIN SMALL LETTER O WITH ACUTE
    "\xfa"  # 0xA3 -> LATIN SMALL LETTER U WITH ACUTE
    "\xf1"  # 0xA4 -> LATIN SMALL LETTER N WITH TILDE
    "\xd1"  # 0xA5 -> LATIN CAPITAL LETTER N WITH TILDE
    "\xaa"  # 0xA6 -> FEMININE ORDINAL INDICATOR
    "\xba"  # 0xA7 -> MASCULINE ORDINAL INDICATOR
    "\xbf"  # 0xA8 -> INVERTED QUESTION MARK
    "\u2310"  # 0xA9 -> REVERSED NOT SIGN
    "\xac"  # 0xAA -> NOT SIGN
    "\xbd"  # 0xAB -> VULGAR FRACTION ONE HALF
    "\xbc"  # 0xAC -> VULGAR FRACTION ONE QUARTER
    "\xa1"  # 0xAD -> INVERTED EXCLAMATION MARK
    "\xab"  # 0xAE -> LEFT-POINTING DOUBLE ANGLE QUOTATION MARK
    "\xbb"  # 0xAF -> RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK
    "\xe3"  # 0xB0 -> LATIN SMALL LETTER A WITH TILDE
    "\xf5"  # 0xB1 -> LATIN SMALL LETTER O WITH TILDE
    "\xd8"  # 0xB2 -> LATIN CAPITAL LETTER O WITH STROKE
    "\xf8"  # 0xB3 -> LATIN SMALL LETTER O WITH STROKE
    "\u0153"  # 0xB4 -> LATIN SMALL LIGATURE OE
    "\u0152"  # 0xB5 -> LATIN CAPITAL LIGATURE OE
    "\xc0"  # 0xB6 -> LATIN CAPITAL LETTER A WITH GRAVE
    "\xc3"  # 0xB7 -> LATIN CAPITAL LETTER A WITH TILDE
    "\xd5"  # 0xB8 -> LATIN CAPITAL LETTER O WITH TILDE
    "\xa8"  # 0xB9 -> DIAERESIS
    "\xb4"  # 0xBA -> ACUTE ACCENT
    "\u2020"  # 0xBB -> DAGGER
    "\xb6"  # 0xBC -> PILCROW SIGN
    "\xa9"  # 0xBD -> COPYRIGHT SIGN
    "\xae"  # 0xBE -> REGISTERED SIGN
    "\u2122"  # 0xBF -> TRADE MARK SIGN
    "\u0133"  # 0xC0 -> LATIN SMALL LIGATURE IJ
    "\u0132"  # 0xC1 -> LATIN CAPITAL LIGATURE IJ
    "\u05d0"  # 0xC2 -> HEBREW LETTER ALEF
    "\u05d1"  # 0xC3 -> HEBREW LETTER BET
    "\u05d2"  # 0xC4 -> HEBREW LETTER GIMEL
    "\u05d3"  # 0xC5 -> HEBREW LETTER DALET
    "\u05d4"  # 0xC6 -> HEBREW LETTER HE
    "\u05d5"  # 0xC7 -> HEBREW LETTER VAV
    "\u05d6"  # 0xC8 -> HEBREW LETTER ZAYIN
    "\u05d7"  # 0xC9 -> HEBREW LETTER HET
    "\u05d8"  # 0xCA -> HEBREW LETTER TET
    "\u05d9"  # 0xCB -> HEBREW LETTER YOD
    "\u05db"  # 0xCC -> HEBREW LETTER KAF
    "\u05dc"  # 0xCD -> HEBREW LETTER LAMED
    "\u05de"  # 0xCE -> HEBREW LETTER MEM
    "\u05e0"  # 0xCF -> HEBREW LETTER NUN
    "\u05e1"  # 0xD0 -> HEBREW LETTER SAMEKH
    "\u05e2"  # 0xD1 -> HEBREW LETTER AYIN
    "\u05e4"  # 0xD2 -> HEBREW LETTER PE
    "\u05e6"  # 0xD3 -> HEBREW LETTER TSADI
    "\u05e7"  # 0xD4 -> HEBREW LETTER QOF
    "\u05e8"  # 0xD5 -> HEBREW LETTER RESH
    "\u05e9"  # 0xD6 -> HEBREW LETTER SHIN
    "\u05ea"  # 0xD7 -> HEBREW LETTER TAV
    "\u05df"  # 0xD8 -> HEBREW LETTER FINAL NUN
    "\u05da"  # 0xD9 -> HEBREW LETTER FINAL KAF
    "\u05dd"  # 0xDA -> HEBREW LETTER FINAL MEM
    "\u05e3"  # 0xDB -> HEBREW LETTER FINAL PE
    "\u05e5"  # 0xDC -> HEBREW LETTER FINAL TSADI
    "\xa7"  # 0xDD -> SECTION SIGN
    "\u2227"  # 0xDE -> LOGICAL AND
    "\u221e"  # 0xDF -> INFINITY
    "\u03b1"  # 0xE0 -> GREEK SMALL LETTER ALPHA
    "\u03b2"  # 0xE1 -> GREEK SMALL LETTER BETA
    "\u0393"  # 0xE2 -> GREEK CAPITAL LETTER GAMMA
    "\u03c0"  # 0xE3 -> GREEK SMALL LETTER PI
    "\u03a3"  # 0xE4 -> GREEK CAPITAL LETTER SIGMA
    "\u03c3"  # 0xE5 -> GREEK SMALL LETTER SIGMA
    "\xb5"  # 0xE6 -> MICRO SIGN
    "\u03c4"  # 0xE7 -> GREEK SMALL LETTER TAU
    "\u03a6"  # 0xE8 -> GREEK CAPITAL LETTER PHI
    "\u0398"  # 0xE9 -> GREEK CAPITAL LETTER THETA
    "\u03a9"  # 0xEA -> GREEK CAPITAL LETTER OMEGA
    "\u03b4"  # 0xEB -> GREEK SMALL LETTER DELTA
    "\u222e"  # 0xEC -> CONTOUR INTEGRAL
    "\u03c6"  # 0xED -> GREEK SMALL LETTER PHI
    "\u2208"  # 0xEE -> ELEMENT OF SIGN
    "\u2229"  # 0xEF -> INTERSECTION
    "\u2261"  # 0xF0 -> IDENTICAL TO
    "\xb1"  # 0xF1 -> PLUS-MINUS SIGN
    "\u2265"  # 0xF2 -> GREATER-THAN OR EQUAL TO
    "\u2264"  # 0xF3 -> LESS-THAN OR EQUAL TO
    "\u2320"  # 0xF4 -> TOP HALF INTEGRAL
    "\u2321"  # 0xF5 -> BOTTOM HALF INTEGRAL
    "\xf7"  # 0xF6 -> DIVISION SIGN
    "\u2248"  # 0xF7 -> ALMOST EQUAL TO
    "\xb0"  # 0xF8 -> DEGREE SIGN
    "\u2219"  # 0xF9 -> BULLET OPERATOR
    "\xb7"  # 0xFA -> MIDDLE DOT
    "\u221a"  # 0xFB -> SQUARE ROOT
    "\u207f"  # 0xFC -> SUPERSCRIPT LATIN SMALL LETTER N
    "\xb2"  # 0xFD -> SUPERSCRIPT TWO
    "\xb3"  # 0xFE -> SUPERSCRIPT THREE
    "\xaf"  # 0xFF -> MACRON
)

# Encoding table
ENCODING_TABLE = codecs.charmap_build(DECODING_TABLE)
