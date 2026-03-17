"""
This gives other modules access to the gritty details about characters and the
encodings that use them.
"""

from __future__ import annotations

import html
import itertools
import re
import unicodedata

# These are the encodings we will try to fix in ftfy, in the
# order that they should be tried.
CHARMAP_ENCODINGS = [
    "latin-1",
    "sloppy-windows-1252",
    "sloppy-windows-1251",
    "sloppy-windows-1250",
    "sloppy-windows-1253",
    "sloppy-windows-1254",
    "sloppy-windows-1257",
    "iso-8859-2",
    "macroman",
    "cp437",
]

SINGLE_QUOTE_RE = re.compile("[\u02bc\u2018-\u201b]")
DOUBLE_QUOTE_RE = re.compile("[\u201c-\u201f]")


def _build_regexes() -> dict[str, re.Pattern[str]]:
    """
    ENCODING_REGEXES contain reasonably fast ways to detect if we
    could represent a given string in a given encoding. The simplest one is
    the 'ascii' detector, which of course just determines if all characters
    are between U+0000 and U+007F.
    """
    # Define a regex that matches ASCII text.
    encoding_regexes = {"ascii": re.compile("^[\x00-\x7f]*$")}

    for encoding in CHARMAP_ENCODINGS:
        # Make a sequence of characters that bytes \x80 to \xFF decode to
        # in each encoding, as well as byte \x1A, which is used to represent
        # the replacement character � in the sloppy-* encodings.
        byte_range = bytes(list(range(0x80, 0x100)) + [0x1A])
        charlist = byte_range.decode(encoding)

        # The rest of the ASCII bytes -- bytes \x00 to \x19 and \x1B
        # to \x7F -- will decode as those ASCII characters in any encoding we
        # support, so we can just include them as ranges. This also lets us
        # not worry about escaping regex special characters, because all of
        # them are in the \x1B to \x7F range.
        regex = f"^[\x00-\x19\x1b-\x7f{charlist}]*$"
        encoding_regexes[encoding] = re.compile(regex)
    return encoding_regexes


ENCODING_REGEXES = _build_regexes()


def _build_html_entities() -> dict[str, str]:
    entities = {}
    # Create a dictionary based on the built-in HTML5 entity dictionary.
    # Add a limited set of HTML entities that we'll also decode if they've
    # been case-folded to uppercase, such as decoding &NTILDE; as "Ñ".
    for name, char in html.entities.html5.items():  # type: ignore
        if name.endswith(";"):
            entities["&" + name] = char

            # Restrict the set of characters we can attempt to decode if their
            # name has been uppercased. If we tried to handle all entity names,
            # the results would be ambiguous.
            if name == name.lower():
                name_upper = name.upper()
                entity_upper = "&" + name_upper
                if html.unescape(entity_upper) == entity_upper:
                    entities[entity_upper] = char.upper()
    return entities


HTML_ENTITY_RE = re.compile(r"&#?[0-9A-Za-z]{1,24};")
HTML_ENTITIES = _build_html_entities()


def possible_encoding(text: str, encoding: str) -> bool:
    """
    Given text and a single-byte encoding, check whether that text could have
    been decoded from that single-byte encoding.

    In other words, check whether it can be encoded in that encoding, possibly
    sloppily.
    """
    return bool(ENCODING_REGEXES[encoding].match(text))


def _build_control_char_mapping() -> dict[int, None]:
    """
    Build a translate mapping that strips likely-unintended control characters.
    See :func:`ftfy.fixes.remove_control_chars` for a description of these
    codepoint ranges and why they should be removed.
    """
    control_chars: dict[int, None] = {}

    for i in itertools.chain(
        range(0x00, 0x09),
        [0x0B],
        range(0x0E, 0x20),
        [0x7F],
        range(0x206A, 0x2070),
        [0xFEFF],
        range(0xFFF9, 0xFFFD),
    ):
        control_chars[i] = None

    return control_chars


CONTROL_CHARS = _build_control_char_mapping()


# Recognize UTF-8 sequences that would be valid if it weren't for a b'\xa0'
# that some Windows-1252 program converted to a plain space.
#
# The smaller values are included on a case-by-case basis, because we don't want
# to decode likely input sequences to unlikely characters. These are the ones
# that *do* form likely characters before 0xa0:
#
#   0xc2 -> U+A0 NO-BREAK SPACE
#   0xc3 -> U+E0 LATIN SMALL LETTER A WITH GRAVE
#   0xc5 -> U+160 LATIN CAPITAL LETTER S WITH CARON
#   0xce -> U+3A0 GREEK CAPITAL LETTER PI
#   0xd0 -> U+420 CYRILLIC CAPITAL LETTER ER
#   0xd9 -> U+660 ARABIC-INDIC DIGIT ZERO
#
# In three-character sequences, we exclude some lead bytes in some cases.
#
# When the lead byte is immediately followed by 0xA0, we shouldn't accept
# a space there, because it leads to some less-likely character ranges:
#
#   0xe0 -> Samaritan script
#   0xe1 -> Mongolian script (corresponds to Latin-1 'á' which is too common)
#
# We accept 0xe2 and 0xe3, which cover many scripts. Bytes 0xe4 and
# higher point mostly to CJK characters, which we generally don't want to
# decode near Latin lowercase letters.
#
# In four-character sequences, the lead byte must be F0, because that accounts
# for almost all of the usage of high-numbered codepoints (tag characters whose
# UTF-8 starts with the byte F3 are only used in some rare new emoji sequences).
#
# This is meant to be applied to encodings of text that tests true for `is_bad`.
# Any of these could represent characters that legitimately appear surrounded by
# spaces, particularly U+C5 (Å), which is a word in multiple languages!
#
# We should consider checking for b'\x85' being converted to ... in the future.
# I've seen it once, but the text still wasn't recoverable.

ALTERED_UTF8_RE = re.compile(
    b"[\xc2\xc3\xc5\xce\xd0\xd9][ ]"
    b"|[\xe2\xe3][ ][\x80-\x84\x86-\x9f\xa1-\xbf]"
    b"|[\xe0-\xe3][\x80-\x84\x86-\x9f\xa1-\xbf][ ]"
    b"|[\xf0][ ][\x80-\xbf][\x80-\xbf]"
    b"|[\xf0][\x80-\xbf][ ][\x80-\xbf]"
    b"|[\xf0][\x80-\xbf][\x80-\xbf][ ]"
)


# This expression matches UTF-8 and CESU-8 sequences where some of the
# continuation bytes have been lost. The byte 0x1a (sometimes written as ^Z) is
# used within ftfy to represent a byte that produced the replacement character
# \ufffd. We don't know which byte it was, but we can at least decode the UTF-8
# sequence as \ufffd instead of failing to re-decode it at all.
#
# In some cases, we allow the ASCII '?' in place of \ufffd, but at most once per
# sequence.
LOSSY_UTF8_RE = re.compile(
    b"[\xc2-\xdf][\x1a]"
    b"|[\xc2-\xc3][?]"
    b"|\xed[\xa0-\xaf][\x1a?]\xed[\xb0-\xbf][\x1a?\x80-\xbf]"
    b"|\xed[\xa0-\xaf][\x1a?\x80-\xbf]\xed[\xb0-\xbf][\x1a?]"
    b"|[\xe0-\xef][\x1a?][\x1a\x80-\xbf]"
    b"|[\xe0-\xef][\x1a\x80-\xbf][\x1a?]"
    b"|[\xf0-\xf4][\x1a?][\x1a\x80-\xbf][\x1a\x80-\xbf]"
    b"|[\xf0-\xf4][\x1a\x80-\xbf][\x1a?][\x1a\x80-\xbf]"
    b"|[\xf0-\xf4][\x1a\x80-\xbf][\x1a\x80-\xbf][\x1a?]"
    b"|\x1a"
)


# This regex matches C1 control characters, which occupy some of the positions
# in the Latin-1 character map that Windows assigns to other characters instead.
C1_CONTROL_RE = re.compile(r"[\x80-\x9f]")


# A translate mapping that breaks ligatures made of Latin letters. While
# ligatures may be important to the representation of other languages, in Latin
# letters they tend to represent a copy/paste error. It omits ligatures such
# as æ that are frequently used intentionally.
#
# This list additionally includes some Latin digraphs that represent two
# characters for legacy encoding reasons, not for typographical reasons.
#
# Ligatures and digraphs may also be separated by NFKC normalization, but that
# is sometimes more normalization than you want.

LIGATURES = {
    ord("Ĳ"): "IJ",  # Dutch ligatures
    ord("ĳ"): "ij",
    ord("ŉ"): "ʼn",  # Afrikaans digraph meant to avoid auto-curled quote
    ord("Ǳ"): "DZ",  # Serbian/Croatian digraphs for Cyrillic conversion
    ord("ǲ"): "Dz",
    ord("ǳ"): "dz",
    ord("Ǆ"): "DŽ",
    ord("ǅ"): "Dž",
    ord("ǆ"): "dž",
    ord("Ǉ"): "LJ",
    ord("ǈ"): "Lj",
    ord("ǉ"): "lj",
    ord("Ǌ"): "NJ",
    ord("ǋ"): "Nj",
    ord("ǌ"): "nj",
    ord("ﬀ"): "ff",  # Latin typographical ligatures
    ord("ﬁ"): "fi",
    ord("ﬂ"): "fl",
    ord("ﬃ"): "ffi",
    ord("ﬄ"): "ffl",
    ord("ﬅ"): "ſt",
    ord("ﬆ"): "st",
}


def _build_width_map() -> dict[int, str]:
    """
    Build a translate mapping that replaces halfwidth and fullwidth forms
    with their standard-width forms.
    """
    # Though it's not listed as a fullwidth character, we'll want to convert
    # U+3000 IDEOGRAPHIC SPACE to U+20 SPACE on the same principle, so start
    # with that in the dictionary.
    width_map = {0x3000: " "}
    for i in range(0xFF01, 0xFFF0):
        char = chr(i)
        alternate = unicodedata.normalize("NFKC", char)
        if alternate != char:
            width_map[i] = alternate
    return width_map


WIDTH_MAP = _build_width_map()


# Character classes that help us pinpoint embedded mojibake. These can
# include common characters, because we'll also check them for 'badness'.
#
# Though they go on for many lines, the members of this dictionary are
# single concatenated strings.
#
# This code is generated using scripts/char_data_table.py.
UTF8_CLUES: dict[str, str] = {
    # Letters that decode to 0xC2 - 0xDF in a Latin-1-like encoding
    "utf8_first_of_2": (
        "\N{LATIN CAPITAL LETTER A WITH BREVE}"  # windows-1250:C3
        "\N{LATIN CAPITAL LETTER A WITH CIRCUMFLEX}"  # latin-1:C2
        "\N{LATIN CAPITAL LETTER A WITH DIAERESIS}"  # latin-1:C4
        "\N{LATIN CAPITAL LETTER A WITH MACRON}"  # windows-1257:C2
        "\N{LATIN CAPITAL LETTER A WITH RING ABOVE}"  # latin-1:C5
        "\N{LATIN CAPITAL LETTER A WITH TILDE}"  # latin-1:C3
        "\N{LATIN CAPITAL LETTER AE}"  # latin-1:C6
        "\N{LATIN CAPITAL LETTER C WITH ACUTE}"  # windows-1250:C6
        "\N{LATIN CAPITAL LETTER C WITH CARON}"  # windows-1250:C8
        "\N{LATIN CAPITAL LETTER C WITH CEDILLA}"  # latin-1:C7
        "\N{LATIN CAPITAL LETTER D WITH CARON}"  # windows-1250:CF
        "\N{LATIN CAPITAL LETTER D WITH STROKE}"  # windows-1250:D0
        "\N{LATIN CAPITAL LETTER E WITH ACUTE}"  # latin-1:C9
        "\N{LATIN CAPITAL LETTER E WITH CARON}"  # windows-1250:CC
        "\N{LATIN CAPITAL LETTER E WITH CIRCUMFLEX}"  # latin-1:CA
        "\N{LATIN CAPITAL LETTER E WITH DIAERESIS}"  # latin-1:CB
        "\N{LATIN CAPITAL LETTER E WITH DOT ABOVE}"  # windows-1257:CB
        "\N{LATIN CAPITAL LETTER E WITH GRAVE}"  # latin-1:C8
        "\N{LATIN CAPITAL LETTER E WITH MACRON}"  # windows-1257:C7
        "\N{LATIN CAPITAL LETTER E WITH OGONEK}"  # windows-1250:CA
        "\N{LATIN CAPITAL LETTER ETH}"  # latin-1:D0
        "\N{LATIN CAPITAL LETTER G WITH BREVE}"  # windows-1254:D0
        "\N{LATIN CAPITAL LETTER G WITH CEDILLA}"  # windows-1257:CC
        "\N{LATIN CAPITAL LETTER I WITH ACUTE}"  # latin-1:CD
        "\N{LATIN CAPITAL LETTER I WITH CIRCUMFLEX}"  # latin-1:CE
        "\N{LATIN CAPITAL LETTER I WITH DIAERESIS}"  # latin-1:CF
        "\N{LATIN CAPITAL LETTER I WITH DOT ABOVE}"  # windows-1254:DD
        "\N{LATIN CAPITAL LETTER I WITH GRAVE}"  # latin-1:CC
        "\N{LATIN CAPITAL LETTER I WITH MACRON}"  # windows-1257:CE
        "\N{LATIN CAPITAL LETTER K WITH CEDILLA}"  # windows-1257:CD
        "\N{LATIN CAPITAL LETTER L WITH ACUTE}"  # windows-1250:C5
        "\N{LATIN CAPITAL LETTER L WITH CEDILLA}"  # windows-1257:CF
        "\N{LATIN CAPITAL LETTER L WITH STROKE}"  # windows-1257:D9
        "\N{LATIN CAPITAL LETTER N WITH ACUTE}"  # windows-1250:D1
        "\N{LATIN CAPITAL LETTER N WITH CARON}"  # windows-1250:D2
        "\N{LATIN CAPITAL LETTER N WITH CEDILLA}"  # windows-1257:D2
        "\N{LATIN CAPITAL LETTER N WITH TILDE}"  # latin-1:D1
        "\N{LATIN CAPITAL LETTER O WITH ACUTE}"  # latin-1:D3
        "\N{LATIN CAPITAL LETTER O WITH CIRCUMFLEX}"  # latin-1:D4
        "\N{LATIN CAPITAL LETTER O WITH DIAERESIS}"  # latin-1:D6
        "\N{LATIN CAPITAL LETTER O WITH DOUBLE ACUTE}"  # windows-1250:D5
        "\N{LATIN CAPITAL LETTER O WITH GRAVE}"  # latin-1:D2
        "\N{LATIN CAPITAL LETTER O WITH MACRON}"  # windows-1257:D4
        "\N{LATIN CAPITAL LETTER O WITH STROKE}"  # latin-1:D8
        "\N{LATIN CAPITAL LETTER O WITH TILDE}"  # latin-1:D5
        "\N{LATIN CAPITAL LETTER R WITH CARON}"  # windows-1250:D8
        "\N{LATIN CAPITAL LETTER S WITH ACUTE}"  # windows-1257:DA
        "\N{LATIN CAPITAL LETTER S WITH CARON}"  # windows-1257:D0
        "\N{LATIN CAPITAL LETTER S WITH CEDILLA}"  # windows-1254:DE
        "\N{LATIN CAPITAL LETTER T WITH CEDILLA}"  # windows-1250:DE
        "\N{LATIN CAPITAL LETTER THORN}"  # latin-1:DE
        "\N{LATIN CAPITAL LETTER U WITH ACUTE}"  # latin-1:DA
        "\N{LATIN CAPITAL LETTER U WITH CIRCUMFLEX}"  # latin-1:DB
        "\N{LATIN CAPITAL LETTER U WITH DIAERESIS}"  # latin-1:DC
        "\N{LATIN CAPITAL LETTER U WITH DOUBLE ACUTE}"  # windows-1250:DB
        "\N{LATIN CAPITAL LETTER U WITH GRAVE}"  # latin-1:D9
        "\N{LATIN CAPITAL LETTER U WITH MACRON}"  # windows-1257:DB
        "\N{LATIN CAPITAL LETTER U WITH OGONEK}"  # windows-1257:D8
        "\N{LATIN CAPITAL LETTER U WITH RING ABOVE}"  # windows-1250:D9
        "\N{LATIN CAPITAL LETTER Y WITH ACUTE}"  # latin-1:DD
        "\N{LATIN CAPITAL LETTER Z WITH ACUTE}"  # windows-1257:CA
        "\N{LATIN CAPITAL LETTER Z WITH CARON}"  # windows-1257:DE
        "\N{LATIN CAPITAL LETTER Z WITH DOT ABOVE}"  # windows-1257:DD
        "\N{LATIN SMALL LETTER SHARP S}"  # latin-1:DF
        "\N{MULTIPLICATION SIGN}"  # latin-1:D7
        "\N{GREEK CAPITAL LETTER BETA}"  # windows-1253:C2
        "\N{GREEK CAPITAL LETTER GAMMA}"  # windows-1253:C3
        "\N{GREEK CAPITAL LETTER DELTA}"  # windows-1253:C4
        "\N{GREEK CAPITAL LETTER EPSILON}"  # windows-1253:C5
        "\N{GREEK CAPITAL LETTER ZETA}"  # windows-1253:C6
        "\N{GREEK CAPITAL LETTER ETA}"  # windows-1253:C7
        "\N{GREEK CAPITAL LETTER THETA}"  # windows-1253:C8
        "\N{GREEK CAPITAL LETTER IOTA}"  # windows-1253:C9
        "\N{GREEK CAPITAL LETTER KAPPA}"  # windows-1253:CA
        "\N{GREEK CAPITAL LETTER LAMDA}"  # windows-1253:CB
        "\N{GREEK CAPITAL LETTER MU}"  # windows-1253:CC
        "\N{GREEK CAPITAL LETTER NU}"  # windows-1253:CD
        "\N{GREEK CAPITAL LETTER XI}"  # windows-1253:CE
        "\N{GREEK CAPITAL LETTER OMICRON}"  # windows-1253:CF
        "\N{GREEK CAPITAL LETTER PI}"  # windows-1253:D0
        "\N{GREEK CAPITAL LETTER RHO}"  # windows-1253:D1
        "\N{GREEK CAPITAL LETTER SIGMA}"  # windows-1253:D3
        "\N{GREEK CAPITAL LETTER TAU}"  # windows-1253:D4
        "\N{GREEK CAPITAL LETTER UPSILON}"  # windows-1253:D5
        "\N{GREEK CAPITAL LETTER PHI}"  # windows-1253:D6
        "\N{GREEK CAPITAL LETTER CHI}"  # windows-1253:D7
        "\N{GREEK CAPITAL LETTER PSI}"  # windows-1253:D8
        "\N{GREEK CAPITAL LETTER OMEGA}"  # windows-1253:D9
        "\N{GREEK CAPITAL LETTER IOTA WITH DIALYTIKA}"  # windows-1253:DA
        "\N{GREEK CAPITAL LETTER UPSILON WITH DIALYTIKA}"  # windows-1253:DB
        "\N{GREEK SMALL LETTER ALPHA WITH TONOS}"  # windows-1253:DC
        "\N{GREEK SMALL LETTER EPSILON WITH TONOS}"  # windows-1253:DD
        "\N{GREEK SMALL LETTER ETA WITH TONOS}"  # windows-1253:DE
        "\N{GREEK SMALL LETTER IOTA WITH TONOS}"  # windows-1253:DF
        "\N{CYRILLIC CAPITAL LETTER VE}"  # windows-1251:C2
        "\N{CYRILLIC CAPITAL LETTER GHE}"  # windows-1251:C3
        "\N{CYRILLIC CAPITAL LETTER DE}"  # windows-1251:C4
        "\N{CYRILLIC CAPITAL LETTER IE}"  # windows-1251:C5
        "\N{CYRILLIC CAPITAL LETTER ZHE}"  # windows-1251:C6
        "\N{CYRILLIC CAPITAL LETTER ZE}"  # windows-1251:C7
        "\N{CYRILLIC CAPITAL LETTER I}"  # windows-1251:C8
        "\N{CYRILLIC CAPITAL LETTER SHORT I}"  # windows-1251:C9
        "\N{CYRILLIC CAPITAL LETTER KA}"  # windows-1251:CA
        "\N{CYRILLIC CAPITAL LETTER EL}"  # windows-1251:CB
        "\N{CYRILLIC CAPITAL LETTER EM}"  # windows-1251:CC
        "\N{CYRILLIC CAPITAL LETTER EN}"  # windows-1251:CD
        "\N{CYRILLIC CAPITAL LETTER O}"  # windows-1251:CE
        "\N{CYRILLIC CAPITAL LETTER PE}"  # windows-1251:CF
        "\N{CYRILLIC CAPITAL LETTER ER}"  # windows-1251:D0
        "\N{CYRILLIC CAPITAL LETTER ES}"  # windows-1251:D1
        "\N{CYRILLIC CAPITAL LETTER TE}"  # windows-1251:D2
        "\N{CYRILLIC CAPITAL LETTER U}"  # windows-1251:D3
        "\N{CYRILLIC CAPITAL LETTER EF}"  # windows-1251:D4
        "\N{CYRILLIC CAPITAL LETTER HA}"  # windows-1251:D5
        "\N{CYRILLIC CAPITAL LETTER TSE}"  # windows-1251:D6
        "\N{CYRILLIC CAPITAL LETTER CHE}"  # windows-1251:D7
        "\N{CYRILLIC CAPITAL LETTER SHA}"  # windows-1251:D8
        "\N{CYRILLIC CAPITAL LETTER SHCHA}"  # windows-1251:D9
        "\N{CYRILLIC CAPITAL LETTER HARD SIGN}"  # windows-1251:DA
        "\N{CYRILLIC CAPITAL LETTER YERU}"  # windows-1251:DB
        "\N{CYRILLIC CAPITAL LETTER SOFT SIGN}"  # windows-1251:DC
        "\N{CYRILLIC CAPITAL LETTER E}"  # windows-1251:DD
        "\N{CYRILLIC CAPITAL LETTER YU}"  # windows-1251:DE
        "\N{CYRILLIC CAPITAL LETTER YA}"  # windows-1251:DF
    ),
    # Letters that decode to 0xE0 - 0xEF in a Latin-1-like encoding
    "utf8_first_of_3": (
        "\N{LATIN SMALL LETTER A WITH ACUTE}"  # latin-1:E1
        "\N{LATIN SMALL LETTER A WITH BREVE}"  # windows-1250:E3
        "\N{LATIN SMALL LETTER A WITH CIRCUMFLEX}"  # latin-1:E2
        "\N{LATIN SMALL LETTER A WITH DIAERESIS}"  # latin-1:E4
        "\N{LATIN SMALL LETTER A WITH GRAVE}"  # latin-1:E0
        "\N{LATIN SMALL LETTER A WITH MACRON}"  # windows-1257:E2
        "\N{LATIN SMALL LETTER A WITH OGONEK}"  # windows-1257:E0
        "\N{LATIN SMALL LETTER A WITH RING ABOVE}"  # latin-1:E5
        "\N{LATIN SMALL LETTER A WITH TILDE}"  # latin-1:E3
        "\N{LATIN SMALL LETTER AE}"  # latin-1:E6
        "\N{LATIN SMALL LETTER C WITH ACUTE}"  # windows-1250:E6
        "\N{LATIN SMALL LETTER C WITH CARON}"  # windows-1250:E8
        "\N{LATIN SMALL LETTER C WITH CEDILLA}"  # latin-1:E7
        "\N{LATIN SMALL LETTER D WITH CARON}"  # windows-1250:EF
        "\N{LATIN SMALL LETTER E WITH ACUTE}"  # latin-1:E9
        "\N{LATIN SMALL LETTER E WITH CARON}"  # windows-1250:EC
        "\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}"  # latin-1:EA
        "\N{LATIN SMALL LETTER E WITH DIAERESIS}"  # latin-1:EB
        "\N{LATIN SMALL LETTER E WITH DOT ABOVE}"  # windows-1257:EB
        "\N{LATIN SMALL LETTER E WITH GRAVE}"  # latin-1:E8
        "\N{LATIN SMALL LETTER E WITH MACRON}"  # windows-1257:E7
        "\N{LATIN SMALL LETTER E WITH OGONEK}"  # windows-1250:EA
        "\N{LATIN SMALL LETTER E WITH OGONEK}"  # windows-1250:EA
        "\N{LATIN SMALL LETTER G WITH CEDILLA}"  # windows-1257:EC
        "\N{LATIN SMALL LETTER I WITH ACUTE}"  # latin-1:ED
        "\N{LATIN SMALL LETTER I WITH CIRCUMFLEX}"  # latin-1:EE
        "\N{LATIN SMALL LETTER I WITH DIAERESIS}"  # latin-1:EF
        "\N{LATIN SMALL LETTER I WITH GRAVE}"  # latin-1:EC
        "\N{LATIN SMALL LETTER I WITH MACRON}"  # windows-1257:EE
        "\N{LATIN SMALL LETTER I WITH OGONEK}"  # windows-1257:E1
        "\N{LATIN SMALL LETTER K WITH CEDILLA}"  # windows-1257:ED
        "\N{LATIN SMALL LETTER L WITH ACUTE}"  # windows-1250:E5
        "\N{LATIN SMALL LETTER L WITH CEDILLA}"  # windows-1257:EF
        "\N{LATIN SMALL LETTER R WITH ACUTE}"  # windows-1250:E0
        "\N{LATIN SMALL LETTER Z WITH ACUTE}"  # windows-1257:EA
        "\N{GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND TONOS}"  # windows-1253:E0
        "\N{GREEK SMALL LETTER ALPHA}"  # windows-1253:E1
        "\N{GREEK SMALL LETTER BETA}"  # windows-1253:E2
        "\N{GREEK SMALL LETTER GAMMA}"  # windows-1253:E3
        "\N{GREEK SMALL LETTER DELTA}"  # windows-1253:E4
        "\N{GREEK SMALL LETTER EPSILON}"  # windows-1253:E5
        "\N{GREEK SMALL LETTER ZETA}"  # windows-1253:E6
        "\N{GREEK SMALL LETTER ETA}"  # windows-1253:E7
        "\N{GREEK SMALL LETTER THETA}"  # windows-1253:E8
        "\N{GREEK SMALL LETTER IOTA}"  # windows-1253:E9
        "\N{GREEK SMALL LETTER KAPPA}"  # windows-1253:EA
        "\N{GREEK SMALL LETTER LAMDA}"  # windows-1253:EB
        "\N{GREEK SMALL LETTER MU}"  # windows-1253:EC
        "\N{GREEK SMALL LETTER NU}"  # windows-1253:ED
        "\N{GREEK SMALL LETTER XI}"  # windows-1253:EE
        "\N{GREEK SMALL LETTER OMICRON}"  # windows-1253:EF
        "\N{CYRILLIC SMALL LETTER A}"  # windows-1251:E0
        "\N{CYRILLIC SMALL LETTER BE}"  # windows-1251:E1
        "\N{CYRILLIC SMALL LETTER VE}"  # windows-1251:E2
        "\N{CYRILLIC SMALL LETTER GHE}"  # windows-1251:E3
        "\N{CYRILLIC SMALL LETTER DE}"  # windows-1251:E4
        "\N{CYRILLIC SMALL LETTER IE}"  # windows-1251:E5
        "\N{CYRILLIC SMALL LETTER ZHE}"  # windows-1251:E6
        "\N{CYRILLIC SMALL LETTER ZE}"  # windows-1251:E7
        "\N{CYRILLIC SMALL LETTER I}"  # windows-1251:E8
        "\N{CYRILLIC SMALL LETTER SHORT I}"  # windows-1251:E9
        "\N{CYRILLIC SMALL LETTER KA}"  # windows-1251:EA
        "\N{CYRILLIC SMALL LETTER EL}"  # windows-1251:EB
        "\N{CYRILLIC SMALL LETTER EM}"  # windows-1251:EC
        "\N{CYRILLIC SMALL LETTER EN}"  # windows-1251:ED
        "\N{CYRILLIC SMALL LETTER O}"  # windows-1251:EE
        "\N{CYRILLIC SMALL LETTER PE}"  # windows-1251:EF
    ),
    # Letters that decode to 0xF0 or 0xF3 in a Latin-1-like encoding.
    # (Other leading bytes correspond only to unassigned codepoints)
    "utf8_first_of_4": (
        "\N{LATIN SMALL LETTER D WITH STROKE}"  # windows-1250:F0
        "\N{LATIN SMALL LETTER ETH}"  # latin-1:F0
        "\N{LATIN SMALL LETTER G WITH BREVE}"  # windows-1254:F0
        "\N{LATIN SMALL LETTER O WITH ACUTE}"  # latin-1:F3
        "\N{LATIN SMALL LETTER S WITH CARON}"  # windows-1257:F0
        "\N{GREEK SMALL LETTER PI}"  # windows-1253:F0
        "\N{GREEK SMALL LETTER SIGMA}"  # windows-1253:F3
        "\N{CYRILLIC SMALL LETTER ER}"  # windows-1251:F0
        "\N{CYRILLIC SMALL LETTER U}"  # windows-1251:F3
    ),
    # Letters that decode to 0x80 - 0xBF in a Latin-1-like encoding,
    # including a space standing in for 0xA0
    "utf8_continuation": (
        "\x80-\xbf"
        "\N{SPACE}"  # modification of latin-1:A0, NO-BREAK SPACE
        "\N{LATIN CAPITAL LETTER A WITH OGONEK}"  # windows-1250:A5
        "\N{LATIN CAPITAL LETTER AE}"  # windows-1257:AF
        "\N{LATIN CAPITAL LETTER L WITH CARON}"  # windows-1250:BC
        "\N{LATIN CAPITAL LETTER L WITH STROKE}"  # windows-1250:A3
        "\N{LATIN CAPITAL LETTER O WITH STROKE}"  # windows-1257:A8
        "\N{LATIN CAPITAL LETTER R WITH CEDILLA}"  # windows-1257:AA
        "\N{LATIN CAPITAL LETTER S WITH ACUTE}"  # windows-1250:8C
        "\N{LATIN CAPITAL LETTER S WITH CARON}"  # windows-1252:8A
        "\N{LATIN CAPITAL LETTER S WITH CEDILLA}"  # windows-1250:AA
        "\N{LATIN CAPITAL LETTER T WITH CARON}"  # windows-1250:8D
        "\N{LATIN CAPITAL LETTER Y WITH DIAERESIS}"  # windows-1252:9F
        "\N{LATIN CAPITAL LETTER Z WITH ACUTE}"  # windows-1250:8F
        "\N{LATIN CAPITAL LETTER Z WITH CARON}"  # windows-1252:8E
        "\N{LATIN CAPITAL LETTER Z WITH DOT ABOVE}"  # windows-1250:AF
        "\N{LATIN CAPITAL LIGATURE OE}"  # windows-1252:8C
        "\N{LATIN SMALL LETTER A WITH OGONEK}"  # windows-1250:B9
        "\N{LATIN SMALL LETTER AE}"  # windows-1257:BF
        "\N{LATIN SMALL LETTER F WITH HOOK}"  # windows-1252:83
        "\N{LATIN SMALL LETTER L WITH CARON}"  # windows-1250:BE
        "\N{LATIN SMALL LETTER L WITH STROKE}"  # windows-1250:B3
        "\N{LATIN SMALL LETTER O WITH STROKE}"  # windows-1257:B8
        "\N{LATIN SMALL LETTER R WITH CEDILLA}"  # windows-1257:BA
        "\N{LATIN SMALL LETTER S WITH ACUTE}"  # windows-1250:9C
        "\N{LATIN SMALL LETTER S WITH CARON}"  # windows-1252:9A
        "\N{LATIN SMALL LETTER S WITH CEDILLA}"  # windows-1250:BA
        "\N{LATIN SMALL LETTER T WITH CARON}"  # windows-1250:9D
        "\N{LATIN SMALL LETTER Z WITH ACUTE}"  # windows-1250:9F
        "\N{LATIN SMALL LETTER Z WITH CARON}"  # windows-1252:9E
        "\N{LATIN SMALL LETTER Z WITH DOT ABOVE}"  # windows-1250:BF
        "\N{LATIN SMALL LIGATURE OE}"  # windows-1252:9C
        "\N{MODIFIER LETTER CIRCUMFLEX ACCENT}"  # windows-1252:88
        "\N{CARON}"  # windows-1250:A1
        "\N{BREVE}"  # windows-1250:A2
        "\N{OGONEK}"  # windows-1250:B2
        "\N{SMALL TILDE}"  # windows-1252:98
        "\N{DOUBLE ACUTE ACCENT}"  # windows-1250:BD
        "\N{GREEK TONOS}"  # windows-1253:B4
        "\N{GREEK DIALYTIKA TONOS}"  # windows-1253:A1
        "\N{GREEK CAPITAL LETTER ALPHA WITH TONOS}"  # windows-1253:A2
        "\N{GREEK CAPITAL LETTER EPSILON WITH TONOS}"  # windows-1253:B8
        "\N{GREEK CAPITAL LETTER ETA WITH TONOS}"  # windows-1253:B9
        "\N{GREEK CAPITAL LETTER IOTA WITH TONOS}"  # windows-1253:BA
        "\N{GREEK CAPITAL LETTER OMICRON WITH TONOS}"  # windows-1253:BC
        "\N{GREEK CAPITAL LETTER UPSILON WITH TONOS}"  # windows-1253:BE
        "\N{GREEK CAPITAL LETTER OMEGA WITH TONOS}"  # windows-1253:BF
        "\N{CYRILLIC CAPITAL LETTER IO}"  # windows-1251:A8
        "\N{CYRILLIC CAPITAL LETTER DJE}"  # windows-1251:80
        "\N{CYRILLIC CAPITAL LETTER GJE}"  # windows-1251:81
        "\N{CYRILLIC CAPITAL LETTER UKRAINIAN IE}"  # windows-1251:AA
        "\N{CYRILLIC CAPITAL LETTER DZE}"  # windows-1251:BD
        "\N{CYRILLIC CAPITAL LETTER BYELORUSSIAN-UKRAINIAN I}"  # windows-1251:B2
        "\N{CYRILLIC CAPITAL LETTER YI}"  # windows-1251:AF
        "\N{CYRILLIC CAPITAL LETTER JE}"  # windows-1251:A3
        "\N{CYRILLIC CAPITAL LETTER LJE}"  # windows-1251:8A
        "\N{CYRILLIC CAPITAL LETTER NJE}"  # windows-1251:8C
        "\N{CYRILLIC CAPITAL LETTER TSHE}"  # windows-1251:8E
        "\N{CYRILLIC CAPITAL LETTER KJE}"  # windows-1251:8D
        "\N{CYRILLIC CAPITAL LETTER SHORT U}"  # windows-1251:A1
        "\N{CYRILLIC CAPITAL LETTER DZHE}"  # windows-1251:8F
        "\N{CYRILLIC SMALL LETTER IO}"  # windows-1251:B8
        "\N{CYRILLIC SMALL LETTER DJE}"  # windows-1251:90
        "\N{CYRILLIC SMALL LETTER GJE}"  # windows-1251:83
        "\N{CYRILLIC SMALL LETTER UKRAINIAN IE}"  # windows-1251:BA
        "\N{CYRILLIC SMALL LETTER DZE}"  # windows-1251:BE
        "\N{CYRILLIC SMALL LETTER BYELORUSSIAN-UKRAINIAN I}"  # windows-1251:B3
        "\N{CYRILLIC SMALL LETTER YI}"  # windows-1251:BF
        "\N{CYRILLIC SMALL LETTER JE}"  # windows-1251:BC
        "\N{CYRILLIC SMALL LETTER LJE}"  # windows-1251:9A
        "\N{CYRILLIC SMALL LETTER NJE}"  # windows-1251:9C
        "\N{CYRILLIC SMALL LETTER TSHE}"  # windows-1251:9E
        "\N{CYRILLIC SMALL LETTER KJE}"  # windows-1251:9D
        "\N{CYRILLIC SMALL LETTER SHORT U}"  # windows-1251:A2
        "\N{CYRILLIC SMALL LETTER DZHE}"  # windows-1251:9F
        "\N{CYRILLIC CAPITAL LETTER GHE WITH UPTURN}"  # windows-1251:A5
        "\N{CYRILLIC SMALL LETTER GHE WITH UPTURN}"  # windows-1251:B4
        "\N{EN DASH}"  # windows-1252:96
        "\N{EM DASH}"  # windows-1252:97
        "\N{HORIZONTAL BAR}"  # windows-1253:AF
        "\N{LEFT SINGLE QUOTATION MARK}"  # windows-1252:91
        "\N{RIGHT SINGLE QUOTATION MARK}"  # windows-1252:92
        "\N{SINGLE LOW-9 QUOTATION MARK}"  # windows-1252:82
        "\N{LEFT DOUBLE QUOTATION MARK}"  # windows-1252:93
        "\N{RIGHT DOUBLE QUOTATION MARK}"  # windows-1252:94
        "\N{DOUBLE LOW-9 QUOTATION MARK}"  # windows-1252:84
        "\N{DAGGER}"  # windows-1252:86
        "\N{DOUBLE DAGGER}"  # windows-1252:87
        "\N{BULLET}"  # windows-1252:95
        "\N{HORIZONTAL ELLIPSIS}"  # windows-1252:85
        "\N{PER MILLE SIGN}"  # windows-1252:89
        "\N{SINGLE LEFT-POINTING ANGLE QUOTATION MARK}"  # windows-1252:8B
        "\N{SINGLE RIGHT-POINTING ANGLE QUOTATION MARK}"  # windows-1252:9B
        "\N{EURO SIGN}"  # windows-1252:80
        "\N{NUMERO SIGN}"  # windows-1251:B9
        "\N{TRADE MARK SIGN}"  # windows-1252:99
    ),
    # Letters that decode to 0x80 - 0xBF in a Latin-1-like encoding,
    # and don't usually stand for themselves when adjacent to mojibake.
    # This excludes spaces, dashes, 'bullet', quotation marks, and ellipses.
    "utf8_continuation_strict": (
        "\x80-\xbf"
        "\N{LATIN CAPITAL LETTER A WITH OGONEK}"  # windows-1250:A5
        "\N{LATIN CAPITAL LETTER AE}"  # windows-1257:AF
        "\N{LATIN CAPITAL LETTER L WITH CARON}"  # windows-1250:BC
        "\N{LATIN CAPITAL LETTER L WITH STROKE}"  # windows-1250:A3
        "\N{LATIN CAPITAL LETTER O WITH STROKE}"  # windows-1257:A8
        "\N{LATIN CAPITAL LETTER R WITH CEDILLA}"  # windows-1257:AA
        "\N{LATIN CAPITAL LETTER S WITH ACUTE}"  # windows-1250:8C
        "\N{LATIN CAPITAL LETTER S WITH CARON}"  # windows-1252:8A
        "\N{LATIN CAPITAL LETTER S WITH CEDILLA}"  # windows-1250:AA
        "\N{LATIN CAPITAL LETTER T WITH CARON}"  # windows-1250:8D
        "\N{LATIN CAPITAL LETTER Y WITH DIAERESIS}"  # windows-1252:9F
        "\N{LATIN CAPITAL LETTER Z WITH ACUTE}"  # windows-1250:8F
        "\N{LATIN CAPITAL LETTER Z WITH CARON}"  # windows-1252:8E
        "\N{LATIN CAPITAL LETTER Z WITH DOT ABOVE}"  # windows-1250:AF
        "\N{LATIN CAPITAL LIGATURE OE}"  # windows-1252:8C
        "\N{LATIN SMALL LETTER A WITH OGONEK}"  # windows-1250:B9
        "\N{LATIN SMALL LETTER AE}"  # windows-1257:BF
        "\N{LATIN SMALL LETTER F WITH HOOK}"  # windows-1252:83
        "\N{LATIN SMALL LETTER L WITH CARON}"  # windows-1250:BE
        "\N{LATIN SMALL LETTER L WITH STROKE}"  # windows-1250:B3
        "\N{LATIN SMALL LETTER O WITH STROKE}"  # windows-1257:B8
        "\N{LATIN SMALL LETTER R WITH CEDILLA}"  # windows-1257:BA
        "\N{LATIN SMALL LETTER S WITH ACUTE}"  # windows-1250:9C
        "\N{LATIN SMALL LETTER S WITH CARON}"  # windows-1252:9A
        "\N{LATIN SMALL LETTER S WITH CEDILLA}"  # windows-1250:BA
        "\N{LATIN SMALL LETTER T WITH CARON}"  # windows-1250:9D
        "\N{LATIN SMALL LETTER Z WITH ACUTE}"  # windows-1250:9F
        "\N{LATIN SMALL LETTER Z WITH CARON}"  # windows-1252:9E
        "\N{LATIN SMALL LETTER Z WITH DOT ABOVE}"  # windows-1250:BF
        "\N{LATIN SMALL LIGATURE OE}"  # windows-1252:9C
        "\N{MODIFIER LETTER CIRCUMFLEX ACCENT}"  # windows-1252:88
        "\N{CARON}"  # windows-1250:A1
        "\N{BREVE}"  # windows-1250:A2
        "\N{OGONEK}"  # windows-1250:B2
        "\N{SMALL TILDE}"  # windows-1252:98
        "\N{DOUBLE ACUTE ACCENT}"  # windows-1250:BD
        "\N{GREEK TONOS}"  # windows-1253:B4
        "\N{GREEK DIALYTIKA TONOS}"  # windows-1253:A1
        "\N{GREEK CAPITAL LETTER ALPHA WITH TONOS}"  # windows-1253:A2
        "\N{GREEK CAPITAL LETTER EPSILON WITH TONOS}"  # windows-1253:B8
        "\N{GREEK CAPITAL LETTER ETA WITH TONOS}"  # windows-1253:B9
        "\N{GREEK CAPITAL LETTER IOTA WITH TONOS}"  # windows-1253:BA
        "\N{GREEK CAPITAL LETTER OMICRON WITH TONOS}"  # windows-1253:BC
        "\N{GREEK CAPITAL LETTER UPSILON WITH TONOS}"  # windows-1253:BE
        "\N{GREEK CAPITAL LETTER OMEGA WITH TONOS}"  # windows-1253:BF
        "\N{CYRILLIC CAPITAL LETTER IO}"  # windows-1251:A8
        "\N{CYRILLIC CAPITAL LETTER DJE}"  # windows-1251:80
        "\N{CYRILLIC CAPITAL LETTER GJE}"  # windows-1251:81
        "\N{CYRILLIC CAPITAL LETTER UKRAINIAN IE}"  # windows-1251:AA
        "\N{CYRILLIC CAPITAL LETTER DZE}"  # windows-1251:BD
        "\N{CYRILLIC CAPITAL LETTER BYELORUSSIAN-UKRAINIAN I}"  # windows-1251:B2
        "\N{CYRILLIC CAPITAL LETTER YI}"  # windows-1251:AF
        "\N{CYRILLIC CAPITAL LETTER JE}"  # windows-1251:A3
        "\N{CYRILLIC CAPITAL LETTER LJE}"  # windows-1251:8A
        "\N{CYRILLIC CAPITAL LETTER NJE}"  # windows-1251:8C
        "\N{CYRILLIC CAPITAL LETTER TSHE}"  # windows-1251:8E
        "\N{CYRILLIC CAPITAL LETTER KJE}"  # windows-1251:8D
        "\N{CYRILLIC CAPITAL LETTER SHORT U}"  # windows-1251:A1
        "\N{CYRILLIC CAPITAL LETTER DZHE}"  # windows-1251:8F
        "\N{CYRILLIC SMALL LETTER IO}"  # windows-1251:B8
        "\N{CYRILLIC SMALL LETTER DJE}"  # windows-1251:90
        "\N{CYRILLIC SMALL LETTER GJE}"  # windows-1251:83
        "\N{CYRILLIC SMALL LETTER UKRAINIAN IE}"  # windows-1251:BA
        "\N{CYRILLIC SMALL LETTER DZE}"  # windows-1251:BE
        "\N{CYRILLIC SMALL LETTER BYELORUSSIAN-UKRAINIAN I}"  # windows-1251:B3
        "\N{CYRILLIC SMALL LETTER YI}"  # windows-1251:BF
        "\N{CYRILLIC SMALL LETTER JE}"  # windows-1251:BC
        "\N{CYRILLIC SMALL LETTER LJE}"  # windows-1251:9A
        "\N{CYRILLIC SMALL LETTER NJE}"  # windows-1251:9C
        "\N{CYRILLIC SMALL LETTER TSHE}"  # windows-1251:9E
        "\N{CYRILLIC SMALL LETTER KJE}"  # windows-1251:9D
        "\N{CYRILLIC SMALL LETTER SHORT U}"  # windows-1251:A2
        "\N{CYRILLIC SMALL LETTER DZHE}"  # windows-1251:9F
        "\N{CYRILLIC CAPITAL LETTER GHE WITH UPTURN}"  # windows-1251:A5
        "\N{CYRILLIC SMALL LETTER GHE WITH UPTURN}"  # windows-1251:B4
        "\N{DAGGER}"  # windows-1252:86
        "\N{DOUBLE DAGGER}"  # windows-1252:87
        "\N{PER MILLE SIGN}"  # windows-1252:89
        "\N{SINGLE LEFT-POINTING ANGLE QUOTATION MARK}"  # windows-1252:8B
        "\N{SINGLE RIGHT-POINTING ANGLE QUOTATION MARK}"  # windows-1252:9B
        "\N{EURO SIGN}"  # windows-1252:80
        "\N{NUMERO SIGN}"  # windows-1251:B9
        "\N{TRADE MARK SIGN}"  # windows-1252:99
    ),
}

# This regex uses UTF8_CLUES to find sequences of likely mojibake.
# It matches them with + so that several adjacent UTF-8-looking sequences
# get coalesced into one, allowing them to be fixed more efficiently
# and not requiring every individual subsequence to be detected as 'badness'.
#
# We accept spaces in place of "utf8_continuation", because spaces might have
# been intended to be U+A0 NO-BREAK SPACE.
#
# We do a lookbehind to make sure the previous character isn't a
# "utf8_continuation_strict" character, so that we don't fix just a few
# characters in a huge garble and make the situation worse.
#
# Unfortunately, the matches to this regular expression won't show their
# surrounding context, and including context would make the expression much
# less efficient. The 'badness' rules that require context, such as a preceding
# lowercase letter, will prevent some cases of inconsistent UTF-8 from being
# fixed when they don't see it.
UTF8_DETECTOR_RE = re.compile(
    """
    (?<! [{utf8_continuation_strict}])
    (
        [{utf8_first_of_2}] [{utf8_continuation}]
        |
        [{utf8_first_of_3}] [{utf8_continuation}]{{2}}
        |
        [{utf8_first_of_4}] [{utf8_continuation}]{{3}}
    )+
    """.format(**UTF8_CLUES),
    re.VERBOSE,
)
