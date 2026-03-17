"""
The `ftfy.fixes` module contains the individual fixes that :func:`ftfy.fix_text`
can perform, and provides the functions that are named in "explanations"
such as the output of :func:`ftfy.fix_and_explain`.

Two of these functions are particularly useful on their own, as more robust
versions of functions in the Python standard library:

- :func:`ftfy.fixes.decode_escapes`
- :func:`ftfy.fixes.unescape_html`
"""

import codecs
import html
import re
import warnings
from re import Match
from typing import Any

import ftfy
from ftfy.badness import is_bad
from ftfy.chardata import (
    ALTERED_UTF8_RE,
    C1_CONTROL_RE,
    CONTROL_CHARS,
    DOUBLE_QUOTE_RE,
    HTML_ENTITIES,
    HTML_ENTITY_RE,
    LIGATURES,
    LOSSY_UTF8_RE,
    SINGLE_QUOTE_RE,
    UTF8_DETECTOR_RE,
    WIDTH_MAP,
)


def fix_encoding_and_explain(text: str) -> Any:
    """
    Deprecated copy of `ftfy.fix_encoding_and_explain()`.
    """
    warnings.warn(
        "`fix_encoding_and_explain()` has moved to the main module of ftfy.",
        DeprecationWarning,
        stacklevel=2,
    )
    return ftfy.fix_encoding_and_explain(text)


def fix_encoding(text: str) -> str:
    """
    Deprecated copy of `ftfy.fix_encoding()`.
    """
    warnings.warn(
        "`fix_encoding()` has moved to the main module of ftfy.",
        DeprecationWarning,
        stacklevel=2,
    )
    return ftfy.fix_encoding(text)


def apply_plan(text: str, plan: list[tuple[str, str]]) -> str:
    """
    Deprecated copy of `ftfy.apply_plan()`.
    """
    warnings.warn(
        "`apply_plan()` has moved to the main module of ftfy.",
        DeprecationWarning,
        stacklevel=2,
    )
    return ftfy.apply_plan(text, plan)


def _unescape_fixup(match: Match[str]) -> str:
    """
    Replace one matched HTML entity with the character it represents,
    if possible.
    """
    text = match.group(0)
    if text in HTML_ENTITIES:
        return HTML_ENTITIES[text]
    elif text.startswith("&#"):
        unescaped: str = html.unescape(text)

        # If html.unescape only decoded part of the string, that's not what
        # we want. The semicolon should be consumed.
        if ";" in unescaped:
            return text
        else:
            return unescaped
    else:
        return text


def unescape_html(text: str) -> str:
    """
    Decode HTML entities and character references, including some nonstandard
    ones written in all-caps.

    Python has a built-in called `html.unescape` that can decode HTML escapes,
    including a bunch of messy edge cases such as decoding escapes without
    semicolons such as "&amp".

    If you know you've got HTML-escaped text, applying `html.unescape` is the
    right way to convert it to plain text. But in ambiguous situations, that
    would create false positives. For example, the informally written text
    "this&not that" should not automatically be decoded as "this¬ that".

    In this function, we decode the escape sequences that appear in the
    `html.entities.html5` dictionary, as long as they are the unambiguous ones
    that end in semicolons.

    We also decode all-caps versions of Latin letters and common symbols.
    If a database contains the name 'P&EACUTE;REZ', we can read that and intuit
    that it was supposed to say 'PÉREZ'. This is limited to a smaller set of
    entities, because there are many instances where entity names are
    case-sensitive in complicated ways.

        >>> unescape_html('&lt;tag&gt;')
        '<tag>'

        >>> unescape_html('&Jscr;ohn &HilbertSpace;ancock')
        '𝒥ohn ℋancock'

        >>> unescape_html('&checkmark;')
        '✓'

        >>> unescape_html('P&eacute;rez')
        'Pérez'

        >>> unescape_html('P&EACUTE;REZ')
        'PÉREZ'

        >>> unescape_html('BUNDESSTRA&SZLIG;E')
        'BUNDESSTRASSE'

        >>> unescape_html('&ntilde; &Ntilde; &NTILDE; &nTILDE;')
        'ñ Ñ Ñ &nTILDE;'
    """
    return HTML_ENTITY_RE.sub(_unescape_fixup, text)


ANSI_RE = re.compile("\033\\[((?:\\d|;)*)([a-zA-Z])")


def remove_terminal_escapes(text: str) -> str:
    r"""
    Strip out "ANSI" terminal escape sequences, such as those that produce
    colored text on Unix.

        >>> print(remove_terminal_escapes(
        ...     "\033[36;44mI'm blue, da ba dee da ba doo...\033[0m"
        ... ))
        I'm blue, da ba dee da ba doo...
    """
    return ANSI_RE.sub("", text)


def uncurl_quotes(text: str) -> str:
    r"""
    Replace curly quotation marks with straight equivalents.

        >>> print(uncurl_quotes('\u201chere\u2019s a test\u201d'))
        "here's a test"
    """
    return SINGLE_QUOTE_RE.sub("'", DOUBLE_QUOTE_RE.sub('"', text))


def fix_latin_ligatures(text: str) -> str:
    """
    Replace single-character ligatures of Latin letters, such as 'ﬁ', with the
    characters that they contain, as in 'fi'. Latin ligatures are usually not
    intended in text strings (though they're lovely in *rendered* text).  If
    you have such a ligature in your string, it is probably a result of a
    copy-and-paste glitch.

    We leave ligatures in other scripts alone to be safe. They may be intended,
    and removing them may lose information. If you want to take apart nearly
    all ligatures, use NFKC normalization.

        >>> print(fix_latin_ligatures("ﬂuﬃeﬆ"))
        fluffiest
    """
    return text.translate(LIGATURES)


def fix_character_width(text: str) -> str:
    """
    The ASCII characters, katakana, and Hangul characters have alternate
    "halfwidth" or "fullwidth" forms that help text line up in a grid.

    If you don't need these width properties, you probably want to replace
    these characters with their standard form, which is what this function
    does.

    Note that this replaces the ideographic space, U+3000, with the ASCII
    space, U+20.

        >>> print(fix_character_width("ＬＯＵＤ　ＮＯＩＳＥＳ"))
        LOUD NOISES
        >>> print(fix_character_width("Ｕﾀｰﾝ"))   # this means "U-turn"
        Uターン
    """
    return text.translate(WIDTH_MAP)


def fix_line_breaks(text: str) -> str:
    r"""
    Convert all line breaks to Unix style.

    This will convert the following sequences into the standard \\n
    line break:

    - CRLF (\\r\\n), used on Windows and in some communication protocols
    - CR (\\r), once used on Mac OS Classic, and now kept alive by misguided
      software such as Microsoft Office for Mac
    - LINE SEPARATOR (\\u2028) and PARAGRAPH SEPARATOR (\\u2029), defined by
      Unicode and used to sow confusion and discord
    - NEXT LINE (\\x85), a C1 control character that is certainly not what you
      meant

    The NEXT LINE character is a bit of an odd case, because it
    usually won't show up if `fix_encoding` is also being run.
    \\x85 is very common mojibake for \\u2026, HORIZONTAL ELLIPSIS.

        >>> print(fix_line_breaks(
        ...     "This string is made of two things:\u2029"
        ...     "1. Unicode\u2028"
        ...     "2. Spite"
        ... ))
        This string is made of two things:
        1. Unicode
        2. Spite

    For further testing and examples, let's define a function to make sure
    we can see the control characters in their escaped form:

        >>> def eprint(text):
        ...     print(text.encode('unicode-escape').decode('ascii'))

        >>> eprint(fix_line_breaks("Content-type: text/plain\r\n\r\nHi."))
        Content-type: text/plain\n\nHi.

        >>> eprint(fix_line_breaks("This is how Microsoft \r trolls Mac users"))
        This is how Microsoft \n trolls Mac users

        >>> eprint(fix_line_breaks("What is this \x85 I don't even"))
        What is this \n I don't even
    """
    return (
        text.replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\u2028", "\n")
        .replace("\u2029", "\n")
        .replace("\u0085", "\n")
    )


SURROGATE_RE = re.compile("[\ud800-\udfff]")
SURROGATE_PAIR_RE = re.compile("[\ud800-\udbff][\udc00-\udfff]")


def convert_surrogate_pair(match: Match[str]) -> str:
    """
    Convert a surrogate pair to the single codepoint it represents.

    This implements the formula described at:
    http://en.wikipedia.org/wiki/Universal_Character_Set_characters#Surrogates
    """
    pair = match.group(0)
    codept = 0x10000 + (ord(pair[0]) - 0xD800) * 0x400 + (ord(pair[1]) - 0xDC00)
    return chr(codept)


def fix_surrogates(text: str) -> str:
    """
    Replace 16-bit surrogate codepoints with the characters they represent
    (when properly paired), or with \ufffd otherwise.

        >>> high_surrogate = chr(0xd83d)
        >>> low_surrogate = chr(0xdca9)
        >>> print(fix_surrogates(high_surrogate + low_surrogate))
        💩
        >>> print(fix_surrogates(low_surrogate + high_surrogate))
        ��

    The above doctest had to be very carefully written, because even putting
    the Unicode escapes of the surrogates in the docstring was causing
    various tools to fail, which I think just goes to show why this fixer is
    necessary.
    """
    if SURROGATE_RE.search(text):
        text = SURROGATE_PAIR_RE.sub(convert_surrogate_pair, text)
        text = SURROGATE_RE.sub("\ufffd", text)
    return text


def remove_control_chars(text: str) -> str:
    """
    Remove various control characters that you probably didn't intend to be in
    your text. Many of these characters appear in the table of "Characters not
    suitable for use with markup" at
    http://www.unicode.org/reports/tr20/tr20-9.html.

    This includes:

    - ASCII control characters, except for the important whitespace characters
      (U+00 to U+08, U+0B, U+0E to U+1F, U+7F)
    - Deprecated Arabic control characters (U+206A to U+206F)
    - Interlinear annotation characters (U+FFF9 to U+FFFB)
    - The Object Replacement Character (U+FFFC)
    - The byte order mark (U+FEFF)

    However, these similar characters are left alone:

    - Control characters that produce whitespace (U+09, U+0A, U+0C, U+0D,
      U+2028, and U+2029)
    - C1 control characters (U+80 to U+9F) -- even though they are basically
      never used intentionally, they are important clues about what mojibake
      has happened
    - Control characters that affect glyph rendering, such as joiners and
      right-to-left marks (U+200C to U+200F, U+202A to U+202E)
    - Musical notation control characters (U+1D173 to U+1D17A) because wow if
      you're using those you probably have a good reason
    - Tag characters, because they are now used in emoji sequences such as
      "Flag of Wales"
    """
    return text.translate(CONTROL_CHARS)


def remove_bom(text: str) -> str:
    r"""
    Remove a byte-order mark that was accidentally decoded as if it were part
    of the text.

    >>> print(remove_bom(chr(0xfeff) + "Where do you want to go today?"))
    Where do you want to go today?
    """
    return text.lstrip(chr(0xFEFF))


# Define a regex to match valid escape sequences in Python string literals.
ESCAPE_SEQUENCE_RE = re.compile(
    r"""
    ( \\U........      # 8-digit hex escapes
    | \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\N\{[^}]+\}     # Unicode characters by name
    | \\[\\'"abfnrtv]  # Single-character escapes
    )""",
    re.UNICODE | re.VERBOSE,
)


def decode_escapes(text: str) -> str:
    r"""
    Decode backslashed escape sequences, including \\x, \\u, and \\U character
    references, even in the presence of other Unicode.

    This function has to be called specifically. It's not run automatically by
    ftfy, because escaped text is not necessarily a mistake, and there is no
    way to distinguish when it is.

    This is what Python's "string-escape" and "unicode-escape" codecs were
    meant to do, but in contrast, this actually works. It will decode the
    string exactly the same way that the Python interpreter decodes its string
    literals.

        >>> factoid = '\\u20a1 is the currency symbol for the colón.'
        >>> print(factoid[1:])
        u20a1 is the currency symbol for the colón.
        >>> print(decode_escapes(factoid))
        ₡ is the currency symbol for the colón.

    Even though Python itself can read string literals with a combination of
    escapes and literal Unicode -- you're looking at one right now -- the
    "unicode-escape" codec doesn't work on literal Unicode. (See
    http://stackoverflow.com/a/24519338/773754 for more details.)

    Instead, this function searches for just the parts of a string that
    represent escape sequences, and decodes them, leaving the rest alone. All
    valid escape sequences are made of ASCII characters, and this allows
    "unicode-escape" to work correctly.
    """

    def decode_match(match: Match[str]) -> str:
        "Given a regex match, decode the escape sequence it contains."
        return codecs.decode(match.group(0), "unicode-escape")

    return ESCAPE_SEQUENCE_RE.sub(decode_match, text)


# This regex implements an exception to restore_byte_a0, so we can decode the
# very common mojibake of (for example) "Ã la mode" as "à la mode", not "àla
# mode".
#
# If byte C3 appears with a single space after it -- most commonly this shows
# up as " Ã " appearing as an entire word -- we'll insert \xa0 while keeping
# the space. Without this change, we would decode "à" as the start of the next
# word, such as "àla". It's almost always intended to be a separate word, as in
# "à la", but when mojibake turns this into "Ã\xa0 la", the two kinds of spaces
# get coalesced into "Ã la".
#
# We make exceptions for the Portuguese words "às", "àquele", "àquela",
# "àquilo" and their plurals -- these are contractions of, for example, "a
# aquele" and are very common. Note that the final letter is important to
# distinguish this case from French "à quel point".
#
# Other instances in Portuguese, such as "àfrica", seem to be typos (intended
# to be "África" with the accent in the other direction).
#
# Unfortunately, "à" is a common letter in Catalan, and mojibake of words that
# contain it will end up with inserted spaces. We can't do the right thing with
# every word. The cost is that the mojibake text "fÃ cil" will be interpreted as
# "fà cil", not "fàcil".
A_GRAVE_WORD_RE = re.compile(b"\xc3 (?! |quele|quela|quilo|s )")


def restore_byte_a0(byts: bytes) -> bytes:
    """
    Some mojibake has been additionally altered by a process that said "hmm,
    byte A0, that's basically a space!" and replaced it with an ASCII space.
    When the A0 is part of a sequence that we intend to decode as UTF-8,
    changing byte A0 to 20 would make it fail to decode.

    This process finds sequences that would convincingly decode as UTF-8 if
    byte 20 were changed to A0, and puts back the A0. For the purpose of
    deciding whether this is a good idea, this step gets a cost of twice
    the number of bytes that are changed.

    This is used as a step within `fix_encoding`.
    """
    byts = A_GRAVE_WORD_RE.sub(b"\xc3\xa0 ", byts)

    def replacement(match: Match[bytes]) -> bytes:
        "The function to apply when this regex matches."
        return match.group(0).replace(b"\x20", b"\xa0")

    return ALTERED_UTF8_RE.sub(replacement, byts)


def replace_lossy_sequences(byts: bytes) -> bytes:
    """
    This function identifies sequences where information has been lost in
    a "sloppy" codec, indicated by byte 1A, and if they would otherwise look
    like a UTF-8 sequence, it replaces them with the UTF-8 sequence for U+FFFD.

    A further explanation:

    ftfy can now fix text in a few cases that it would previously fix
    incompletely, because of the fact that it can't successfully apply the fix
    to the entire string. A very common case of this is when characters have
    been erroneously decoded as windows-1252, but instead of the "sloppy"
    windows-1252 that passes through unassigned bytes, the unassigned bytes get
    turned into U+FFFD (�), so we can't tell what they were.

    This most commonly happens with curly quotation marks that appear
    ``â€œ like this â€�``.

    We can do better by building on ftfy's "sloppy codecs" to let them handle
    less-sloppy but more-lossy text. When they encounter the character ``�``,
    instead of refusing to encode it, they encode it as byte 1A -- an
    ASCII control code called SUBSTITUTE that once was meant for about the same
    purpose. We can then apply a fixer that looks for UTF-8 sequences where
    some continuation bytes have been replaced by byte 1A, and decode the whole
    sequence as �; if that doesn't work, it'll just turn the byte back into �
    itself.

    As a result, the above text ``â€œ like this â€�`` will decode as
    ``“ like this �``.

    If U+1A was actually in the original string, then the sloppy codecs will
    not be used, and this function will not be run, so your weird control
    character will be left alone but wacky fixes like this won't be possible.

    This is used as a transcoder within `fix_encoding`.
    """
    return LOSSY_UTF8_RE.sub("\ufffd".encode(), byts)


def decode_inconsistent_utf8(text: str) -> str:
    """
    Sometimes, text from one encoding ends up embedded within text from a
    different one. This is common enough that we need to be able to fix it.

    This is used as a transcoder within `fix_encoding`.
    """

    def fix_embedded_mojibake(match: Match[str]) -> str:
        substr = match.group(0)

        # Require the match to be shorter, so that this doesn't recurse infinitely
        if len(substr) < len(text) and is_bad(substr):
            return ftfy.fix_encoding(substr)
        else:
            return substr

    return UTF8_DETECTOR_RE.sub(fix_embedded_mojibake, text)


def _c1_fixer(match: Match[str]) -> str:
    return match.group(0).encode("latin-1").decode("sloppy-windows-1252")


def fix_c1_controls(text: str) -> str:
    """
    If text still contains C1 control characters, treat them as their
    Windows-1252 equivalents. This matches what Web browsers do.
    """
    return C1_CONTROL_RE.sub(_c1_fixer, text)
