"""Core tests for wcwidth module."""
# std imports
import sys
import importlib.metadata

# 3rd party
import pytest

# local
import wcwidth

_wcwidth_module = sys.modules['wcwidth.wcwidth']
_WIDTH_FAST_PATH_MIN_LEN = _wcwidth_module._WIDTH_FAST_PATH_MIN_LEN


def test_package_version():
    """wcwidth.__version__ is expected value."""
    # given,
    expected = importlib.metadata.version('wcwidth')

    # exercise,
    result = wcwidth.__version__

    # verify.
    assert result == expected


def test_empty_string():
    """
    Test empty string is OK.

    https://github.com/jquast/wcwidth/issues/24
    """
    phrase = ""
    expect_length_each = 0
    expect_length_phrase = 0

    # exercise,
    length_each = wcwidth.wcwidth(phrase)
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def basic_string_type():
    """
    This is a python 2-specific test of the basic "string type".

    Such strings cannot contain anything but ascii in python2.
    """
    # given,
    phrase = 'hello\x00world'
    expect_length_each = (1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1)
    expect_length_phrase = sum(expect_length_each)

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def test_hello_jp():
    """
    Width of Japanese phrase: コンニチハ, セカイ!

    Given a phrase of 5 and 3 Katakana ideographs, joined with
    3 English-ASCII punctuation characters, totaling 11, this
    phrase consumes 19 cells of a terminal emulator.
    """
    # given,
    phrase = 'コンニチハ, セカイ!'
    expect_length_each = (2, 2, 2, 2, 2, 1, 1, 2, 2, 2, 1)
    expect_length_phrase = sum(expect_length_each)

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def test_wcswidth_substr():
    """
    Test wcswidth() optional 2nd parameter, ``n``.

    ``n`` determines at which position of the string
    to stop counting length.
    """
    # given,
    phrase = 'コンニチハ, セカイ!'
    end = 7
    expect_length_each = (2, 2, 2, 2, 2, 1, 1,)
    expect_length_phrase = sum(expect_length_each)

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))[:end]
    length_phrase = wcwidth.wcswidth(phrase, end)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def test_null_width_0():
    """NULL (0) reports width 0."""
    # given,
    phrase = 'abc\x00def'
    expect_length_each = (1, 1, 1, 0, 1, 1, 1)
    expect_length_phrase = sum(expect_length_each)

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase, len(phrase))

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def test_control_c0_width_negative_1():
    """
    How the API reacts to CSI (Control sequence initiate).

    An example of bad fortune, this terminal sequence is a width of 0 on all terminals, but wcwidth
    doesn't parse Control-Sequence-Inducer (CSI) sequences.

    Also the "legacy" posix functions wcwidth and wcswidth return -1 for any string containing the
    C1 control character \x1b (ESC).
    """
    # given,
    phrase = '\x1b[0m'
    expect_length_each = (-1, 1, 1, 1)
    expect_length_phrase = -1

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify, though this is actually *0* width for a terminal emulator
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def test_combining_width():
    """Simple test combining reports total width of 4."""
    # given,
    phrase = '--\u05bf--'
    expect_length_each = (1, 1, 0, 1, 1)
    expect_length_phrase = 4

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def test_combining_cafe():
    """Phrase cafe + COMBINING ACUTE ACCENT is café of length 4."""
    phrase = "cafe\u0301"
    expect_length_each = (1, 1, 1, 1, 0)
    expect_length_phrase = 4

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def test_combining_enclosing():
    """CYRILLIC CAPITAL LETTER A + COMBINING CYRILLIC HUNDRED THOUSANDS SIGN is of length 1."""
    phrase = "\u0410\u0488"
    expect_length_each = (1, 0)
    expect_length_phrase = 1

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def test_balinese_script():
    """
    Balinese kapal (ship) is length 3.

    This may be an example that is not yet correctly rendered by any terminal so far, like
    devanagari.
    """
    phrase = ("\u1B13"    # Category 'Lo', EAW 'N' -- BALINESE LETTER KA
              "\u1B28"    # Category 'Lo', EAW 'N' -- BALINESE LETTER PA KAPAL
              "\u1B2E"    # Category 'Lo', EAW 'N' -- BALINESE LETTER LA
              "\u1B44")   # Category 'Mc', EAW 'N' -- BALINESE ADEG ADEG
    expect_length_each = (1, 1, 1, 0)
    expect_length_phrase = 4

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase

    # verify width() parse mode also handles Mc correctly
    assert wcwidth.width(phrase) == expect_length_phrase

    # standalone Mc has zero width
    assert wcwidth.wcswidth("\u1B44") == 0
    assert wcwidth.width("\u1B44") == 0


def test_kr_jamo():
    """
    Test basic combining of HANGUL CHOSEONG and JUNGSEONG.

    Example and from Raymond Chen's blog post,
    https://devblogs.microsoft.com/oldnewthing/20201009-00/?p=104351
    """
    # This is an example where both characters are "wide" when displayed alone.
    #
    # But JUNGSEONG (vowel) is designed for combination with a CHOSEONG (consonant).
    #
    # This wcwidth library understands their width only when combination,
    # and not by independent display, like other zero-width characters that may
    # only combine with an appropriate preceding character.
    phrase = (
        "\u1100"  # ᄀ HANGUL CHOSEONG KIYEOK (consonant)
        "\u1161"  # ᅡ HANGUL JUNGSEONG A (vowel)
    )
    expect_length_each = (2, 0)
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def test_kr_jamo_filler():
    """
    Jamo filler is 0 width.

    Example from https://www.unicode.org/L2/L2006/06310-hangul-decompose9.pdf
    """
    phrase = (
        "\u1100"  # HANGUL CHOSEONG KIYEOK (consonant)
        "\u1160"  # HANGUL JUNGSEONG FILLER (vowel)
    )
    expect_length_each = (2, 0)
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def test_devanagari_script():
    """
    Attempt to test the measurement width of Devanagari script.

    I believe this 'phrase' should be length 3.

    This is a difficult problem, and this library does not yet get it right,
    because we interpret the unicode data files programmatically, but they do
    not correctly describe how their terminal width is measured.

    There are very few Terminals that do!

    As of 2023,

    - iTerm2: correct length but individual characters are out of order and
              horizaontally misplaced as to be unreadable in its language when
              using 'Noto Sans' font.
    - mlterm: mixed results, it offers several options in the configuration
              dialog, "Xft", "Cario", and "Variable Column Width" have some
              effect, but with neither 'Noto Sans' or 'unifont', it is not
              recognizable as the Devanagari script it is meant to display.

    Previous testing with Devanagari documented at address https://benizi.com/vim/devanagari/

    See also, https://askubuntu.com/questions/8437/is-there-a-good-mono-spaced-font-for-devanagari-script-in-the-terminal
    """
    # This test adapted from https://www.unicode.org/L2/L2023/23107-terminal-suppt.pdf
    # please note that document correctly points out that the final width cannot be determined
    # as a sum of each individual width, as this library currently performs with exception of
    # ZWJ, but I think it incorrectly gestures what a stateless call to wcwidth.wcwidth of
    # each codepoint *should* return.
    phrase = ("\u0915"    # Akhand, Category 'Lo', East Asian Width property 'N' -- DEVANAGARI LETTER KA
              "\u094D"    # Joiner, Category 'Mn', East Asian Width property 'N' -- DEVANAGARI SIGN VIRAMA
              "\u0937"    # Fused, Category 'Lo', East Asian Width property 'N' -- DEVANAGARI LETTER SSA
              "\u093F")   # MatraL, Category 'Mc', East Asian Width property 'N' -- DEVANAGARI VOWEL SIGN I
    # 23107-terminal-suppt.pdf suggests wcwidth.wcwidth should return (2, 0, 0, 1)
    expect_length_each = (1, 0, 1, 0)
    # virama conjunct collapses KA+virama+SSA into one cell, Mc adds +1
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase
    assert wcwidth.width(phrase) == expect_length_phrase


def test_tamil_script():
    # This test adapted from https://www.unicode.org/L2/L2023/23107-terminal-suppt.pdf
    phrase = ("\u0b95"    # Akhand, Category 'Lo', East Asian Width property 'N' -- TAMIL LETTER KA
              "\u0bcd"    # Joiner, Category 'Mn', East Asian Width property 'N' -- TAMIL SIGN VIRAMA
              "\u0bb7"    # Fused, Category 'Lo', East Asian Width property 'N' -- TAMIL LETTER SSA
              "\u0bcc")   # MatraLR, Category 'Mc', East Asian Width property 'N' -- TAMIL VOWEL SIGN AU
    # 23107-terminal-suppt.pdf suggests wcwidth.wcwidth should return (3, 0, 0, 4)
    expect_length_each = (1, 0, 1, 0)

    # virama conjunct collapses KA+virama+SSA into one cell, Mc adds +1
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase
    assert wcwidth.width(phrase) == expect_length_phrase


def test_kannada_script():
    # This test adapted from https://www.unicode.org/L2/L2023/23107-terminal-suppt.pdf
    # |ರ್ಝೈ|
    # |123|
    phrase = ("\u0cb0"    # Repha, Category 'Lo', East Asian Width property 'N' -- KANNADA LETTER RA
              "\u0ccd"    # Joiner, Category 'Mn', East Asian Width property 'N' -- KANNADA SIGN VIRAMA
              "\u0c9d"    # Base, Category 'Lo', East Asian Width property 'N' -- KANNADA LETTER JHA
              "\u0cc8")   # MatraUR, Category 'Mc', East Asian Width property 'N' -- KANNADA VOWEL SIGN AI
    # 23107-terminal-suppt.pdf suggests should be (2, 0, 3, 1)
    expect_length_each = (1, 0, 1, 0)
    # virama conjunct collapses RA+virama+JHA into one cell, Mc adds +1
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase
    assert wcwidth.width(phrase) == expect_length_phrase


def test_kannada_script_2():
    # This test adapted from https://www.unicode.org/L2/L2023/23107-terminal-suppt.pdf
    # |ರ಼್ಚ|
    # |12|
    phrase = ("\u0cb0"    # Base, Category 'Lo', East Asian Width property 'N' -- KANNADA LETTER RA
              "\u0cbc"    # Nukta, Category 'Mn', East Asian Width property 'N' -- KANNADA SIGN NUKTA
              "\u0ccd"    # Joiner, Category 'Lo', East Asian Width property 'N' -- KANNADA SIGN VIRAMA
              "\u0c9a")   # Subjoin, Category 'Mc', East Asian Width property 'N' -- KANNADA LETTER CA
    # 23107-terminal-suppt.pdf suggests wcwidth.wcwidth should return (2, 0, 0, 1)
    expect_length_each = (1, 0, 0, 1)
    # virama conjunct collapses RA(+Nukta)+virama+CA into one cell
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase
    assert wcwidth.width(phrase) == expect_length_phrase


def test_bengali_nukta_mc():
    # Mc following Mn (Nukta) is still counted as spacing mark.
    #
    # Discovered via UDHR Bengali text where wrap() produced lines exceeding the requested width.
    # The root cause was that width() only recognized a Spacing Combining Mark (Mc) when it was
    # *immediately* adjacent to the base character (index == last_base + 1).
    #
    # In Bengali, a Nukta (U+09BC, category Mn) commonly sits between the consonant and the vowel
    # sign, so the Mc vowel sign was skipped and measured as zero instead of one.
    #
    # The nukta between consonant and vowel sign does not break the combining sequence, so the Mc
    # must still be counted.
    phrase = "\u09AF\u09BC\u09C7"
    assert wcwidth.wcwidth("\u09C7") == 0
    assert wcwidth.wcswidth(phrase) == 2
    assert wcwidth.width(phrase) == 2


@pytest.mark.parametrize("repeat", [1, _WIDTH_FAST_PATH_MIN_LEN])
def test_mc_width_consistency(repeat):
    # width(), wcswidth(), and per-grapheme width sums must all agree.
    #
    # The repeat parameter ensures both the short (parse) and long (fast) code
    # paths of width() are exercised.  At repeat=1 the phrases are short enough
    # to go through character-by-character parse mode.  At repeat=_WIDTH_FAST_PATH_MIN_LEN
    # every phrase exceeds the threshold and takes the fast path that delegates
    # to wcswidth().
    phrases = [
        "\u0915\u094D\u0937\u093F",
        "\u0b95\u0bcd\u0bb7\u0bcc",
        "\u0cb0\u0ccd\u0c9d\u0cc8",
        "\u0cb0\u0cbc\u0ccd\u0c9a",
        "\u09AF\u09BC\u09C7",
        "\u09B9\u09AF\u09BC\u09C7\u099B\u09C7",
        "\u0915\u09BE\u0999\u09CD\u0996\u09BE",
    ]
    # Virama conjunct collapsing is context-sensitive across grapheme
    # boundaries (virama ends one grapheme, consonant starts the next),
    # so per-grapheme width sums may exceed wcswidth/width totals for
    # phrases containing conjuncts.
    no_conjunct_phrases = [
        "\u09AF\u09BC\u09C7",
    ]
    for phrase in phrases:
        text = phrase * repeat
        assert wcwidth.width(text) == wcwidth.wcswidth(text)
    for phrase in no_conjunct_phrases:
        text = phrase * repeat
        grapheme_sum = sum(wcwidth.width(g) for g in wcwidth.iter_graphemes(text))
        assert wcwidth.width(text) == grapheme_sum


@pytest.mark.parametrize("phrase,expected", [
    ("\u0999\u09CD\u0997\u09C7", 2),
    ("\u0915\u094D\u0924\u093F", 2),
    ("\u0915\u094D\u0930\u093F", 2),
    ("\u0A95\u0ACD\u0A95\u0ACB", 2),
    ("\u0938\u094D\u0924\u094D\u0930", 2),
    ("\u0938\u094D\u0924", 2),
    ("\u0915\u094D\u0020", 2),
    ("\u09A4\u09CD\u200D\u09AA", 2),
    ("\u0915\u094D\u200D\u0924", 2),
    ("\u0D15\u0D4D\u0D15\u0D41\u0D02", 2),
    ("\u0915\u094D\u0924\u0941\u0902", 2),
])
def test_virama_conjunct(phrase, expected):
    assert wcwidth.wcswidth(phrase) == expected
    assert wcwidth.width(phrase) == expected


def test_soft_hyphen():
    # Test SOFT HYPHEN, category 'Cf' usually are zero-width, but most
    # implementations agree to draw it was '1' cell, visually
    # indistinguishable from a space, ' ' in Konsole, for example.
    assert wcwidth.wcwidth(chr(0x000ad)) == 1


PREPENDED_CONCATENATION_MARKS = [
    (0x0600, 'ARABIC NUMBER SIGN'),
    (0x0601, 'ARABIC SIGN SANAH'),
    (0x0602, 'ARABIC FOOTNOTE MARKER'),
    (0x0603, 'ARABIC SIGN SAFHA'),
    (0x0604, 'ARABIC SIGN SAMVAT'),
    (0x0605, 'ARABIC NUMBER MARK ABOVE'),
    (0x06DD, 'ARABIC END OF AYAH'),
    (0x070F, 'SYRIAC ABBREVIATION MARK'),
    (0x0890, 'ARABIC POUND MARK ABOVE'),
    (0x0891, 'ARABIC PIASTRE MARK ABOVE'),
    (0x08E2, 'ARABIC DISPUTED END OF AYAH'),
    (0x110BD, 'KAITHI NUMBER SIGN'),
    (0x110CD, 'KAITHI NUMBER SIGN ABOVE'),
]


@pytest.mark.parametrize('codepoint,name', PREPENDED_CONCATENATION_MARKS)
def test_prepended_concatenation_mark_width(codepoint, name):
    """Prepended Concatenation Marks have width 1, not 0."""
    # https://github.com/jquast/wcwidth/issues/119
    assert wcwidth.wcwidth(chr(codepoint)) == 1
