"""Tests for emoji width measurement and ZWJ sequences."""
# std imports
import os

# 3rd party
import pytest

# local
import wcwidth

# some tests cannot be done on some builds of python, where the internal
# unicode structure is limited to 0x10000 for memory conservation,
# "ValueError: unichr() arg not in range(0x10000) (narrow Python build)"
try:
    chr(0x2fffe)
    NARROW_ONLY = False
except ValueError:
    NARROW_ONLY = True


def make_sequence_from_line(line):
    # convert '002A FE0F  ; ..' -> (0x2a, 0xfe0f) -> chr(0x2a) + chr(0xfe0f)
    return ''.join(chr(int(cp, 16)) for cp in line.split(';', 1)[0].strip().split())


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def emoji_zwj_sequence():
    """Emoji zwj sequence of four codepoints is just 2 cells."""
    phrase = ("\U0001f469"   # Base, Category So, East Asian Width property 'W' -- WOMAN
              "\U0001f3fb"   # Modifier, Category Sk, East Asian Width property 'W' -- EMOJI MODIFIER FITZPATRICK TYPE-1-2
              "\u200d"       # Joiner, Category Cf, East Asian Width property 'N'  -- ZERO WIDTH JOINER
              "\U0001f4bb")  # Fused, Category So, East Asian Width property 'W' -- PERSONAL COMPUTER
    # This test adapted from https://www.unicode.org/L2/L2023/23107-terminal-suppt.pdf
    expect_length_each = (2, 2, 0, 2)
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_unfinished_zwj_sequence():
    """Ensure index-out-of-bounds does not occur for ZWJ without any following character."""
    phrase = ("\U0001f469"   # Base, Category So, East Asian Width property 'W' -- WOMAN
              "\U0001f3fb"   # Modifier, Category Sk, East Asian Width property 'W' -- EMOJI MODIFIER FITZPATRICK TYPE-1-2
              "\u200d")      # Joiner, Category Cf, East Asian Width property 'N'  -- ZERO WIDTH JOINER
    expect_length_each = (2, 2, 0)
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_non_recommended_zwj_sequence():
    """Verify ZWJ with characters that cannot be joined, wcwidth does not verify."""
    phrase = ("\U0001f469"   # Base, Category So, East Asian Width property 'W' -- WOMAN
              "\U0001f3fb"   # Modifier, Category Sk, East Asian Width property 'W' -- EMOJI MODIFIER FITZPATRICK TYPE-1-2
              "\u200d")      # Joiner, Category Cf, East Asian Width property 'N'  -- ZERO WIDTH JOINER
    expect_length_each = (2, 2, 0)
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_another_emoji_zwj_sequence():
    phrase = (
        "\u26F9"        # PERSON WITH BALL
        "\U0001F3FB"    # EMOJI MODIFIER FITZPATRICK TYPE-1-2
        "\u200D"        # ZERO WIDTH JOINER
        "\u2640"        # FEMALE SIGN
        "\uFE0F")       # VARIATION SELECTOR-16
    expect_length_each = (1, 2, 0, 1, 0)
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_longer_emoji_zwj_sequence():
    """
    A much longer emoji ZWJ sequence of 10 total codepoints is just 2 cells!

    Also test the same sequence in duplicate, verifying multiple VS-16 sequences in a single
    function call.
    """
    # 'Category Code', 'East Asian Width property' -- 'description'
    phrase = ("\U0001F9D1"   # 'So', 'W' -- ADULT
              "\U0001F3FB"   # 'Sk', 'W' -- EMOJI MODIFIER FITZPATRICK TYPE-1-2
              "\u200d"       # 'Cf', 'N' -- ZERO WIDTH JOINER
              "\u2764"       # 'So', 'N' -- HEAVY BLACK HEART
              "\uFE0F"       # 'Mn', 'A' -- VARIATION SELECTOR-16
              "\u200d"       # 'Cf', 'N' -- ZERO WIDTH JOINER
              "\U0001F48B"   # 'So', 'W' -- KISS MARK
              "\u200d"       # 'Cf', 'N' -- ZERO WIDTH JOINER
              "\U0001F9D1"   # 'So', 'W' -- ADULT
              "\U0001F3FD"   # 'Sk', 'W' -- EMOJI MODIFIER FITZPATRICK TYPE-4
              ) * 2
    # This test adapted from https://www.unicode.org/L2/L2023/23107-terminal-suppt.pdf
    expect_length_each = (2, 2, 0, 1, 0, 0, 2, 0, 2, 2) * 2
    expect_length_phrase = 4

    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


def read_sequences_from_file(filename):
    import yatest.common as yc
    with open(os.path.join(os.path.dirname(yc.source_path(__file__)), filename), encoding='utf-8') as fp:
        lines = [line.strip()
                 for line in fp.readlines()
                 if not line.startswith('#') and line.strip()]
    sequences = [make_sequence_from_line(line) for line in lines]
    return lines, sequences


@pytest.mark.skipif(NARROW_ONLY, reason="Some sequences in text file are not compatible with 'narrow' builds")
@pytest.mark.skipif(not os.path.exists(os.path.join(os.path.dirname(__file__), 'emoji-zwj-sequences.txt')),
                    reason="emoji-zwj-sequences.txt; missing run bin/update-tables.py")
def test_recommended_emoji_zwj_sequences(benchmark):
    """Test wcswidth of all of the unicode.org-published emoji-zwj-sequences.txt."""
    lines, sequences = read_sequences_from_file('emoji-zwj-sequences.txt')

    def measure_all():
        errors = []
        for sequence, line in zip(sequences, lines):
            measured_width = wcwidth.wcswidth(sequence)
            if measured_width != 2:
                errors.append({
                    'expected_width': 2,
                    'line': line,
                    'measured_width': measured_width,
                    'sequence': sequence,
                })
        return errors

    errors = benchmark(measure_all)
    assert not errors
    assert len(sequences) >= 1468


@pytest.mark.skipif(not os.path.exists(os.path.join(os.path.dirname(__file__), 'emoji-variation-sequences.txt')),
                    reason="emoji-variation-sequences.txt is missing; run bin/update-tables.py")
def test_recommended_variation_16_sequences(benchmark):
    """Test wcswidth of all of the unicode.org-published emoji-variation-sequences.txt."""
    lines, sequences = read_sequences_from_file('emoji-variation-sequences.txt')
    vs16_sequences = [(seq, line) for seq, line in zip(sequences, lines) if '\ufe0f' in seq]

    def measure_all():
        errors = []
        for sequence, line in vs16_sequences:
            measured_width = wcwidth.wcswidth(sequence)
            if measured_width != 2:
                errors.append({
                    'expected_width': 2,
                    'line': line,
                    'measured_width': measured_width,
                    'sequence': sequence,
                })
        return errors

    errors = benchmark(measure_all)
    assert not errors
    assert len(sequences) >= 742


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_regional_indicator_single():
    """Single Regional Indicator symbol is width 2."""
    assert wcwidth.wcwidth('\U0001F1FA') == 2
    assert wcwidth.wcswidth('\U0001F1FA') == 2


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_regional_indicator_pair():
    """Flag pair (two Regional Indicators) is width 2, not 4."""
    assert wcwidth.wcswidth('\U0001F1FA\U0001F1F8') == 2


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_regional_indicator_three():
    """Three Regional Indicators: one pair (2) + one single (2) = 4."""
    assert wcwidth.wcswidth('\U0001F1FA\U0001F1F8\U0001F1E6') == 4


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_regional_indicator_four():
    """Four Regional Indicators: two pairs = 2 + 2 = 4."""
    assert wcwidth.wcswidth(
        '\U0001F1FA\U0001F1F8\U0001F1E6\U0001F1FA') == 4


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_zwj_after_non_emoji():
    """ZWJ after non-emoji unconditionally consumes next character."""
    # This does *not* match most terminal behavior -- it is a negative test,
    # they fail because our library doesn't handle 'glitch' emoji as an
    # optimization. Non-emoji + ZWJ is undefined per Unicode UAX #29 GB11.
    assert wcwidth.wcswidth('xx\u200d\U0001F384') == 2
    assert wcwidth.wcswidth('a\u200d\U0001F600') == 1
    assert wcwidth.wcswidth('\u4e16\u200d\U0001F600') == 2


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_fitzpatrick_standalone():
    """Standalone Fitzpatrick modifier is width 2."""
    assert wcwidth.wcwidth('\U0001F3FB') == 2
    assert wcwidth.wcswidth('\U0001F3FB') == 2


@pytest.mark.skipif(NARROW_ONLY, reason="Test cannot verify on python 'narrow' builds")
def test_fitzpatrick_after_emoji():
    """Fitzpatrick modifier after emoji base combines, total width 2."""
    assert wcwidth.wcswidth('\U0001F469\U0001F3FB') == 2


def test_vs16_effect():
    """Verify effect of VS-16 (always active with latest Unicode version)."""
    phrase = ("\u2640"        # FEMALE SIGN
              "\uFE0F")       # VARIATION SELECTOR-16

    expect_length_each = (1, 0)
    expect_length_phrase = 2

    # exercise,
    length_each = tuple(wcwidth.wcwidth(w_char) for w_char in phrase)
    length_phrase = wcwidth.wcswidth(phrase)

    # verify.
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase
