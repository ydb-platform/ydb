"""Tests for grapheme cluster segmentation."""
# std imports
import os

# 3rd party
import pytest

# local
from wcwidth import iter_graphemes

try:
    chr(0x2fffe)
    NARROW_ONLY = False
except ValueError:
    NARROW_ONLY = True


def parse_grapheme_break_test_line(line):
    """Parse a line from GraphemeBreakTest.txt."""
    data, _, _ = line.partition('#')
    data = data.strip()
    if not data:
        return None, None

    parts = []
    current_cluster = []

    for token in data.split():
        if token == '÷':
            if current_cluster:
                parts.append(current_cluster)
                current_cluster = []
        elif token == '×':
            pass
        else:
            try:
                current_cluster.append(int(token, 16))
            except ValueError:
                continue

    if current_cluster:
        parts.append(current_cluster)

    all_codepoints = []
    expected_clusters = []
    for cluster in parts:
        cluster_str = ''.join(chr(cp) for cp in cluster)
        expected_clusters.append(cluster_str)
        all_codepoints.extend(cluster)

    if not all_codepoints:
        return None, None

    input_str = ''.join(chr(cp) for cp in all_codepoints)
    return input_str, expected_clusters


def read_grapheme_break_test():
    """Read and parse GraphemeBreakTest.txt."""
    test_file = os.path.join(os.path.dirname(__file__), 'GraphemeBreakTest.txt')
    if not os.path.exists(test_file):
        return []

    test_cases = []
    with open(test_file, encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            input_str, expected = parse_grapheme_break_test_line(line)
            if input_str is not None:
                test_cases.append(pytest.param(input_str, expected, id=f"line{line_num}"))

    return test_cases


@pytest.mark.parametrize(("input_str", "expected"), [
    ('', []),
    ('a', ['a']),
    ('abc', ['a', 'b', 'c']),
    ('cafe\u0301', ['c', 'a', 'f', 'e\u0301']),
    ('\r\n', ['\r\n']),
    ('ok\r\nok', ['o', 'k', '\r\n', 'o', 'k']),
    ('\r', ['\r']),
    ('ok\rok', ['o', 'k', '\r', 'o', 'k']),
    ('\n', ['\n']),
    ('ok\nok', ['o', 'k', '\n', 'o', 'k']),
    ('\r\r', ['\r', '\r']),
    ('ok\r\rok', ['o', 'k', '\r', '\r', 'o', 'k']),
])
def test_core_grapheme(input_str, expected):
    """Basic grapheme cluster segmentation."""
    assert list(iter_graphemes(input_str)) == expected


@pytest.mark.parametrize(("input_str", "start", "end", "expected"), [
    ('abcdef', 2, None, ['c', 'd', 'e', 'f']),
    ('abcdef', 0, 4, ['a', 'b', 'c', 'd']),
    ('abcdef', 1, 4, ['b', 'c', 'd']),
    ('abc', 10, None, []),
    ('abc', 0, 10, ['a', 'b', 'c']),
])
def test_iter_graphemes_slice(input_str, start, end, expected):
    """Grapheme iteration with start/end parameters."""
    assert list(iter_graphemes(input_str, start=start, end=end)) == expected


HANGUL_LV = '\u1100\u1161'
HANGUL_LVT = '\uAC00\u11A8'
FLAG_US = '\U0001F1FA\U0001F1F8'
FLAG_AU = '\U0001F1E6\U0001F1FA'
RI_A = '\U0001F1E6'
FAMILY = '\U0001F468\u200D\U0001F469\u200D\U0001F467'
WAVE_SKIN = '\U0001F44B\U0001F3FB'
HEART_EMOJI = '\u2764\uFE0F'


@pytest.mark.skipif(NARROW_ONLY, reason="requires wide Unicode")
@pytest.mark.parametrize(("input_str", "expected"), [
    (HANGUL_LV, [HANGUL_LV]),
    ('ok' + HANGUL_LV + 'ok', ['o', 'k', HANGUL_LV, 'o', 'k']),
    (HANGUL_LVT, [HANGUL_LVT]),
    ('ok' + HANGUL_LVT + 'ok', ['o', 'k', HANGUL_LVT, 'o', 'k']),
    (FLAG_US, [FLAG_US]),
    ('ok' + FLAG_US + 'ok', ['o', 'k', FLAG_US, 'o', 'k']),
    (FLAG_US + RI_A, [FLAG_US, RI_A]),
    ('ok' + FLAG_US + RI_A + 'ok', ['o', 'k', FLAG_US, RI_A, 'o', 'k']),
    (FLAG_US + FLAG_AU, [FLAG_US, FLAG_AU]),
    ('ok' + FLAG_US + FLAG_AU + 'ok', ['o', 'k', FLAG_US, FLAG_AU, 'o', 'k']),
    (FAMILY, [FAMILY]),
    ('ok' + FAMILY + 'ok', ['o', 'k', FAMILY, 'o', 'k']),
    (WAVE_SKIN, [WAVE_SKIN]),
    ('ok' + WAVE_SKIN + 'ok', ['o', 'k', WAVE_SKIN, 'o', 'k']),
    (HEART_EMOJI, [HEART_EMOJI]),
    ('ok' + HEART_EMOJI + 'ok', ['o', 'k', HEART_EMOJI, 'o', 'k']),
])
def test_wide_unicode_graphemes(input_str, expected):
    """Grapheme segmentation for wide Unicode characters."""
    assert list(iter_graphemes(input_str)) == expected


@pytest.mark.skipif(NARROW_ONLY, reason="requires wide Unicode")
@pytest.mark.parametrize(("input_str", "expected"), read_grapheme_break_test())
def test_unicode_grapheme_break_test(input_str, expected):
    """Validate against official Unicode GraphemeBreakTest.txt."""
    assert list(iter_graphemes(input_str)) == expected
