"""Tests for grapheme cluster segmentation."""
# std imports
import os

# 3rd party
import pytest

# local
from wcwidth import iter_graphemes, iter_graphemes_reverse, grapheme_boundary_before

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
    import yatest.common as yc
    test_file = os.path.join(os.path.dirname(yc.source_path(__file__)), 'GraphemeBreakTest.txt')
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
@pytest.mark.skipif(not os.path.exists(os.path.join(os.path.dirname(__file__), 'GraphemeBreakTest.txt')),
                    reason="GraphemeBreakTest.txt is missing; run bin/update-tables.py")
@pytest.mark.parametrize(("input_str", "expected"), read_grapheme_break_test())
def test_unicode_grapheme_break_test(input_str, expected):
    """Validate against official Unicode GraphemeBreakTest.txt."""
    assert list(iter_graphemes(input_str)) == expected


# Prepend: Arabic Number Sign
PREPEND_CHAR = '\u0600'
# Multiple combining marks: e + acute + grave
MULTI_COMBINE = 'e\u0301\u0300'


# grapheme_boundary_before(text, pos) returns start of grapheme cluster before pos.
# (text, pos, expected): pos=search from here, expected=where cluster starts
@pytest.mark.parametrize(("text", "pos", "expected"), [
    # 'abc': 0=a, 1=b, 2=c
    ('abc', 3, 2),  # from end -> 'c' at 2
    ('abc', 2, 1),  # from 'c' -> 'b' at 1
    ('abc', 1, 0),  # from 'b' -> 'a' at 0
    # 'a\r\nb': CRLF is one cluster (GB3)
    ('a\r\nb', 3, 1),  # from 'b' -> '\r\n' at 1
    # 'café': e + combining acute is one cluster (GB9)
    ('cafe\u0301', 5, 3),  # from end -> 'é' at 3
    ('cafe\u0301', 4, 3),  # from acute -> still 'é' at 3
    # Multiple combining marks: e + acute + grave (GB9)
    ('a' + MULTI_COMBINE + 'b', 4, 1),  # from 'b' -> e+marks at 1
    # Prepend + char is one cluster (GB9b)
    (PREPEND_CHAR + 'a', 2, 0),  # whole cluster
    # Prepend + Control: control breaks (GB4)
    (PREPEND_CHAR + '\n', 2, 1),  # '\n' separate at 1
    # C1 control (NEL, 0x85) stops backward scan in _find_cluster_start (GB4)
    ('X\x85\u0301', 3, 2),
])
def test_grapheme_boundary_before_basic(text, pos, expected):
    """Basic grapheme_boundary_before tests."""
    assert grapheme_boundary_before(text, pos) == expected


@pytest.mark.skipif(NARROW_ONLY, reason="requires wide Unicode")
@pytest.mark.parametrize(("text", "pos", "expected"), [
    # 'Hi 👋🏻!': 0=H,1=i,2=space,3=wave,4=skin,5=!; wave+skin is one cluster
    ('Hi \U0001F44B\U0001F3FB!', 6, 5),  # from end -> '!' at 5
    ('Hi \U0001F44B\U0001F3FB!', 5, 3),  # from '!' -> wave+skin at 3
    ('Hi \U0001F44B\U0001F3FB!', 3, 2),  # from wave -> space at 2
    # 'a🇺🇸b': 0=a,1-2=flag,3=b; flag is one cluster (GB12/13)
    ('a' + FLAG_US + 'b', 4, 3),  # from end -> 'b' at 3
    ('a' + FLAG_US + 'b', 3, 1),  # from 'b' -> flag at 1
    # Three RIs (🇺🇸🇦): flag + solo RI
    (FLAG_US + RI_A, 3, 2),  # from end -> solo RI at 2
    (FLAG_US + RI_A, 2, 0),  # from solo -> flag at 0
    # 'a👨‍👩‍👧b': 0=a,1-5=family,6=b; ZWJ sequence is one cluster (GB11)
    ('a' + FAMILY + 'b', 7, 6),  # from end -> 'b' at 6
    ('a' + FAMILY + 'b', 6, 1),  # from 'b' -> family at 1
])
def test_grapheme_boundary_before_unicode(text, pos, expected):
    """grapheme_boundary_before with emoji and wide Unicode."""
    assert grapheme_boundary_before(text, pos) == expected


@pytest.mark.parametrize(("input_str", "expected"), [
    ('', []),
    ('abc', ['c', 'b', 'a']),
    # café with combining mark mixed with CRLF
    ('cafe\u0301\r\nok', ['k', 'o', '\r\n', 'e\u0301', 'f', 'a', 'c']),
])
def test_iter_graphemes_reverse_basic(input_str, expected):
    """Basic iter_graphemes_reverse tests."""
    assert list(iter_graphemes_reverse(input_str)) == expected


@pytest.mark.skipif(NARROW_ONLY, reason="requires wide Unicode")
@pytest.mark.parametrize(("input_str", "expected"), [
    # Multiple emoji types in one string
    ('cafe\u0301 ' + WAVE_SKIN + ' ' + FLAG_US + '!',
     ['!', FLAG_US, ' ', WAVE_SKIN, ' ', 'e\u0301', 'f', 'a', 'c']),
    # Two families
    (FAMILY + FAMILY, [FAMILY, FAMILY]),
    # Flag + solo RI + text
    ('Hi' + FLAG_US + RI_A + '!', ['!', RI_A, FLAG_US, 'i', 'H']),
])
def test_iter_graphemes_reverse_unicode(input_str, expected):
    """iter_graphemes_reverse with wide Unicode."""
    assert list(iter_graphemes_reverse(input_str)) == expected


@pytest.mark.skipif(NARROW_ONLY, reason="requires wide Unicode")
@pytest.mark.skipif(not os.path.exists(os.path.join(os.path.dirname(__file__), 'GraphemeBreakTest.txt')),
                    reason="GraphemeBreakTest.txt is missing; run bin/update-tables.py")
@pytest.mark.parametrize(("input_str", "expected"), read_grapheme_break_test())
def test_grapheme_roundtrip_consistency(input_str, expected):
    """Forward and reverse iteration produce identical boundaries."""
    forward = list(iter_graphemes(input_str))
    reverse = list(iter_graphemes_reverse(input_str))[::-1]
    assert forward == reverse


def test_grapheme_boundary_before_edge_cases():
    """Edge cases for grapheme_boundary_before."""
    assert grapheme_boundary_before('abc', 0) == 0
    assert grapheme_boundary_before('abc', 100) == 2  # pos > len clamps
    assert grapheme_boundary_before('', 0) == 0


def test_iter_graphemes_reverse_edge_cases():
    """Edge cases for iter_graphemes_reverse."""
    assert list(iter_graphemes_reverse('abcdef', start=2, end=5)) == ['e', 'd', 'c']
    assert list(iter_graphemes_reverse('abc', start=0, end=100)) == ['c', 'b', 'a']
    assert not list(iter_graphemes_reverse('abc', start=5))
    assert not list(iter_graphemes_reverse('abc', start=2, end=2))
    # PREPEND + char is one grapheme (GB9b), so start=1 yields nothing (won't split)
    assert not list(iter_graphemes_reverse(PREPEND_CHAR + 'a', start=1))
    # But start=0 yields the full grapheme
    assert list(iter_graphemes_reverse(PREPEND_CHAR + 'a', start=0)) == [PREPEND_CHAR + 'a']
    # Negative start is clamped to 0
    assert list(iter_graphemes_reverse('abc', start=-5)) == ['c', 'b', 'a']
