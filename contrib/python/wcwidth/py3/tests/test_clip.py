"""Tests for clip() and strip_sequences() functions."""
# 3rd party
import pytest

# local
from wcwidth import clip, width, strip_sequences

STRIP_SEQUENCES_CASES = [
    ('', ''),
    ('hello', 'hello'),
    ('hello world', 'hello world'),
    ('\x1b[31m', ''),
    ('\x1b[0m', ''),
    ('\x1b[m', ''),
    ('\x1b[31mred\x1b[0m', 'red'),
    ('\x1b[1m\x1b[31mbold red\x1b[0m', 'bold red'),
    ('\x1b[1m\x1b[31m\x1b[4m', ''),
    ('\x1b[1mbold\x1b[0m \x1b[3mitalic\x1b[0m', 'bold italic'),
    ('\x1b]0;title\x07', ''),
    ('\x1b]0;title\x07text', 'text'),
    ('\x1b]8;;https://example.com\x07link\x1b]8;;\x07', 'link'),
    ('\x1b[31m中文\x1b[0m', '中文'),
    ('\x1b[1m\U0001F468\u200D\U0001F469\u200D\U0001F467\x1b[0m',
     '\U0001F468\u200D\U0001F469\u200D\U0001F467'),
    ('\x1b', '\x1b'),
    ('a\x1bb', 'a\x1bb'),
    ('\x1b[', ''),
    ('text\x1b[mmore', 'textmore'),
]


@pytest.mark.parametrize('text,expected', STRIP_SEQUENCES_CASES)
def test_strip_sequences(text, expected):
    assert strip_sequences(text) == expected


CLIP_BASIC_CASES = [
    ('', 0, 5, ''),
    ('', 0, 0, ''),
    ('hello', 0, 0, ''),
    ('hello', 5, 5, ''),
    ('hello', 5, 3, ''),
    ('hello', -5, 3, 'hel'),
    ('hello', 0, 5, 'hello'),
    ('hello', 0, 3, 'hel'),
    ('hello', 2, 5, 'llo'),
    ('hello', 1, 4, 'ell'),
    ('hello world', 0, 5, 'hello'),
    ('hello world', 6, 11, 'world'),
    ('hello world', 0, 11, 'hello world'),
    ('hi', 0, 100, 'hi'),
    ('hi', 100, 200, ''),
]


@pytest.mark.parametrize('text,start,end,expected', CLIP_BASIC_CASES)
def test_clip_basic(text, start, end, expected):
    assert clip(text, start, end) == expected


CLIP_CJK_CASES = [
    ('中文字', 0, 6, '中文字'),
    ('中文字', 0, 4, '中文'),
    ('中文字', 0, 2, '中'),
    ('中文字', 2, 4, '文'),
    ('中文字', 0, 3, '中 '),
    ('中文字', 1, 6, ' 文字'),
    ('中文字', 1, 5, ' 文 '),
    ('A中B', 0, 4, 'A中B'),
    ('A中B', 0, 3, 'A中'),
    ('A中B', 1, 4, '中B'),
    ('A中B', 1, 3, '中'),
    ('A中B', 2, 4, ' B'),
    ('中', 0, 2, '中'),
    ('中', 0, 1, ' '),
    ('中', 1, 2, ' '),
]


@pytest.mark.parametrize('text,start,end,expected', CLIP_CJK_CASES)
def test_clip_cjk(text, start, end, expected):
    assert clip(text, start, end) == expected


def test_clip_cjk_custom_fillchar():
    assert clip('中文字', 1, 5, fillchar='.') == '.文.'
    assert clip('中文', 1, 3, fillchar='\u00b7') == '\u00b7\u00b7'


CLIP_CJK_WIDTH_CASES = [
    ('中文字', 0, 6, 6),
    ('中文字', 0, 3, 3),
    ('中文字', 1, 6, 5),
    ('中文字', 1, 5, 4),
]


@pytest.mark.parametrize('text,start,end,expected_width', CLIP_CJK_WIDTH_CASES)
def test_clip_cjk_width_consistency(text, start, end, expected_width):
    assert width(clip(text, start, end)) == expected_width


def test_clip_sequences_preserve_sgr():
    result = clip('\x1b[31mred\x1b[0m', 0, 3)
    assert result == '\x1b[31mred\x1b[0m'
    assert strip_sequences(result) == 'red'


def test_clip_sequences_before_start():
    assert clip('\x1b[31mred text\x1b[0m', 4, 8) == '\x1b[31mtext\x1b[0m'


def test_clip_sequences_after_end():
    # With propagate_sgr=True (default), no style active at start, so no prefix
    assert clip('hello\x1b[31m world\x1b[0m', 0, 5) == 'hello'
    # With propagate_sgr=False, all sequences preserved
    assert clip('hello\x1b[31m world\x1b[0m', 0, 5, propagate_sgr=False) == 'hello\x1b[31m\x1b[0m'


def test_clip_sequences_multiple():
    # With propagate_sgr=True (default), sequences collapsed to minimal
    assert clip('\x1b[1m\x1b[31mbold red\x1b[0m', 0, 4) == '\x1b[1;31mbold\x1b[0m'
    # With propagate_sgr=False, all sequences preserved separately
    assert clip('\x1b[1m\x1b[31mbold red\x1b[0m', 0, 4, propagate_sgr=False) == '\x1b[1m\x1b[31mbold\x1b[0m'


def test_clip_sequences_only():
    # With propagate_sgr=True (default), no visible text means empty result
    assert clip('\x1b[31m\x1b[0m', 0, 10) == ''
    # With propagate_sgr=False, sequences preserved
    assert clip('\x1b[31m\x1b[0m', 0, 10, propagate_sgr=False) == '\x1b[31m\x1b[0m'


def test_clip_sequences_osc_hyperlink():
    assert clip('\x1b]8;;https://example.com\x07link\x1b]8;;\x07', 0, 4) == \
        '\x1b]8;;https://example.com\x07link\x1b]8;;\x07'


def test_clip_sequences_cjk_with_sequences():
    assert clip('\x1b[31m中文\x1b[0m', 0, 3) == '\x1b[31m中 \x1b[0m'


def test_clip_sequences_partial_wide_at_start():
    assert clip('\x1b[31m中文\x1b[0m', 1, 4) == '\x1b[31m 文\x1b[0m'


def test_clip_sequences_between_chars():
    assert clip('a\x1b[31mb\x1b[0mc', 1, 2) == '\x1b[31mb\x1b[0m'


def test_clip_sequences_lone_esc():
    assert clip('a\x1bb', 0, 2) == 'a\x1bb'


CLIP_EMOJI_CASES = [
    ('\U0001F600', 2),
    ('\U0001F468\u200D\U0001F469\u200D\U0001F467', 2),
    ('\u2764\uFE0F', 2),
    ('\U0001F1FA\U0001F1F8', 2),
]


@pytest.mark.parametrize('emoji,full_width', CLIP_EMOJI_CASES)
def test_clip_emoji(emoji, full_width):
    assert clip(emoji, 0, full_width) == emoji
    assert clip(emoji, 0, 1) == ' '
    assert width(emoji) == full_width


def test_clip_emoji_with_sequences():
    assert clip('\x1b[1m\U0001F600\x1b[0m', 0, 2) == '\x1b[1m\U0001F600\x1b[0m'


def test_clip_combining_accent():
    assert clip('cafe\u0301', 0, 4) == 'cafe\u0301'
    assert clip('cafe\u0301', 0, 3) == 'caf'


def test_clip_combining_multiple():
    assert clip('e\u0301\u0327', 0, 1) == 'e\u0301\u0327'


def test_clip_zero_width_position_bounds():
    # Standalone combining mark before visible region should NOT be included
    assert clip('\u0301hello', 1, 4) == 'ell'
    # Standalone combining mark after visible region should NOT be included
    assert clip('hello\u0301', 0, 3) == 'hel'
    # Combining mark within visible region should be included (attached to base)
    assert clip('he\u0301llo', 0, 4) == 'he\u0301ll'


def test_clip_prepend_grapheme():
    # PREPEND characters (Arabic Number Sign) cluster with following char, width 2
    # Full cluster fits
    assert clip('\u0600abc', 0, 2) == '\u0600a'
    # Cluster split at start boundary - replaced with fillchar
    assert clip('\u0600abc', 0, 1) == ' '
    # Cluster split at end boundary - partial overlap gets fillchar
    assert clip('\u0600abc', 1, 3) == ' b'
    # Clipping after the prepend cluster
    assert clip('\u0600abc', 2, 4) == 'bc'


def test_clip_ambiguous_width_1():
    assert clip('\u00b1test', 0, 3, ambiguous_width=1) == '\u00b1te'


def test_clip_ambiguous_width_2():
    assert clip('\u00b1test', 0, 3, ambiguous_width=2) == '\u00b1t'


CLIP_TAB_CASES = [
    ('a\tb', 0, 10, 8, 'a       b'),
    ('a\tb', 0, 4, 8, 'a   '),
    ('a\tb', 0, 10, 4, 'a   b'),
    ('a\tb', 4, 10, 8, '    b'),
    ('a\tb\tc', 0, 20, 4, 'a   b   c'),
    ('中\tb', 0, 10, 4, '中  b'),
    ('a\tb', 0, 5, 0, 'a\tb'),
]


@pytest.mark.parametrize('text,start,end,tabsize,expected', CLIP_TAB_CASES)
def test_clip_tab_expansion(text, start, end, tabsize, expected):
    assert clip(text, start, end, tabsize=tabsize) == expected


def test_clip_tab_with_sequences():
    assert clip('\x1b[31mab\tc\x1b[0m', 0, 12, tabsize=4) == '\x1b[31mab  c\x1b[0m'


CLIP_CONTROL_CHAR_CASES = [
    ('abc\bde', 0, 5, 'abc\bde'),
    ('ab\acd', 0, 4, 'ab\acd'),
    ('ab\x00cd', 0, 4, 'ab\x00cd'),
    ('abc\rde', 0, 5, 'abc\rde'),
    ('\a\b\rHello', 0, 5, '\a\b\rHello'),
    ('ab\x01\x02cd', 0, 4, 'ab\x01\x02cd'),
]


@pytest.mark.parametrize('text,start,end,expected', CLIP_CONTROL_CHAR_CASES)
def test_clip_control_chars_zero_width(text, start, end, expected):
    assert clip(text, start, end) == expected


CLIP_CURSOR_SEQUENCE_CASES = [
    ('ab\x1b[5Ccd', 0, 4, 'ab\x1b[5Ccd'),
    ('abcde\x1b[2Df', 0, 6, 'abcde\x1b[2Df'),
    ('ab\x1b[10Ccd', 0, 4, 'ab\x1b[10Ccd'),
    ('ab\x1b[Ccd', 0, 4, 'ab\x1b[Ccd'),
]


@pytest.mark.parametrize('text,start,end,expected', CLIP_CURSOR_SEQUENCE_CASES)
def test_clip_cursor_sequences_zero_width(text, start, end, expected):
    assert clip(text, start, end) == expected


def test_clip_tab_first_visible_with_sgr():
    """Tab as first visible character with SGR propagation."""
    assert clip('\x1b[31m\tb', 0, 4, tabsize=8) == '\x1b[31m    \x1b[0m'
