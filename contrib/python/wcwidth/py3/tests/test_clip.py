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
    ('a\x1bb', 'a'),
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
    assert repr(clip('hello\x1b[31m world\x1b[0m', 0, 5, propagate_sgr=False)) == repr('hello\x1b[31m\x1b[0m')


def test_clip_sequences_multiple():
    # With propagate_sgr=True (default), sequences collapsed to minimal
    assert clip('\x1b[1m\x1b[31mbold red\x1b[0m', 0, 4) == '\x1b[1;31mbold\x1b[0m'
    # With propagate_sgr=False, all sequences preserved separately
    assert repr(clip('\x1b[1m\x1b[31mbold red\x1b[0m', 0, 4, propagate_sgr=False)) == repr('\x1b[1m\x1b[31mbold\x1b[0m')


def test_clip_sequences_only():
    # With propagate_sgr=True (default), no visible text means empty result
    assert clip('\x1b[31m\x1b[0m', 0, 10) == ''
    # With propagate_sgr=False, sequences preserved
    assert repr(clip('\x1b[31m\x1b[0m', 0, 10, propagate_sgr=False)) == repr('\x1b[31m\x1b[0m')


def test_clip_sequences_osc_hyperlink():
    assert repr(clip('\x1b]8;;https://example.com\x07link\x1b]8;;\x07', 0, 4)) == repr(
        '\x1b]8;;https://example.com\x07link\x1b]8;;\x07'
    )


# OSC 8 hyperlink clipping

OSC_START_BEL = '\x1b]8;;http://example.com\x07'
OSC_END_BEL = '\x1b]8;;\x07'
OSC_START_ST = '\x1b]8;;http://example.com\x1b\\'
OSC_END_ST = '\x1b]8;;\x1b\\'


CLIP_HYPERLINK_CASES = [
    # Full hyperlink visible -- preserved as-is
    (f'{OSC_START_BEL}link{OSC_END_BEL}', 0, 4,
     f'{OSC_START_BEL}link{OSC_END_BEL}'),
    # Clipping middle of hyperlink text -- rebuild around clipped inner text
    (f'{OSC_START_BEL}Click This link{OSC_END_BEL}', 6, 10,
     f'{OSC_START_BEL}This{OSC_END_BEL}'),
    # Clipping from start -- only first portion
    (f'{OSC_START_BEL}Click This{OSC_END_BEL}', 0, 5,
     f'{OSC_START_BEL}Click{OSC_END_BEL}'),
    # Clipping from end -- only last portion
    (f'{OSC_START_BEL}Click This{OSC_END_BEL}', 6, 10,
     f'{OSC_START_BEL}This{OSC_END_BEL}'),
    # Hyperlink entirely before clip window -- dropped
    (f'{OSC_START_BEL}link{OSC_END_BEL}world', 0, 4,
     f'{OSC_START_BEL}link{OSC_END_BEL}'),
    # Hyperlink entirely after clip window -- dropped
    (f'hello{OSC_START_BEL}link{OSC_END_BEL}', 0, 5, 'hello'),
    # Hyperlink clipped to nothing -- empty hyperlink dropped
    (f'{OSC_START_BEL}link{OSC_END_BEL}', 5, 10, ''),
    # Empty hyperlink (no inner text) -- dropped
    (f'before{OSC_START_BEL}{OSC_END_BEL}after', 0, 11, 'beforeafter'),
    # Hyperlink with CJK text clipped
    (f'{OSC_START_BEL}中文文字{OSC_END_BEL}', 0, 4,
     f'{OSC_START_BEL}中文{OSC_END_BEL}'),
    # Hyperlink with CJK text clipped at odd column
    (f'{OSC_START_BEL}中文文字{OSC_END_BEL}', 0, 3,
     f'{OSC_START_BEL}中 {OSC_END_BEL}'),
    # Hyperlink with ST terminator
    (f'{OSC_START_ST}Click This{OSC_END_ST}', 0, 5,
     f'{OSC_START_ST}Click{OSC_END_ST}'),
    # Multiple non-overlapping hyperlinks
    (f'{OSC_START_BEL}ab{OSC_END_BEL} {OSC_START_ST}cd{OSC_END_ST}', 0, 5,
     f'{OSC_START_BEL}ab{OSC_END_BEL} {OSC_START_ST}cd{OSC_END_ST}'),
    # Hyperlink with params preserved
    ('\x1b]8;id=myid;http://example.com\x07link\x1b]8;;\x07', 1, 3,
     '\x1b]8;id=myid;http://example.com\x07in\x1b]8;;\x07'),
    # Hyperlink text before clip window, hyperlink within
    (f'before{OSC_START_BEL}link{OSC_END_BEL}', 6, 10,
     f'{OSC_START_BEL}link{OSC_END_BEL}'),
    # SGR inside hyperlink is preserved
    (f'{OSC_START_BEL}\x1b[31mred link\x1b[0m{OSC_END_BEL}', 4, 8,
     f'{OSC_START_BEL}\x1b[31mlink\x1b[0m{OSC_END_BEL}'),
    # Hyperlink open without matching close -- preserved as regular sequence
    ('\x1b]8;;http://example.com\x07link', 0, 4, '\x1b]8;;http://example.com\x07link'),
    # Bare ESC between hyperlink markers
    ('\x1b]8;;url\x07ab\x1bxcd\x1b]8;;\x07', 0, 6,
     '\x1b]8;;url\x07ab\x1bxcd\x1b]8;;\x07'),
    # Per OSC 8 spec "A note on opening/closing hyperlinks": terminal
    # emulators treat hyperlinks as a state attribute, not nested anchors.
    # Opening a new hyperlink replaces the current one; a single close
    # terminates the hyperlink regardless of how many opens preceded it.
    #
    # Two opens, one close: URL "b" replaces "a", close terminates.
    ('\x1b]8;;a\x07AB\x1b]8;;b\x07CD\x1b]8;;\x07EF', 0, 6,
     '\x1b]8;;a\x07AB\x1b]8;;b\x07CD\x1b]8;;\x07EF'),
    # URL switch without closing: "b" replaces "a", no close in input.
    ('\x1b]8;;a\x07AB\x1b]8;;b\x07CD', 0, 4,
     '\x1b]8;;a\x07AB\x1b]8;;b\x07CD'),
    # Multiple opens, close, bare close: "b" replaces "a", first close
    # terminates, trailing close is harmless (closing when not open).
    ('\x1b]8;;a\x07ABCD \x1b]8;;b\x07XY\x1b]8;;\x07 EF\x1b]8;;\x07', 0, 10,
     '\x1b]8;;a\x07ABCD \x1b]8;;b\x07XY\x1b]8;;\x07 EF\x1b]8;;\x07'),
]


@pytest.mark.parametrize('text,start,end,expected', CLIP_HYPERLINK_CASES)
def test_clip_osc_hyperlink_text_clipping(text, start, end, expected):
    """OSC 8 hyperlink inner text is clipped and hyperlink rebuilt."""
    assert repr(clip(text, start, end)) == repr(expected)


# Control_codes variants with cursor movement into hyperlink
#
# Overwriting hyperlink cells causes corrupted "run on" hyperlinks in practical
# testing with kitty, presumably the hidden "end hyperlink" sequence is
# overwritten, in any case, we make no attempt to parse overwrite of
# hyperlinks, we consider it a "glitch sequence
_HLINK_OVERWRITE = f'{OSC_START_BEL}link{OSC_END_BEL}\x1b[2Dxy'
CLIP_HYPERLINK_CONTROL_CODES_CASES = [
    ('parse', 0, 4, f'{OSC_START_BEL}link{OSC_END_BEL}'),
    ('parse', 0, 3, f'{OSC_START_BEL}lin{OSC_END_BEL}'),
    ('parse', 0, 2, f'{OSC_START_BEL}li{OSC_END_BEL}'),
    ('parse', 0, 1, f'{OSC_START_BEL}l{OSC_END_BEL}'),
    # these next two are certainly "in error"
    ('parse', 1, 4, f'{OSC_START_BEL}ink{OSC_END_BEL}y'),
    ('parse', 1, 3, f'{OSC_START_BEL}in{OSC_END_BEL}x'),
    ('parse', 1, 2, f'{OSC_START_BEL}i{OSC_END_BEL}'),
    ('ignore', 0, 20, f'{_HLINK_OVERWRITE}'),
    # and these two, 'xy' are missing entirely, also "in error"
    ('parse', 0, 20, f'{OSC_START_BEL}link{OSC_END_BEL}'),
    ('strict', 0, 20, f'{OSC_START_BEL}link{OSC_END_BEL}'),
]


@pytest.mark.parametrize('control_codes,start,end,expected',
                         CLIP_HYPERLINK_CONTROL_CODES_CASES)
def test_clip_hyperlink_control_codes_overwrite(control_codes, start, end, expected):
    assert repr(clip(_HLINK_OVERWRITE, start, end, control_codes=control_codes)) == repr(expected)


# Painter-path hyperlink edge cases
CLIP_HYPERLINK_PAINTER_CASES = [
    # Empty hyperlink dropped
    (f'\x1b[2D{OSC_START_BEL}{OSC_END_BEL}xy', 'parse', 0, 4, 'xy'),
    # Hyperlink entirely after clip window -- skipped
    (f'\x1b[2Dab{OSC_START_BEL}cde{OSC_END_BEL}', 'parse', 0, 2, 'ab'),
    # Hyperlink entirely before clip window -- skipped
    (f'{OSC_START_BEL}ab{OSC_END_BEL}\x1b[2Dcdef', 'parse', 2, 4, 'ef'),
    # Hyperlink overlapping clip window -- clipped
    (f'\x1b[2D{OSC_START_BEL}abcdef{OSC_END_BEL}', 'parse', 0, 3,
     f'{OSC_START_BEL}abc{OSC_END_BEL}'),
    # Bare ESC inside hyperlink in painter path
    (f'\x1b[2D{OSC_START_BEL}a\x1bb{OSC_END_BEL}', 'parse', 0, 4,
     f'{OSC_START_BEL}a\x1bb{OSC_END_BEL}'),
    # strict mode: non-hyperlink cells don't overlap hyperlink_cells
    (f'{OSC_START_BEL}link{OSC_END_BEL}\x1b[5Chi', 'strict', 0, 11,
     f'{OSC_START_BEL}link{OSC_END_BEL}     hi'),
]


@pytest.mark.parametrize('text,control_codes,start,end,expected',
                         CLIP_HYPERLINK_PAINTER_CASES)
def test_clip_hyperlink_painter_cases(text, control_codes, start, end, expected):
    assert repr(clip(text, start, end, control_codes=control_codes)) == repr(expected)


def test_clip_sequences_cjk_with_sequences():
    assert clip('\x1b[31m中文\x1b[0m', 0, 3) == '\x1b[31m中 \x1b[0m'


def test_clip_sequences_partial_wide_at_start():
    assert clip('\x1b[31m中文\x1b[0m', 1, 4) == '\x1b[31m 文\x1b[0m'


def test_clip_sequences_between_chars():
    assert clip('a\x1b[31mb\x1b[0mc', 1, 2) == '\x1b[31mb\x1b[0m'


def test_clip_sequences_fs_escape():
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
    ('abc\bde', 0, 5, 'abde'),
    ('ab\acd', 0, 4, 'ab\x07cd'),
    ('ab\x00cd', 0, 4, 'ab\x00cd'),
    ('abc\rde', 0, 5, 'dec'),
    ('\a\b\rHello', 0, 5, '\x07Hello'),
    ('ab\x01\x02cd', 0, 4, 'ab\x01\x02cd'),
    ('ab\x1b\x00cd', 0, 4, 'ab\x1b\x00cd'),
]


@pytest.mark.parametrize('text,start,end,expected', CLIP_CONTROL_CHAR_CASES)
def test_clip_control_chars_zero_width(text, start, end, expected):
    assert clip(text, start, end) == expected


def test_clip_tab_first_visible_with_sgr():
    """Tab as first visible character with SGR propagation."""
    assert clip('\x1b[31m\tb', 0, 4, tabsize=8) == '\x1b[31m    \x1b[0m'


def test_clip_overtyping_override_by_control_codes_ignore():
    """When overtyping=True and control_codes='ignore', overtyping is overridden to False."""
    # elif entered: overtyping=True + control_codes='ignore' → overtyping=False
    assert clip('hello world', 0, 5, overtyping=True, control_codes='ignore') == 'hello'
    # Verify that overtyping is actually disabled: cursor movement chars are
    # treated as zero-width, so the result is the same as without overtyping.
    assert clip('ab\x08cd', 0, 4, overtyping=True, control_codes='ignore') == 'ab\x08cd'


def test_clip_overtyping_without_ignore():
    """When overtyping=True and control_codes='parse', elif is not entered."""
    # elif skipped: overtyping=True + control_codes='parse' → overtyping stays True
    # The painter path is used, cursor movement sequences affect output.
    assert clip('ab\x1b[2Dcd', 0, 4, overtyping=True, control_codes='parse') == 'cd'


# Indeterminate-effect sequences that raise ValueError in strict mode
# (matching width() behavior).  These are not cursor-movement sequences,
# so they exercise the simple (non-overtyping) path.

INDETERMINATE_SEQUENCES = [
    ('\x1b[K', 'erase_in_line'),
    ('\x1b[2K', 'erase_in_line_params'),
    ('\x1b[J', 'erase_in_display'),
    ('\x1b[2J', 'erase_in_display_params'),
    ('\x1b[H', 'cursor_home'),
    ('\x1b[1;1H', 'cursor_address'),
    ('\x1b[A', 'cursor_up'),
    ('\x1b[2A', 'cursor_up_params'),
    ('\x1b[B', 'cursor_down'),
    ('\x1b[5B', 'cursor_down_params'),
    ('\x1b[P', 'delete_character'),
    ('\x1b[1P', 'parm_dch'),
    ('\x1b[M', 'delete_line'),
    ('\x1b[1M', 'parm_delete_line'),
    ('\x1b[L', 'insert_line'),
    ('\x1b[1L', 'parm_insert_line'),
    ('\x1b[@', 'insert_character'),
    ('\x1b[1X', 'erase_chars'),
    ('\x1b[S', 'scroll_up'),
    ('\x1b[T', 'scroll_down'),
    ('\x1b[?1049h', 'enter_fullscreen'),
    ('\x1b[?1049l', 'exit_fullscreen'),
    ('\x1bD', 'scroll_forward'),
    ('\x1bM', 'scroll_reverse'),
    ('\x1b8', 'restore_cursor'),
    ('\x1bc', 'full_reset'),
]


@pytest.mark.parametrize('seq,cap_name', INDETERMINATE_SEQUENCES)
def test_clip_strict_indeterminate_raises(seq, cap_name):
    """Clip() strict mode raises ValueError on indeterminate-effect sequences."""
    with pytest.raises(ValueError, match='Indeterminate cursor sequence'):
        clip(f'hello{seq}world', 0, 10, control_codes='strict')


@pytest.mark.parametrize('seq,cap_name', INDETERMINATE_SEQUENCES)
def test_clip_parse_indeterminate_preserved(seq, cap_name):
    """Clip() parse mode preserves indeterminate sequences as zero-width."""
    result = clip(f'hello{seq}world', 0, 10, control_codes='parse')
    # The sequence is preserved, visible text is hello + world = 10 chars
    assert 'hello' in result
    assert 'world' in result
    assert seq in result
