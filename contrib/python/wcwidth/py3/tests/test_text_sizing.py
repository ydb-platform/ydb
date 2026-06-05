"""Tests for Text Sizing Protocol (OSC 66) support."""

# 3rd party
import pytest

# local
from wcwidth import (TextSizing,
                     TextSizingParams,
                     clip,
                     width,
                     wcswidth,
                     iter_sequences,
                     strip_sequences)
from wcwidth.text_sizing import TEXT_FIELD_MAPPING
from wcwidth.escape_sequences import TEXT_SIZING_PATTERN

_W_HI = TEXT_FIELD_MAPPING['w'].high
_N_HI = TEXT_FIELD_MAPPING['n'].high
_D_HI = TEXT_FIELD_MAPPING['d'].high

CONTROL_CODES_PARAMS_CASES = [
    ('x=2', "", "Unknown text sizing field 'x' in "),
    ('s=3:x=3', "s=3", "Unknown text sizing field 'x' in "),
    ('s=2:x=3:w=9', f"s=2:w={_W_HI}", "Unknown text sizing field 'x' in "),
    ('xyz=2', "", "Unknown text sizing field 'xyz' in "),
    ('xxx', "", "Expected '=' in text sizing parameter"),
    ('s=xxx', "", "Illegal text sizing value 'xxx' in "),
    ('s=-99', "", "Out of bounds text sizing value '-99' in "),
    ('s=99', f"s={_W_HI}", "Out of bounds text sizing value '99' in "),
    ('w=-1', "", "Out of bounds text sizing value '-1' in "),
    ('w=8', f"w={_W_HI}", "Out of bounds text sizing value '8' in "),
    ('n=20', f"n={_N_HI}", "Out of bounds text sizing value '20' in "),
    ('d=99', f"d={_D_HI}", "Out of bounds text sizing value '99' in "),
    ('v=5', "v=2", "Out of bounds text sizing value '5' in "),
    ('h=3', "h=2", "Out of bounds text sizing value '3' in "),
]


@pytest.mark.parametrize('given_params,expected_remainder,expected_exc,', CONTROL_CODES_PARAMS_CASES)
def test_text_sizing_params_control_codes(given_params, expected_remainder, expected_exc):
    """Verify control_codes='strict' and 'parse' behavior in TextSizingParams.from_params()."""
    # assert control_codes='strict' raises expected exception,
    with pytest.raises(ValueError) as exc_info:
        TextSizingParams.from_params(given_params, control_codes='strict')
    assert exc_info.value.args[0].startswith(expected_exc)

    # when 'parse' (default), any illegal argument or value is filtered, excluded, or clipped
    params = TextSizingParams.from_params(given_params)
    assert params.make_sequence() == expected_remainder


@pytest.mark.parametrize('given_params,expected_remainder,expected_exc,', CONTROL_CODES_PARAMS_CASES)
def test_text_sizing_width_control_codes(given_params, expected_remainder, expected_exc):
    """Verify control_codes='strict' with invalid OSC 66 sequences in wciwdth.width()."""
    seq1 = '\x1b]66;' + given_params + ';ABC' + '\x07'
    seq2 = '\x1b]66;' + given_params + ';ABC' + '\x1b\\'
    for seq in (seq1, seq2):
        with pytest.raises(ValueError) as exc_info:
            width(seq, control_codes='strict')
        assert exc_info.value.args[0].startswith(expected_exc)


@pytest.mark.parametrize('params,expected_repr', [
    (TextSizingParams(), 'TextSizingParams()'),
    (TextSizingParams(scale=2, width=1), 'TextSizingParams(scale=2, width=1)'),
    (TextSizingParams(scale=2, width=3, numerator=1, denominator=2,
                      vertical_align=1, horizontal_align=2),
     'TextSizingParams(scale=2, width=3, numerator=1, denominator=2, '
     'vertical_align=1, horizontal_align=2)'),
])
def test_text_sizing_params_repr(params, expected_repr):
    """Verify TextSizingParams.__repr__ output."""
    assert repr(params) == expected_repr


@pytest.mark.parametrize('params,text,expected_width', [
    # cases of static width=N values,
    (TextSizingParams(scale=2, width=1), 'climclam', 2),
    (TextSizingParams(scale=2, width=3), 'anything', 6),
    (TextSizingParams(scale=1, width=5), '', 5),
    (TextSizingParams(scale=3, width=1), 'x', 3),
    # and automatic width (width=0) values,
    (TextSizingParams(), '', 0),
    (TextSizingParams(), 'AB', 2),
    (TextSizingParams(), '中', 2),
    (TextSizingParams(scale=2), 'AB', 4),
    (TextSizingParams(scale=2), '中', 4),
    (TextSizingParams(scale=3), '', 0),
    (TextSizingParams(scale=7, width=7, numerator=15, denominator=15,
                      vertical_align=2, horizontal_align=2), 'x!yzzy', 49),
])
def test_text_sizing_width(params, text, expected_width):
    """Verify width using with both kinds of terminator."""
    # verify internal TextSizing.display_width() result,
    assert TextSizing(params, text, terminator='\x07').display_width() == expected_width
    assert TextSizing(params, text, terminator='\x1b\\').display_width() == expected_width
    seq1 = TextSizing(params, text, terminator='\x07').make_sequence()
    seq2 = TextSizing(params, text, terminator='\x1b\\').make_sequence()

    # verify round-trip
    ts_match1, ts_match2 = TEXT_SIZING_PATTERN.match(seq1), TEXT_SIZING_PATTERN.match(seq2)
    assert ts_match1 and ts_match2
    assert TextSizing.from_match(ts_match1) == TextSizing(params, text, terminator='\x07')
    assert TextSizing.from_match(ts_match2) == TextSizing(params, text, terminator='\x1b\\')

    # and external width(),
    assert width(seq1) == expected_width
    assert width(seq2) == expected_width

    # verify 'strict' does not raise ValueError
    width(seq1, control_codes='strict')
    width(seq2, control_codes='strict')

    # and verify 'ignore' measures only inner_text (does not parse scale or width)
    assert width(seq1, control_codes='ignore') == wcswidth(text)
    assert width(seq2, control_codes='ignore') == wcswidth(text)


@pytest.mark.parametrize('given_sequence,expected_text,expected_params,expected_width', [
    ('\x1b]66;s=2:w=2;AB\x07', 'AB', 's=2:w=2', 4),
    ('\x1b]66;s=2:w=2;\u4e2d\x07', '\u4e2d', 's=2:w=2', 4),
    ('\x1b]66;s=3:w=1;x\x07', 'x', 's=3:w=1', 3),
    ('\x1b]66;w=5;hello\x07', 'hello', 'w=5', 5),
    ('\x1b]66;s=2:w=3;anything\x07', 'anything', 's=2:w=3', 6),
    ('\x1b]66;w=3;x\x07', 'x', 'w=3', 3),
    ('\x1b]66;s=1;AB\x07', 'AB', '', 2),
    ('\x1b]66;s=2;AB\x07', 'AB', 's=2', 4),
    ('\x1b]66;s=2;中\x07', '中', 's=2', 4),
    ('\x1b]66;s=2;\x07', '', 's=2', 0),
    ('\x1b]66;s=1:w=1;\x07', '', 'w=1', 1),
    ('\x1b]66;w=2;A\x07', 'A', 'w=2', 2),
    ('\x1b]66;s=2:w=3;text\x1b\\', 'text', 's=2:w=3', 6),
])
def test_text_sizing_sequence(given_sequence, expected_text, expected_params, expected_width):
    """Verify parsing and measured width of raw OSC 66 sequence."""
    ts_match = TEXT_SIZING_PATTERN.match(given_sequence)
    assert ts_match is not None
    text_size = TextSizing.from_match(ts_match)
    assert text_size.params.make_sequence() == expected_params
    assert text_size.text == expected_text
    assert width(given_sequence, control_codes='parse') == expected_width
    assert width(given_sequence, control_codes='strict') == expected_width
    assert width(given_sequence, control_codes='ignore') == wcswidth(expected_text)


@pytest.mark.parametrize('text,expected', [
    ('\x1b]66;s=2:w=3:n=1:d=2:v=1:h=2;x!yzzy\x1b\\', 6),
    ('\x1b]66;s=2:w=3;anything\x07', 6),
    ('\x1b]66;w=3;x\x07', 3),
    ('\x1b]66;s=1:w=0;AB\x07', 2),
    ('\x1b]66;s=2:w=0;AB\x07', 4),
    ('\x1b]66;s=2:w=0;\u4e2d\x07', 4),  # '中'
    ('\x1b]66;s=1:w=0;\x07', 0),
    ('abc\x1b]66;w=3;x\x07def', 9),
    ('\x1b]66;w=2;A\x07\x1b]66;w=3;B\x07', 5),
    ('\x1b]66;s=2:w=3;text\x1b\\', 6),
    ('\x1b[31m\x1b]66;w=2;AB\x07\x1b[0m', 2),
])
def test_strings_with_text_sizing(text, expected):
    """Verify measured width strings containing OSC66."""
    assert width(text) == expected
    assert width(text, control_codes='strict') == expected


@pytest.mark.parametrize('text,expected', [
    ('\x1b]66;s=2;hello\x07', 'hello'),
    ('\x1b]66;s=2;hello\x1b\\', 'hello'),
    ('\x1b]66;;text\x07', 'text'),
    ('\x1b]66;s=3:w=2;\x07', ''),
    ('abc\x1b]66;w=2;XY\x07def', 'abcXYdef'),
    ('\x1b[31m\x1b]66;s=2;red\x07\x1b[0m', 'red'),
    ('\x1b]66;w=1;A\x07\x1b]66;w=1;B\x07', 'AB'),
])
def test_strip_strings_with_text_sizing(text, expected):
    assert strip_sequences(text) == expected


@pytest.mark.parametrize('text,expected_segs', [
    ('abc\x1b]66;s=2;hello\x07def', [('abc', False), ('\x1b]66;s=2;hello\x07', True), ('def', False)]),
    ('abc\x1b]66;s=2;n=1,d=2,w=3;hello\x1b\\def', [('abc', False), ('\x1b]66;s=2;n=1,d=2,w=3;hello\x1b\\', True), ('def', False)]),
])
def test_iter_sequences_text_sizing(text, expected_segs):
    assert list(iter_sequences(text)) == expected_segs


@pytest.mark.parametrize('text,start,end,expected', [
    ('\x1b]66;w=3;ABC\x07', 0, 3, '\x1b]66;w=3;ABC\x07'),
    ('\x1b]66;w=3;ABC\x07', 0, 2, '\x1b]66;w=2;AB\x07'),
    ('\x1b]66;w=3;ABC\x07', 1, 3, '\x1b]66;w=2;BC\x07'),
    ('ab\x1b]66;w=2;XY\x07cd', 0, 6, 'ab\x1b]66;w=2;XY\x07cd'),
    ('ab\x1b]66;w=2;XY\x07cd', 0, 3, 'ab\x1b]66;w=1;X\x07'),
    ('ab\x1b]66;w=2;XY\x07cd', 3, 6, '\x1b]66;w=1;Y\x07cd'),
    ('ab\x1b]66;w=2;XY\x07cd', 4, 6, 'cd'),
])
def test_clip_text_sizing_basic(text, start, end, expected):
    """Test basic support of clip() with text sizing sequence."""
    assert repr(clip(text, start, end)) == repr(expected)


@pytest.mark.parametrize('text,start,end,expected', [
    ('\x1b]66;s=2;ABC\x07', 0, 0, ''),
    ('\x1b]66;s=2;ABC\x07', 6, 6, ''),
    ('\x1b]66;s=2;ABC\x07', 0, 2, '\x1b]66;s=2;A\x07'),
    ('\x1b]66;s=2;ABC\x07', 0, 4, '\x1b]66;s=2;AB\x07'),
    ('\x1b]66;s=2;ABC\x07', 0, 6, '\x1b]66;s=2;ABC\x07'),
    ('\x1b]66;s=2;ABC\x07', 2, 6, '\x1b]66;s=2;BC\x07'),
    ('\x1b]66;s=2;ABC\x07', 4, 6, '\x1b]66;s=2;C\x07'),
])
def test_clip_text_sizing_scaled(text, start, end, expected):
    """Test support of clip() with scale=N arguments."""
    assert repr(clip(text, start, end)) == repr(expected)


@pytest.mark.parametrize('text,start,end,expected', [
    #  a   b   c
    # === === ===
    # 012 345 678
    # .
    # ..
    # *a*
    # *a* .
    # ... *b*
    # ... *b* .
    # ... *b* ..
    # ... *b* *c*
    ('\x1b]66;s=3;ABC\x07', 0, 0, ''),
    ('\x1b]66;s=3;ABC\x07', 0, 1, '.'),
    ('\x1b]66;s=3;ABC\x07', 0, 2, '..'),
    ('\x1b]66;s=3;ABC\x07', 0, 3, '\x1b]66;s=3;A\x07'),
    ('\x1b]66;s=3;ABC\x07', 0, 4, '\x1b]66;s=3;A\x07.'),
    ('\x1b]66;s=3;ABC\x07', 0, 5, '\x1b]66;s=3;A\x07..'),
    ('\x1b]66;s=3;ABC\x07', 0, 6, '\x1b]66;s=3;AB\x07'),
    ('\x1b]66;s=3;ABC\x07', 0, 7, '\x1b]66;s=3;AB\x07.'),
    ('\x1b]66;s=3;ABC\x07', 0, 8, '\x1b]66;s=3;AB\x07..'),
    ('\x1b]66;s=3;ABC\x07', 0, 9, '\x1b]66;s=3;ABC\x07'),
    ('\x1b]66;s=3;ABC\x07', 0, 10, '\x1b]66;s=3;ABC\x07'),
    #  a   b
    # === === ===
    # 012 345 678
    #  .             1, 2
    #  ..            1, 3
    #  .. .          1, 4
    #  .. ..         1, 5
    #  .. *b*        1, 6
    #  .. *b* .      1, 7
    #  .. *b* ..     1, 8
    #  .. *b* *c*    1, 9
    ('\x1b]66;s=3;ABC\x07', 1, 1, ''),
    ('\x1b]66;s=3;ABC\x07', 1, 2, '.'),
    ('\x1b]66;s=3;ABC\x07', 1, 3, '..'),
    ('\x1b]66;s=3;ABC\x07', 1, 4, '...'),
    ('\x1b]66;s=3;ABC\x07', 1, 5, '....'),
    ('\x1b]66;s=3;ABC\x07', 1, 6, '..\x1b]66;s=3;B\x07'),
    ('\x1b]66;s=3;ABC\x07', 1, 7, '..\x1b]66;s=3;B\x07.'),
    ('\x1b]66;s=3;ABC\x07', 1, 8, '..\x1b]66;s=3;B\x07..'),
    ('\x1b]66;s=3;ABC\x07', 1, 9, '..\x1b]66;s=3;BC\x07'),
    ('\x1b]66;s=3;ABC\x07', 1, 10, '..\x1b]66;s=3;BC\x07'),
    # two-thirds of string 'A' and half of string 'B' is fillchar
    # ('\x1b]66;s=3;ABC\x07', 2, 4, '..'),
    # half of string 'A' and all of string 'B'
    #  a   b
    # === === ===
    # 012 345 678
    #   .            2, 3
    #   . .          2, 4
    #   . ..         2, 5
    #   . *b*        2, 6
    #   . *b* .      2, 7
    #   . *b* ..     2, 8
    #   . *b* *c*    2, 9
    ('\x1b]66;s=3;ABC\x07', 2, 2, ''),
    ('\x1b]66;s=3;ABC\x07', 2, 3, '.'),
    ('\x1b]66;s=3;ABC\x07', 2, 4, '..'),
    ('\x1b]66;s=3;ABC\x07', 2, 5, '...'),
    ('\x1b]66;s=3;ABC\x07', 2, 6, '.\x1b]66;s=3;B\x07'),
    ('\x1b]66;s=3;ABC\x07', 2, 7, '.\x1b]66;s=3;B\x07.'),
    ('\x1b]66;s=3;ABC\x07', 2, 8, '.\x1b]66;s=3;B\x07..'),
    ('\x1b]66;s=3;ABC\x07', 2, 9, '.\x1b]66;s=3;BC\x07'),
    ('\x1b]66;s=3;ABC\x07', 2, 10, '.\x1b]66;s=3;BC\x07'),
    # and now 3:10, should be easy ...
    ('\x1b]66;s=3;ABC\x07', 3, 3, ''),
    ('\x1b]66;s=3;ABC\x07', 3, 4, '.'),
    ('\x1b]66;s=3;ABC\x07', 3, 5, '..'),
    ('\x1b]66;s=3;ABC\x07', 3, 6, '\x1b]66;s=3;B\x07'),
    ('\x1b]66;s=3;ABC\x07', 3, 7, '\x1b]66;s=3;B\x07.'),
    ('\x1b]66;s=3;ABC\x07', 3, 8, '\x1b]66;s=3;B\x07..'),
    ('\x1b]66;s=3;ABC\x07', 3, 9, '\x1b]66;s=3;BC\x07'),
    ('\x1b]66;s=3;ABC\x07', 3, 10, '\x1b]66;s=3;BC\x07'),
])
def test_clip_text_sizing_scaled_with_fillchar(text, start, end, expected):
    """Test support of clip() with scale=N and fillchar is needed to fill remainder."""
    assert repr(clip(text, start, end, fillchar='.')) == repr(expected)


def test_clip_simple_path_padding():
    """Simple-path clip with w=N larger than text length exercises padding loop."""
    # w=4 but only 1 grapheme 'X' — 3 empty units are padded.
    # Clip window (0, 1) forces partial overlap, triggering
    # _text_sizing_clip_simple's padding branch.
    assert repr(clip('\x1b]66;w=4;X\x07', 0, 1)) == repr('\x1b]66;w=1;X\x07')


@pytest.mark.parametrize('text,start,end,expected', [
    # CR forces painter path; fully-visible text sizing sequence
    ('\r\x1b]66;w=2;XY\x07', 0, 3, '\x1b]66;w=2;XY\x07'),
    # CR painter path, text sizing partially clipped (first unit visible)
    ('\r\x1b]66;w=2;XY\x07', 0, 1, '\x1b]66;w=1;X\x07'),
    # BS forces painter path; text sizing fully visible
    ('ab\b\b\x1b]66;w=2;XY\x07', 0, 4, '\x1b]66;w=2;XY\x07'),
    # Painter path with partial text sizing overlap (exercises _text_sizing_clip_painter)
    ('\ra\x1b]66;s=2;BC\x07', 0, 3, 'a\x1b]66;s=2;B\x07'),
    # Painter path: text sizing scaled partial overlap with fillchar
    ('\r\x1b]66;s=3;ABC\x07', 1, 6, '  \x1b]66;s=3;B\x07'),
    # CSI movement + text sizing fully visible
    ('ab\x1b[2D\x1b]66;w=2;XY\x07', 0, 4, '\x1b]66;w=2;XY\x07'),
    # Painter path: text sizing entirely outside clip window (before start)
    ('\r\x1b]66;w=2;XY\x07', 2, 4, ''),
    # CR + text sizing with auto-width (w=0), partial overlap
    ('\ra\x1b]66;s=2;BC\x07', 0, 5, 'a\x1b]66;s=2;BC\x07'),
    # Painter path: padding when w=N has more units than graphemes
    ('\r\x1b]66;w=3;A\x07', 0, 2, '\x1b]66;w=2;A\x07'),
    # Painter path: text sizing with unit entirely before clip window (skip path)
    ('\r\x1b]66;s=2;ABCD\x07', 4, 8, '\x1b]66;s=2;CD\x07'),
])
def test_clip_text_sizing_painter(text, start, end, expected):
    """Test clip() with text sizing sequences in the cursor-movement (painter) path."""
    assert repr(clip(text, start, end)) == repr(expected)
