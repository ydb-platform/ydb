"""
Tests for clip()'s overtyping (painter) path.

The painter algorithm is used when the text contains cursor movement sequences
(CSI n C/D, backspace, carriage return, HPA) that require column-level tracking
to determine the final visible output.  Auto-detection of the overtyping path
happens in clip() via the presence of \\x08, \\r, or horizontal cursor movement
escape sequences, or can be forced with ``overtyping=True``.

These tests codify expected visible results when cursor movement sequences
affect horizontal positions.
"""

# 3rd party
import pytest

# local
from wcwidth import clip


@pytest.mark.parametrize("text,start,end,kwargs,expected", [
    # Cursor-right introduces a gap that should be filled with spaces
    ("hello\x1b[10Cworld", 0, 10, {}, "hello" + " " * 5),
    # Clipping just the initial region ignores the later rightward write
    ("hello\x1b[10Cworld", 0, 5, {}, "hello"),
    # Cursor-left overwrites previous characters
    ("hello\x1b[2DXY", 0, 5, {}, "helXY"),
    # Cursor-left overwrites entire visible token
    ("abc\x1b[3DXY", 0, 5, {}, "XYc"),
    # Cursor-left at column 0 (prev_col not > col, no overwrite)
    ("\x1b[2Dhi", 0, 2, {}, "hi"),
    # Cursor-left with no visible tokens emitted
    ("\x1b[5C\x1b[2Dhi", 5, 7, {}, ""),
    # Cursor-left overwrites text, seq tokens preserve column spatial order
    ("ab\x1b]8;;http://example.com\x07\x1b[2Dcd", 0, 4, {}, "cd\x1b]8;;http://example.com\x07"),
    # Cursor-left into wide char twice, second time on empty token triggers i < 0 break
    ("中\x1b[D\x1b[Da", 0, 4, {}, "a "),
    ('ab\x1b[5Ccd', 0, 4, {}, 'ab  '),
    ('abcde\x1b[2Df', 0, 6, {}, 'abcfe'),
    ('hello\x1b[5Dw', 0, 5, {}, 'wello'),
    ('ab\x1b[10Ccd', 0, 4, {}, 'ab  '),
    ('XY\x1b[Czy', 0, 4, {}, 'XY z'),
    ('XY\x1b[Czy', 0, 5, {}, 'XY zy'),
    ('XY\x1b[Czy', 1, 3, {}, 'Y '),
    ('XY\x1b[Czy', 1, 4, {}, 'Y z'),
    ('LOL\x1b[5Clol', 0, 12, {}, 'LOL     lol'),
    ('LOL\x1b[5Clol', 1, 11, {}, 'OL     lol'),
    ('LOL\x1b[5Clol', 2, 11, {}, 'L     lol'),
    ('LOL\x1b[5Clol', 3, 11, {}, '     lol'),
    ('LOL\x1b[5Clol', 4, 11, {}, '    lol'),
    ('LOL\x1b[5Clol', 5, 11, {}, '   lol'),
    ('LOL\x1b[5Clol', 6, 11, {}, '  lol'),
    ('LOL\x1b[5Clol', 7, 11, {}, ' lol'),
    ('LOL\x1b[5Clol', 8, 11, {}, 'lol'),
    ('LOL\x1b[5Clol', 9, 11, {}, 'ol'),
    # SGR + cursor movement: SGR state update in painter path (line 245)
    ('\x1b[31mab\x1b[2Dcd', 0, 4, {}, '\x1b[31mcd\x1b[0m'),
    # Tab tabsize=0 in painter path (line 272->280 else branch)
    ('ab\x1b[2D\tcd', 0, 4, {'tabsize': 0}, '\tcd'),
    # Zero-width grapheme outside clip window in painter (line 290->301)
    ('\x1b[2D\u0301hello', 1, 4, {}, 'ell'),
    # Wide char partially clipped in painter (lines 298-299)
    ('ab\x1b[2D中d', 1, 4, {}, ' d'),
    # walk_col >= end in painter reconstruction (327->328)
    ('hello\x1b[2Dxy', 0, 3, {}, 'hel'),
    # Hole fillchar in painter reconstruction (345->346)
    ('\x1b[5Chi', 0, 7, {}, '     hi'),
    # Trailing sequences stored at columns after col_limit (352, 354->355, 355->356)
    ('abc\x1b[2D', 0, 2, {}, 'ab'),
    # Bare ESC not part of any sequence, pass through in painter path (239->240)
    ('a\x1bb\x1b[2Dc', 0, 3, {}, 'c\x1bb'),
    # Tab with tabsize>0 in painter; `b` falls at col 4, inside (0,5) (277->284, 278->279, 278->280)
    ('\x1b[2Da\tb', 0, 5, {'tabsize': 4}, 'a   b'),
    # propagate_sgr=False in painter path (225->226)
    ('ab\x1b[2Dcd', 0, 4, {'propagate_sgr': False}, 'cd'),
    # Non-SGR sequence before any visible text in painter (225->226 True)
    ('\x1b]8;;http://example.com\x07ab\x1b[2Dcd', 0, 4, {}, '\x1b]8;;http://example.com\x07cd'),
    # Bare ESC at end of text in painter (239->240)
    ('ab\x1b[2D\x1b', 0, 2, {}, '\x1bab'),
    # Wide char overwritten from right side (212 orphan fixup)
    ('a中\x1b[Db', 0, 4, {}, 'a b'),
    # Tab expansion with col+=1 not inside clip window (277->279, 293)
    ('\x1b[2Ca\tb', 2, 4, {'tabsize': 8}, 'a '),
    # CR: carriage return resets column to 0, overwriting earlier cells
    ('aaa\r\r\rxxx', 0, 4, {}, 'xxx'),
    ('abc\rXY', 0, 5, {}, 'XYc'),
    ('hello\rworld', 0, 5, {}, 'world'),
    # CR moves back to column 0 then writes within clip window
    ('abc\rde', 1, 3, {}, 'ec'),
    # BS: backspace overwrites previous character
    ('abc\bde', 0, 5, {}, 'abde'),
    ('abc\b\bXY', 0, 5, {}, 'aXY'),
    ('ab\b\b\bXY', 0, 4, {}, 'XY'),
    # HPA: horizontal position absolute (CSI n G)
    ('abc\x1b[GXY', 0, 5, {}, 'XYc'),
    ('abc\x1b[2GXY', 0, 5, {}, 'aXY'),
    ('abc\x1b[5GXY', 0, 7, {}, 'abc XY'),
    ('abc\x1b[5GXY', 0, 5, {}, 'abc X'),
    ('\x1b[5GXY', 3, 7, {}, ' XY'),
    # HPA no-param inside clip window
    ('abc\x1b[GXY', 1, 4, {}, 'Yc'),
    # walk_col >= end with sequences at column == end (line 351)
    ('\x1b[5C\x1b]8;;http://example.com\x07', 0, 5, {'propagate_sgr': False}, '     \x1b]8;;http://example.com\x07'),
    # Trailing sequences past col_limit (line 374)
    ('\x1b[5C\x1b]8;;http://example.com\x07', 0, 3, {'propagate_sgr': False}, '   \x1b]8;;http://example.com\x07'),
    # Lone ESC as first visible thing in painter (captured_style = current_style, line 398)
    ('\x1b[D\x1b\x1bXy', 0, 3, {}, '\x1b\x1bXy'),
    # Hyperlink VISIBLE after captured_style already set
    ('a\x1b[C\x1b]8;;http://x\x07hi\x1b]8;;\x07', 0, 5, {}, 'a \x1b]8;;http://x\x07hi\x1b]8;;\x07'),
    # Tab with tabsize=0 as first visible thing in painter
    ('\x1b[D\tab', 0, 2, {'tabsize': 0}, '\tab'),
    # Zero-width grapheme as first visible thing in painter
    ('\x1b[D\u0301x', 0, 3, {}, '\u0301x'),
    # Generic escape sequence as first visible in painter
    ('\x1b[D\x1b[Hxy', 0, 3, {}, '\x1b[Hxy'),
])
def test_clip_cursor_sequences_expected_behaviour(text, start, end, kwargs, expected):
    """Verify clip() output matches terminal-visible columns after cursor moves."""
    result = clip(text, start, end, **kwargs)
    assert repr(result) == repr(expected)


def test_clip_cursor_left_strict_out_of_bounds():
    """Clip() with control_codes='strict' raises on cursor-left beyond string start."""
    with pytest.raises(ValueError, match='Cursor left movement'):
        clip('a\x1b[5Da', 0, 1, control_codes='strict')


def test_clip_cursor_left_strict_out_of_bounds_painter():
    """Clip() strict-mode raises on cursor-left beyond start in painter path."""
    with pytest.raises(ValueError, match='Cursor left movement'):
        clip('\x1b[2Dab', 0, 2, control_codes='strict')


def test_clip_cursor_left_out_of_bounds_parse_no_raise():
    """Clip() parse mode silently clamps cursor-left beyond start."""
    assert clip('a\x1b[5Da', 0, 1) == 'a'
    assert clip('ab\x1b[99Dcd', 0, 4) == 'cd'


def test_clip_strict_cr_allowed():
    """Carriage return is allowed in strict mode (text begins at column 0)."""
    assert clip('hello\rworld', 0, 5, control_codes='strict') == 'world'


def test_clip_strict_hpa_allowed():
    """HPA is allowed in strict mode (text begins at column 0)."""
    assert clip('abc\x1b[5Gde', 0, 10, control_codes='strict') == 'abc de'


def test_clip_strict_cursor_left_allowed():
    """Cursor-left within bounds is allowed in strict mode."""
    assert clip('hello\x1b[2Dxy', 0, 5, control_codes='strict') == 'helxy'


def test_clip_strict_indeterminate_sequence_painter():
    """Clip() strict-mode raises on indeterminate sequence in painter path."""
    with pytest.raises(ValueError, match='Indeterminate cursor sequence'):
        clip('a\x1b[D\x1b[Hb', 0, 3, control_codes='strict')
