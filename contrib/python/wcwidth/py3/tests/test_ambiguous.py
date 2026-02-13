"""Tests for ambiguous_width parameter."""
# 3rd party
import pytest

# local
import wcwidth

AMBIGUOUS_CHARS = [
    ('\u00a1', 'INVERTED_EXCLAMATION'),
    ('\u00a7', 'SECTION_SIGN'),
    ('\u00b0', 'DEGREE_SIGN'),
    ('\u00b1', 'PLUS_MINUS'),
    ('\u00d7', 'MULTIPLICATION'),
    ('\u00f7', 'DIVISION'),
    ('\u2460', 'CIRCLED_ONE'),
    ('\u2500', 'BOX_HORIZONTAL'),
    ('\u25a0', 'BLACK_SQUARE'),
    ('\u2605', 'BLACK_STAR'),
]


@pytest.mark.parametrize('char,name', AMBIGUOUS_CHARS)
def test_wcwidth_ambiguous_default(char, name):
    """Ambiguous characters return width 1 by default."""
    assert wcwidth.wcwidth(char) == 1


@pytest.mark.parametrize('char,name', AMBIGUOUS_CHARS)
def test_wcwidth_ambiguous_wide(char, name):
    """Ambiguous characters return width 2 when ambiguous_width=2."""
    assert wcwidth.wcwidth(char, ambiguous_width=2) == 2


def test_wcswidth_mixed_ambiguous_and_wide():
    """Mixed CJK and ambiguous characters."""
    text = '\u4e2d\u00b1'  # CJK (wide=2) + PLUS-MINUS (ambiguous)
    assert wcwidth.wcswidth(text) == 3
    assert wcwidth.wcswidth(text, ambiguous_width=2) == 4


def test_width_ambiguous():
    """Width() respects ambiguous_width parameter."""
    assert wcwidth.width('\u00b1') == 1
    assert wcwidth.width('\u00b1', ambiguous_width=2) == 2


def test_ljust_ambiguous():
    """Ljust respects ambiguous_width parameter."""
    text = '\u00b1'
    assert wcwidth.ljust(text, 4) == '\u00b1   '
    assert wcwidth.ljust(text, 4, ambiguous_width=2) == '\u00b1  '


def test_rjust_ambiguous():
    """Rjust respects ambiguous_width parameter."""
    text = '\u00b1'
    assert wcwidth.rjust(text, 4) == '   \u00b1'
    assert wcwidth.rjust(text, 4, ambiguous_width=2) == '  \u00b1'


def test_center_ambiguous():
    """Center respects ambiguous_width parameter."""
    text = '\u00b1'
    assert wcwidth.center(text, 5) == '  \u00b1  '
    assert wcwidth.center(text, 6, ambiguous_width=2) == '  \u00b1  '


def test_wrap_ambiguous():
    """Wrap respects ambiguous_width parameter."""
    text = '\u00b1' * 5  # 5 ambiguous characters
    assert wcwidth.wrap(text, 4) == ['\u00b1\u00b1\u00b1\u00b1', '\u00b1']
    assert wcwidth.wrap(text, 4, ambiguous_width=2) == ['\u00b1\u00b1', '\u00b1\u00b1', '\u00b1']


def test_wide_not_affected_by_ambiguous():
    """Wide characters remain wide regardless of ambiguous_width."""
    cjk = '\u4e2d'  # CJK character (always wide)
    assert wcwidth.wcwidth(cjk) == 2
    assert wcwidth.wcwidth(cjk, ambiguous_width=2) == 2
    assert wcwidth.wcwidth(cjk, ambiguous_width=1) == 2
