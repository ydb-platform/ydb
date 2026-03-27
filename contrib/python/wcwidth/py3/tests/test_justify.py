"""Tests for text justification functions."""
# local
from wcwidth import ljust, rjust, width, center

SGR_RED = '\x1b[31m'
SGR_RESET = '\x1b[0m'
CJK_WORD = '\u4e2d\u6587'
CAFE_COMBINING = 'cafe\u0301'
EMOJI_FAMILY = '\U0001F468\u200D\U0001F469\u200D\U0001F467'


def test_ljust():
    # our ljust() matches standard python ljust() for ascii
    assert ljust('hi', 5) == 'hi   ' == str.ljust('hi', 5)
    assert ljust('', 5) == '     ' == str.ljust('', 5)
    assert ljust('hello', 3) == 'hello' == str.ljust('hello', 3)
    assert ljust('hello', 5) == 'hello' == str.ljust('hello', 5)
    assert ljust('hi', 5, fillchar='-') == 'hi---' == str.ljust('hi', 5, '-')
    # advanced capabilities
    assert ljust('\x1b[31mhi\x1b[0m', 5) == '\x1b[31mhi\x1b[0m   '
    assert ljust('\u4e2d', 4) == '\u4e2d  '
    assert ljust('hi', 5, fillchar='\u00b7') == 'hi\u00b7\u00b7\u00b7'
    assert ljust(CJK_WORD, 8) == CJK_WORD + '    '
    assert width(ljust(CJK_WORD, 8)) == 8
    assert width(ljust(CAFE_COMBINING, 8)) == 8
    assert width(ljust(EMOJI_FAMILY, 6)) == 6
    text = f'{SGR_RED}hi{SGR_RESET}'
    assert len(ljust(text, 6, control_codes='ignore')) - len(SGR_RED) - len(SGR_RESET) == 6


def test_rjust():
    # our rjust() matches standard python rjust() for ascii
    assert rjust('hi', 5) == '   hi' == str.rjust('hi', 5)
    assert rjust('', 5) == '     ' == str.rjust('', 5)
    assert rjust('hello', 3) == 'hello' == str.rjust('hello', 3)
    assert rjust('hello', 5) == 'hello' == str.rjust('hello', 5)
    assert rjust('hi', 5, fillchar='-') == '---hi' == str.rjust('hi', 5, '-')
    # advanced capabilities
    assert rjust('\x1b[31mhi\x1b[0m', 5) == '   \x1b[31mhi\x1b[0m'
    assert rjust('\u4e2d', 4) == '  \u4e2d'
    assert rjust('hi', 5, fillchar='\u00b7') == '\u00b7\u00b7\u00b7hi'
    assert rjust(CJK_WORD, 8) == '    ' + CJK_WORD
    assert width(rjust(CAFE_COMBINING, 8)) == 8
    assert width(rjust(EMOJI_FAMILY, 6)) == 6


def test_center():
    # our center() matches standard python center() for ascii
    assert center('hi', 6) == '  hi  ' == str.center('hi', 6)
    assert center('hi', 5) == '  hi ' == str.center('hi', 5)
    assert center('ab', 7) == '   ab  ' == str.center('ab', 7)
    assert center('', 4) == '    ' == str.center('', 4)
    assert center('hello', 3) == 'hello' == str.center('hello', 3)
    assert center('hello', 5) == 'hello' == str.center('hello', 5)
    assert center('hi', 6, fillchar='-') == '--hi--' == str.center('hi', 6, '-')
    assert center('x', 4) == ' x  ' == str.center('x', 4)
    # advanced capabilities
    assert center('\x1b[31mhi\x1b[0m', 6) == '  \x1b[31mhi\x1b[0m  '
    assert center('\u4e2d', 6) == '  \u4e2d  '
    assert center('hi', 6, fillchar='\u00b7') == '\u00b7\u00b7hi\u00b7\u00b7'
    assert width(center(CJK_WORD, 8)) == 8
    assert width(center(CAFE_COMBINING, 8)) == 8
    assert width(center(EMOJI_FAMILY, 6)) == 6
