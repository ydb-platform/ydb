"""Tests for OSC 8 hyperlink parsing."""

# 3rd party
import pytest

# local
from wcwidth.hyperlink import Hyperlink, HyperlinkParams

PARAMS_PARSE_VALID = [
    ('\x1b]8;;http://example.com\x07', 'http://example.com', '', '\x07'),
    ('\x1b]8;id=a;http://example.com\x1b\\', 'http://example.com', 'id=a', '\x1b\\'),
]


@pytest.mark.parametrize('seq,url,params,term', PARAMS_PARSE_VALID)
def test_hyperlinkparams_parse_valid(seq, url, params, term):
    """Parse a valid OSC 8 open sequence."""
    result = HyperlinkParams.parse(seq)
    assert result is not None
    assert result.url == url
    assert result.params == params
    assert result.terminator == term


@pytest.mark.parametrize('seq', [
    'not an escape',
    '\x1b[31m',
    '',
])
def test_hyperlinkparams_parse_invalid(seq):
    """Parse an invalid/non-OSC-8 sequence returns None."""
    assert HyperlinkParams.parse(seq) is None


def test_hyperlinkparams_make_open():
    assert HyperlinkParams(url='http://example.com', params='id=a', terminator='\x07').make_open() == '\x1b]8;id=a;http://example.com\x07'


def test_hyperlinkparams_make_close():
    assert HyperlinkParams(url='http://example.com', terminator='\x07').make_close() == '\x1b]8;;\x07'


_HL = '\x1b]8;;http://example.com\x07Hello\x1b]8;;\x07'


def test_hyperlink_parse_valid():
    hl = Hyperlink.parse(_HL)
    assert hl is not None
    assert hl.text == 'Hello'
    assert hl.params.url == 'http://example.com'


@pytest.mark.parametrize('text,start', [
    ('Hello world', 0),
    ('\x1b[31mHello\x1b[0m', 0),   # SGR, not OSC 8
    ('\x1b]8;;http://example.com\x07Hello', 0),  # open without close
])
def test_hyperlink_parse_returns_none(text, start):
    assert Hyperlink.parse(text, start) is None


def test_hyperlink_find_close_not_found():
    assert Hyperlink.find_close('no escape here', 0) == (-1, -1)


def test_hyperlink_make_sequence():
    hl = Hyperlink.parse(_HL)
    assert hl is not None
    assert hl.make_sequence() == _HL


def test_hyperlink_display_width():
    hl = Hyperlink.parse(_HL)
    assert hl is not None
    assert hl.display_width() == 5
