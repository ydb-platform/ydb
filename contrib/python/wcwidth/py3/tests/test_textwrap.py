"""Tests for sequence-aware text wrapping functions."""
# std imports
import sys
import platform
import textwrap

# 3rd party
import pytest

# local
from wcwidth import iter_sequences
from wcwidth.textwrap import SequenceTextWrapper, wrap


@pytest.fixture(autouse=True)
def mock_hyperlink_ids(monkeypatch):
    """Mock secrets.token_hex to return predictable IDs for testing."""
    counter = 0

    def fake_token_hex(n):
        nonlocal counter
        counter += 1
        return f'{counter:0{n * 2}x}'

    monkeypatch.setattr('secrets.token_hex', fake_token_hex)


SGR_RED = '\x1b[31m'
SGR_BLUE = '\x1b[34m'
SGR_BOLD = '\x1b[1m'
SGR_RESET = '\x1b[0m'
ATTRS = ('\x1b[31m', '\x1b[34m', '\x1b[4m', '\x1b[7m', '\x1b[41m', '\x1b[37m', '\x1b[107m')

OSC_HYPERLINK = '\x1b]8;;https://example.com\x07link\x1b]8;;\x07'
CSI_CURSOR = '\x1b[5C'
CTRL_BEL = '\x07'

ZWJ = '\u200d'
WOMAN = '\U0001F469'
GIRL = '\U0001F467'
FAMILY_ZWJ = f'{WOMAN}{ZWJ}{WOMAN}{ZWJ}{GIRL}'
SMILEY_VS16 = '\u263a\ufe0f'
ZWJ_FAMILY = '\U0001F469\u200D\U0001F469\u200D\U0001F467\u200D\U0001F466'
CAFE_COMBINING = 'cafe\u0301'
HANGUL_GA = '\u1100\u1161'


def _strip(text):
    return ''.join(seg for seg, is_seq in iter_sequences(text) if not is_seq)


def _adjust_stdlib_result(expected, kwargs):
    """
    Adjust stdlib textwrap result for known bugs in older Python versions.

    CPython #140627: Older versions leave trailing whitespace and preceding all-whitespace lines
    when drop_whitespace=True. Fixed in 3.13.11+, 3.14.2+, and 3.15+. We always strip to normalize
    across versions.
    """
    if not expected:
        return expected
    if kwargs.get('drop_whitespace'):
        # Strip trailing whitespace from each line (old Python bug)
        expected = [line.rstrip() for line in expected]
        # Remove leading all-whitespace lines (old Python bug)
        if expected and not expected[0].strip():
            expected = expected[1:]
            if expected and kwargs.get('subsequent_indent'):
                expected[0] = expected[0][len(kwargs['subsequent_indent']):]
    return expected


def _colorize(text):
    return ''.join(
        ATTRS[idx % len(ATTRS)] + char + SGR_RESET if char not in ' -\t' else char
        for idx, char in enumerate(text)
    )


# Edge cases not covered by stdlib comparison
BASIC_EDGE_CASES = [
    ('', 10, []),
    ('   ', 10, []),
    ('\u5973', 0, ['\u5973']),
]


@pytest.mark.parametrize('text,w,expected', BASIC_EDGE_CASES)
def test_wrap_edge_cases(text, w, expected):
    assert wrap(text, w) == expected


def test_wrap_initial_indent():
    assert wrap('hello world', 10, initial_indent='> ') == ['> hello', 'world']


def test_wrap_drops_trailing_whitespace():
    """Trailing whitespace stripped when drop_whitespace=True (CPython #140627)."""
    result = wrap(' Z! a bc defghij', 3)
    assert result[:3] == [' Z!', 'a', 'bc']


LONG_WORD_CASES = [
    ('abcdefghij', 3, True, ['abc', 'def', 'ghi', 'j']),
    ('abcdefghij', 3, False, ['abcdefghij']),
]


@pytest.mark.parametrize('text,w,break_long,expected', LONG_WORD_CASES)
def test_wrap_long_words(text, w, break_long, expected):
    assert wrap(text, w, break_long_words=break_long) == expected


# Hyphen edge cases for long word breaking
HYPHEN_LONG_WORD_CASES = [
    ('a-b-c-d', 3, True, ['a-', 'b-', 'c-d']),
    ('a-b-c-d', 3, False, ['a-b', '-c-', 'd']),
    ('---', 2, True, ['--', '-']),
    ('a---b', 2, True, ['a-', '--', 'b']),
    # With propagate_sgr=True, SGR continues to next line
    ('a-\x1b[31mb', 2, True, ['a-\x1b[31m\x1b[0m', '\x1b[31mb\x1b[0m']),
]

HYPHEN_LONG_WORD_CASES_NO_PROPAGATE = [
    # With propagate_sgr=False, SGR stays where it is
    ('a-\x1b[31mb', 2, True, ['a-\x1b[31m', 'b']),
]


@pytest.mark.parametrize('text,w,break_hyphens,expected', HYPHEN_LONG_WORD_CASES)
def test_wrap_hyphen_long_words(text, w, break_hyphens, expected):
    assert wrap(text, w, break_on_hyphens=break_hyphens) == expected


@pytest.mark.parametrize('text,w,break_hyphens,expected', HYPHEN_LONG_WORD_CASES_NO_PROPAGATE)
def test_wrap_hyphen_long_words_no_propagate(text, w, break_hyphens, expected):
    assert wrap(text, w, break_on_hyphens=break_hyphens, propagate_sgr=False) == expected


# Comprehensive stdlib compatibility
TEXTWRAP_KWARGS = [
    {'break_long_words': False, 'drop_whitespace': False},
    {'break_long_words': False, 'drop_whitespace': True},
    {'break_long_words': True, 'drop_whitespace': False},
    {'break_long_words': True, 'drop_whitespace': True},
    {'break_long_words': True, 'drop_whitespace': False, 'subsequent_indent': ' '},
    {'break_long_words': True, 'drop_whitespace': True, 'subsequent_indent': ' '},
    {'break_long_words': True, 'drop_whitespace': True, 'break_on_hyphens': True},
    {'break_long_words': True, 'drop_whitespace': True, 'break_on_hyphens': False},
]


@pytest.mark.parametrize('kwargs', TEXTWRAP_KWARGS)
@pytest.mark.parametrize('width', [3, 8, 20, 33])
def test_wrap_matches_stdlib(kwargs, width):
    pgraph = ' Z! a bc defghij klmnopqrstuvw<<>>xyz012345678900 ' * 2
    pgraph_colored = _colorize(pgraph)
    expected = _adjust_stdlib_result(
        textwrap.wrap(pgraph, width=width, **kwargs), kwargs
    )
    wrapper = SequenceTextWrapper(width=width, **kwargs)
    assert wrapper.wrap(pgraph) == expected
    # For colored text, strip sequences
    colored_result = [_strip(line) for line in wrapper.wrap(pgraph_colored)]
    if kwargs.get('drop_whitespace'):
        # normalize trailing whitespace, rstrip when drop_whitespace is True
        # matches CPython #140627 fix
        colored_result = [line.rstrip() for line in colored_result]
    assert colored_result == expected


@pytest.mark.parametrize('kwargs', TEXTWRAP_KWARGS)
@pytest.mark.parametrize('width', [8, 20, 37])
@pytest.mark.parametrize('tabsize', [4, 5, 8])
def test_wrap_tabsize_matches_stdlib(kwargs, width, tabsize):
    tabsize = min(tabsize, width)
    pgraph = ' Z! a bc\t defghij\t kl mnopqrs\ttuvw<<>>xyz012345678900 ' * 2
    expected = _adjust_stdlib_result(
        textwrap.wrap(pgraph, width=width, tabsize=tabsize, **kwargs), kwargs
    )
    wrapper = SequenceTextWrapper(width=width, tabsize=tabsize, **kwargs)
    assert wrapper.wrap(pgraph) == expected


def test_wrap_multiline_matches_stdlib():
    given = '\n' + 32 * 'A' + '\n' + 32 * 'B' + '\n' + 32 * 'C' + '\n\n'
    assert wrap(given, 30) == textwrap.wrap(given, 30)


# Wide characters that exceed width=1 (tests force-grapheme logic)
WIDE_CHAR_WIDTH_1_CASES = [
    ('\u5973', 1, ['\u5973']),
    (ZWJ_FAMILY, 1, [ZWJ_FAMILY]),
    (HANGUL_GA, 1, [HANGUL_GA]),
]


@pytest.mark.parametrize('text,w,expected', WIDE_CHAR_WIDTH_1_CASES)
def test_wrap_wide_char_width_1(text, w, expected):
    assert wrap(text, w) == expected


# Unicode width-aware wrapping
UNICODE_CASES = [
    # CJK (2 cells each)
    ('\u4e2d\u6587\u5b57\u7b26', 4, ['\u4e2d\u6587', '\u5b57\u7b26']),
    ('\u4e2d\u6587\u5b57', 5, ['\u4e2d\u6587', '\u5b57']),
    # Combining characters
    (CAFE_COMBINING + '-latte', 4, ['cafe\u0301', '-lat', 'te']),
    # Emoji (ZWJ, VS16)
    (f'{FAMILY_ZWJ} ab', 4, [FAMILY_ZWJ, 'ab']),
    (f'{SMILEY_VS16} ab', 3, [SMILEY_VS16, 'ab']),
    ('\U0001F469\U0001F467\U0001F466', 4, ['\U0001F469\U0001F467', '\U0001F466']),
]


@pytest.mark.parametrize('text,w,expected', UNICODE_CASES)
def test_wrap_unicode(benchmark, text, w, expected):
    kwargs = {'break_on_hyphens': False} if '-' in text else {}
    result = benchmark(wrap, text, w, **kwargs)
    assert result == expected


# Escape sequence preservation (with propagate_sgr=True default)
SEQUENCE_CASES = [
    # SGR sequences propagated across lines
    (f'{SGR_RED}red{SGR_RESET} blue', 4, [f'{SGR_RED}red{SGR_RESET}', 'blue']),
    # SGR at end of line propagates to next line
    (f'hello{SGR_RED} world', 6, [f'hello{SGR_RED}{SGR_RESET}', f'{SGR_RED}world{SGR_RESET}']),
    # Empty/adjacent sequences - sequences kept (no visible content)
    (f'{SGR_RED}{SGR_RESET}', 10, [f'{SGR_RED}{SGR_RESET}']),
    # Reset clears style, sequences kept with second line
    (f'hello {SGR_RED}{SGR_RESET}world', 6, ['hello', f'{SGR_RED}{SGR_RESET}world']),
    # OSC hyperlinks (with space separator) - not SGR, so preserved as-is
    (f'{OSC_HYPERLINK} text', 5, [OSC_HYPERLINK, 'text']),
    # CSI cursor sequences - not SGR, so preserved as-is
    (f'{CSI_CURSOR}text here', 10, [f'{CSI_CURSOR}text', 'here']),
    # Control characters
    (f'{CTRL_BEL}alert text', 6, [f'{CTRL_BEL}alert', 'text']),
    # Sequences in long word breaking - red starts after 'x', continues across lines
    ('x\x1b[31mabcdefghij\x1b[0m', 3,
     ['x\x1b[31mab\x1b[0m', '\x1b[31mcde\x1b[0m', '\x1b[31mfgh\x1b[0m', '\x1b[31mij\x1b[0m']),
    # Lone ESC - not a valid SGR sequence, stays with preceding text
    ('abc\x1bdefghij', 3, ['abc\x1b', 'def', 'ghi', 'j']),
]

# Old behavior tests (propagate_sgr=False)
SEQUENCE_CASES_NO_PROPAGATE = [
    (f'{SGR_RED}red{SGR_RESET} blue', 4, [f'{SGR_RED}red{SGR_RESET}', 'blue']),
    (f'hello{SGR_RED} world', 6, [f'hello{SGR_RED}', 'world']),
    (f'{SGR_RED}{SGR_RESET}', 10, [f'{SGR_RED}{SGR_RESET}']),
    (f'hello {SGR_RED}{SGR_RESET}world', 6, ['hello', f'{SGR_RED}{SGR_RESET}world']),
    # Sequences preserved where they are, not propagated
    ('x\x1b[31mabcdefghij\x1b[0m', 3, ['x\x1b[31mab', 'cde', 'fgh', 'ij\x1b[0m']),
]


@pytest.mark.parametrize('text,w,expected', SEQUENCE_CASES)
def test_wrap_sequences(benchmark, text, w, expected):
    result = benchmark(wrap, text, w)
    if any('\x1b' in e or '\x00' <= e[0] < '\x20' for e in expected if e):
        assert result == expected
    else:
        assert result == expected


@pytest.mark.parametrize('text,w,expected', SEQUENCE_CASES_NO_PROPAGATE)
def test_wrap_sequences_no_propagate(text, w, expected):
    result = wrap(text, w, propagate_sgr=False)
    assert result == expected


# Mixed: sequences + unicode
MIXED_CASES = [
    (f'{SGR_RED}\u4e2d\u6587{SGR_RESET} ab', 5, [f'{SGR_RED}\u4e2d\u6587{SGR_RESET}', 'ab']),
    (f'{SGR_RED}{FAMILY_ZWJ}{SGR_RESET} ab', 4, [f'{SGR_RED}{FAMILY_ZWJ}{SGR_RESET}', 'ab']),
    (f'{SGR_BOLD}\u4e2d{SGR_RESET}y z', 4, [f'{SGR_BOLD}\u4e2d{SGR_RESET}y', 'z']),
]


@pytest.mark.parametrize('text,w,expected', MIXED_CASES)
def test_wrap_mixed(benchmark, text, w, expected):
    result = benchmark(wrap, text, w)
    assert result == expected


# Tabsize with wide characters - tests column alignment with different cell widths
TABSIZE_WIDE_CASES = [
    # CJK (2 cells) + tab: tabsize=4, '\u4e2d' is 2 cols, tab expands to col 4
    ('\u4e2d\ta b', 6, 4, ['\u4e2d   a', 'b']),
    # CJK + tab with tabsize=8: '\u4e2d' is 2 cols, tab expands to col 8
    ('\u4e2d\ta b', 10, 8, ['\u4e2d       a', 'b']),
    # Emoji + tab (emoji width=2): similar column alignment
    (f'{SMILEY_VS16}\ta b', 6, 4, [f'{SMILEY_VS16}  a', 'b']),
    # Multiple CJK + tab: 4 cols, tab to 4 adds 0, but expand_tabs adds min 1
    ('\u4e2d\u6587\ta', 8, 4, ['\u4e2d\u6587  a']),
    # ASCII + tab + CJK: 'a' is 1 col, tab to 4 (3 spaces), CJK is 2 cols
    ('a\t\u4e2d b', 8, 4, ['a   \u4e2d b']),
]


@pytest.mark.parametrize('text,w,tabsize,expected', TABSIZE_WIDE_CASES)
@pytest.mark.skipif(
    platform.python_implementation() == 'PyPy' and sys.version_info < (3, 9),
    reason='PyPy 3.8 str.expandtabs() counts UTF-8 bytes instead of characters'
)
def test_wrap_tabsize_wide_chars(text, w, tabsize, expected):
    """Verify tabsize respects wide character column positions."""
    assert wrap(text, w, tabsize=tabsize) == expected


OSC_START_ST = '\x1b]8;;http://example.com\x1b\\'
OSC_END_ST = '\x1b]8;;\x1b\\'
OSC_START_BEL = '\x1b]8;;http://example.com\x07'
OSC_END_BEL = '\x1b]8;;\x07'

HYPERLINK_WORD_BOUNDARY_CASES = [
    (   # standard, ST-variant,
        f'{OSC_START_ST}link{OSC_END_ST}more',
        5,
        [f'{OSC_START_ST}link{OSC_END_ST}', 'more'],
    ),
    (   # BEL-variant,
        f'{OSC_START_BEL}link{OSC_END_BEL}more',
        5,
        [f'{OSC_START_BEL}link{OSC_END_BEL}', 'more'],
    ),
    (   # hyperlink breaks after word, 'prefix',
        f'prefix{OSC_START_ST}link{OSC_END_ST}',
        6,
        ['prefix', f'{OSC_START_ST}link{OSC_END_ST}'],
    ),
    (
        f'prefix{OSC_START_BEL}link{OSC_END_BEL}',
        6,
        ['prefix', f'{OSC_START_BEL}link{OSC_END_BEL}'],
    ),
    (   # hyperlink breaks before following, 'suffix',
        f'prefix{OSC_START_ST}link{OSC_END_ST}suffix',
        6,
        ['prefix', f'{OSC_START_ST}link{OSC_END_ST}', 'suffix'],
    ),
    (
        f'prefix{OSC_START_BEL}link{OSC_END_BEL}suffix',
        6,
        ['prefix', f'{OSC_START_BEL}link{OSC_END_BEL}', 'suffix'],
    ),
    (   # hyperlink *surrounded* by SGR attributes
        f'foo {SGR_RED}{OSC_START_ST}link{OSC_END_ST}{SGR_RESET} bar',
        6,
        ['foo', f'{SGR_RED}{OSC_START_ST}link{OSC_END_ST}{SGR_RESET}', 'bar'],
    ),
    (
        f'foo {SGR_RED}{OSC_START_BEL}link{OSC_END_BEL}{SGR_RESET} bar',
        6,
        ['foo', f'{SGR_RED}{OSC_START_BEL}link{OSC_END_BEL}{SGR_RESET}', 'bar'],
    ),
    (   # hyperlink *containing* SGR attributes
        f'foo {OSC_START_ST}{SGR_RED}link{SGR_RESET}{OSC_END_ST} bar',
        6,
        ['foo', f'{OSC_START_ST}{SGR_RED}link{SGR_RESET}{OSC_END_ST}', 'bar'],
    ),
    (
        f'foo {OSC_START_BEL}{SGR_RED}link{SGR_RESET}{OSC_END_BEL} bar',
        6,
        ['foo', f'{OSC_START_BEL}{SGR_RED}link{SGR_RESET}{OSC_END_BEL}', 'bar'],
    ),
    (   # hyperlink with internal space - breaks with id continuation (ST)
        f'Go {OSC_START_ST}Click here{OSC_END_ST} now',
        5,
        [
            'Go',
            '\x1b]8;id=00000001;http://example.com\x1b\\Click\x1b]8;;\x1b\\',
            '\x1b]8;id=00000001;http://example.com\x1b\\here\x1b]8;;\x1b\\',
            'now',
        ],
    ),
    (   # hyperlink with internal space - breaks with id continuation (BEL)
        f'Go {OSC_START_BEL}Click here{OSC_END_BEL} now',
        5,
        [
            'Go',
            '\x1b]8;id=00000001;http://example.com\x07Click\x1b]8;;\x07',
            '\x1b]8;id=00000001;http://example.com\x07here\x1b]8;;\x07',
            'now',
        ],
    ),
    (   # hyperlink with existing id= parameter is preserved
        '\x1b]8;id=my-link;http://example.com\x1b\\Click here\x1b]8;;\x1b\\',
        6,
        [
            '\x1b]8;id=my-link;http://example.com\x1b\\Click\x1b]8;;\x1b\\',
            '\x1b]8;id=my-link;http://example.com\x1b\\here\x1b]8;;\x1b\\',
        ],
    ),
    (   # hyperlink spanning 3+ lines
        f'{OSC_START_ST}one two three{OSC_END_ST}',
        5,
        [
            '\x1b]8;id=00000001;http://example.com\x1b\\one\x1b]8;;\x1b\\',
            '\x1b]8;id=00000001;http://example.com\x1b\\two\x1b]8;;\x1b\\',
            '\x1b]8;id=00000001;http://example.com\x1b\\three\x1b]8;;\x1b\\',
        ],
    ),
    (   # multiple hyperlinks in same text
        f'{OSC_START_ST}ab cd{OSC_END_ST} {OSC_START_BEL}ef gh{OSC_END_BEL}',
        4,
        [
            '\x1b]8;id=00000001;http://example.com\x1b\\ab\x1b]8;;\x1b\\',
            '\x1b]8;id=00000001;http://example.com\x1b\\cd\x1b]8;;\x1b\\',
            '\x1b]8;id=00000002;http://example.com\x07ef\x1b]8;;\x07',
            '\x1b]8;id=00000002;http://example.com\x07gh\x1b]8;;\x07',
        ],
    ),
    (   # long word inside hyperlink forces character-level breaking
        f'{OSC_START_ST}abcdefgh{OSC_END_ST}',
        3,
        [
            '\x1b]8;id=00000001;http://example.com\x1b\\abc\x1b]8;;\x1b\\',
            '\x1b]8;id=00000001;http://example.com\x1b\\def\x1b]8;;\x1b\\',
            '\x1b]8;id=00000001;http://example.com\x1b\\gh\x1b]8;;\x1b\\',
        ],
    ),
    (   # params with other keys but no id - id is prepended, other params preserved
        '\x1b]8;foo=bar;http://example.com\x1b\\Click here\x1b]8;;\x1b\\',
        6,
        [
            '\x1b]8;id=00000001:foo=bar;http://example.com\x1b\\Click\x1b]8;;\x1b\\',
            '\x1b]8;id=00000001:foo=bar;http://example.com\x1b\\here\x1b]8;;\x1b\\',
        ],
    ),
    (   # id not at start of params (junk:id=given) - full params preserved
        '\x1b]8;foo=bar:id=mylink;http://example.com\x1b\\Click here\x1b]8;;\x1b\\',
        6,
        [
            '\x1b]8;foo=bar:id=mylink;http://example.com\x1b\\Click\x1b]8;;\x1b\\',
            '\x1b]8;foo=bar:id=mylink;http://example.com\x1b\\here\x1b]8;;\x1b\\',
        ],
    ),
]


@pytest.mark.parametrize('text,w,expected', HYPERLINK_WORD_BOUNDARY_CASES)
def test_wrap_hyperlink_word_boundary(text, w, expected):
    """OSC hyperlink sequences should act as word boundaries."""
    result = wrap(text, w)
    assert result == expected
