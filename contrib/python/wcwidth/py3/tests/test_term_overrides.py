"""Tests for terminal-specific width overrides."""
# std imports
import os

# 3rd party
import pytest

# local
import wcwidth
import wcwidth.table_grapheme_overrides as grapheme_overrides
from wcwidth._constants import (_EMPTY_OVERRIDES,
                                _merge_ranges,
                                resolve_terminal,
                                get_term_overrides,
                                list_term_programs)
from wcwidth.table_overrides import VS15_OVERRIDES


def test_resolve_terminal_aliases():
    """resolve_terminal maps known aliases to canonical names."""
    assert resolve_terminal('kitty') == 'kitty'
    assert resolve_terminal('vscode') == 'xterm.js'
    assert resolve_terminal('urxvt') == 'urxvt'


def test_resolve_terminal_unknown():
    """resolve_terminal returns None for unrecognized names and empty string."""
    assert resolve_terminal('nonexistent') is None
    assert resolve_terminal('') is None


def test_resolve_terminal_auto_detect():
    """resolve_terminal=True reads TERM_PROGRAM env var, falling back to TERM."""
    saved_tprog = os.environ.get('TERM_PROGRAM')
    saved_term = os.environ.get('TERM')
    try:
        for var in ('TERM_PROGRAM', 'TERM'):
            os.environ.pop(var, None)
        resolve_terminal.cache_clear()
        assert resolve_terminal(True) is None
        os.environ['TERM_PROGRAM'] = 'kitty'
        resolve_terminal.cache_clear()
        assert resolve_terminal(True) == 'kitty'
    finally:
        for var, saved in (('TERM_PROGRAM', saved_tprog), ('TERM', saved_term)):
            if saved is not None:
                os.environ[var] = saved
            else:
                os.environ.pop(var, None)
        resolve_terminal.cache_clear()


def test_wcswidth_no_override():
    """Wcswidth works normally without term_program or with empty string."""
    assert wcwidth.wcswidth('hello') == 5
    assert wcwidth.wcstwidth('hello', term_program='') == 5


@pytest.mark.parametrize('char,expected_default,expected_vte', [
    ('\u2630', 2, 1),
    ('\U0001f1e6', 2, 1),
])
def test_wcswidth_vte_override(char, expected_default, expected_vte):
    """VTE override narrows wide characters."""
    assert wcwidth.wcswidth(char) == expected_default
    assert wcwidth.wcstwidth(char, term_program='VTE') == expected_vte


@pytest.mark.parametrize('text,kwargs,expected', [
    ('\u2630', {'term_program': 'VTE'}, 1),
    ('\u2630', {'term_program': 'kitty'}, 2),
    ('\x1b[31m\u2630\u2631\x1b[0m', {'term_program': 'VTE'}, 2),
    ('\u2630\u2631', {'control_codes': 'ignore', 'term_program': 'VTE'}, 2),
])
def test_width_vte_override(text, kwargs, expected):
    """Width() applies VTE overrides with and without control codes."""
    assert wcwidth.width(text, **kwargs) == expected


def test_vs16_override_basic():
    """VS16 override is applied to heart emoji variation."""
    heart_vs16 = '\u2764\ufe0f'
    assert wcwidth.wcswidth(heart_vs16) == 2
    assert wcwidth.wcstwidth(heart_vs16, term_program='VTE') == 1
    assert wcwidth.width(heart_vs16, term_program='VTE') == 1


def test_vs16_libvterm_no_override():
    """Libvterm is not a known terminal; falls back to spec VS16 (returns 2)."""
    assert wcwidth.wcstwidth('\u23ed\ufe0f', term_program='libvterm') == 2
    assert wcwidth.width('\u23ed\ufe0f', term_program='libvterm') == 2


def test_wcwidth_unchanged():
    """Wcwidth() does not accept term_program parameter."""
    assert wcwidth.wcwidth('\u2630') == 2
    with pytest.raises(TypeError):
        wcwidth.wcwidth('\u2630', term_program='VTE')  # type: ignore[call-arg]


def test_wcstwidth_term_program():
    """Empty term_program disables override lookup."""
    assert wcwidth.wcstwidth('\u2630', term_program='') == 2
    assert wcwidth.wcstwidth('\u2630', term_program='VTE') == 1
    assert wcwidth.wcswidth('\u2630') == 2
    assert wcwidth.width('\u2630') == 2


def test_wcswidth_ascii_unchanged():
    """ASCII text is unaffected by terminal overrides."""
    assert wcwidth.wcstwidth('hello world', term_program='VTE') == 11
    assert wcwidth.wcstwidth('hello world', term_program='kitty') == 11


def test_vs15_standalone():
    """VS15 (U+FE0E) alone measures as width 0."""
    assert wcwidth.wcswidth('\ufe0e') == 0
    assert wcwidth.wcstwidth('\ufe0e', term_program='VTE') == 0


def test_vs15_no_override():
    """VS15 after a character not in any override table has no effect."""
    base = '\u2630'
    assert wcwidth.wcswidth(base + '\ufe0e') == wcwidth.wcswidth(base)
    assert wcwidth.wcstwidth(base + '\ufe0e', term_program='kitty') == wcwidth.wcstwidth(base)


def test_vs15_wider_override_unchanged():
    """VS15 narrows by default; VTE wider override restores width 2."""
    assert wcwidth.wcswidth('\u231a') == 2
    assert wcwidth.wcswidth('\u231a\ufe0e') == 1
    assert wcwidth.wcstwidth('\u231a\ufe0e', term_program='VTE') == 2
    assert wcwidth.width('\u231a\ufe0e') == 1
    assert wcwidth.width('\u231a\ufe0e', term_program='VTE') == 2


def test_grapheme_override_zwj_not_in_table():
    """ZWJ cluster not in override table falls through without error."""
    assert wcwidth.wcstwidth('😀\u200d😀', term_program='VTE') == 2
    assert wcwidth.width('😀\u200d😀', term_program='VTE') == 2


def test_width_vs16_zwj_transition():
    """Width() VS16-applied state transitions to ZWJ_BLOCKED."""
    # smiley gets VS16'd (base_state=VS16_APPLIED), then ZWJ_BLOCKED
    assert wcwidth.width('\u263a\ufe0f\u200da') == 2


def test_width_vs15_override():
    """Width() with VS15 and terminal override."""
    assert wcwidth.width('\u231a\ufe0e', term_program='VTE') == 2
    assert wcwidth.width('\u2630\ufe0e', term_program='VTE') == 1


@pytest.mark.parametrize('term_program,expected', [
    (False, 2),
    ('', 2),
    ('nonexistent', 2),
    ('alacritty', 4),
])
def test_grapheme_override_wcswidth_family(term_program, expected):
    """Wcswidth ZWJ grapheme override applied only for recognized terminals with overrides."""
    family = '\U0001F468\u200D\U0001F466'
    assert wcwidth.wcstwidth(family, term_program=term_program) == expected


def test_grapheme_override_multi_zwj_alacritty():
    """Wcswidth handles multi-ZWJ grapheme override."""
    family4 = '\U0001F468\u200D\U0001F469\u200D\U0001F467\u200D\U0001F466'
    default = wcwidth.wcswidth(family4)
    override = wcwidth.wcstwidth(family4, term_program='alacritty')
    assert default == 2
    assert override == 8


@pytest.mark.parametrize('func,kwargs', [
    (wcwidth.width, {'term_program': 'alacritty'}),
    (wcwidth.width, {'control_codes': 'ignore', 'term_program': 'alacritty'}),
])
def test_grapheme_override_width_alacritty(func, kwargs):
    """Width() applies ZWJ grapheme override for alacritty."""
    family = '\U0001F468\u200D\U0001F466'
    assert func(family, **kwargs) == 4


def test_grapheme_override_ascii_unchanged():
    """ASCII text is unaffected by grapheme overrides."""
    assert wcwidth.wcstwidth('hello', term_program='alacritty') == 5
    assert wcwidth.width('hello', term_program='alacritty') == 5


def test_grapheme_override_zwj_at_end():
    """ZWJ at end of string does not trigger override scan."""
    text = '\U0001F468\u200D'
    assert wcwidth.wcstwidth(text, term_program='alacritty') == 2


def test_grapheme_override_fitzpatrick():
    """Fitzpatrick modifier between base and ZWJ handled correctly."""
    text = '\u26F9\U0001F3FB\u200D\u2640\uFE0F'
    assert wcwidth.wcstwidth(text, term_program='alacritty') == 4


def test_list_term_programs():
    """list_term_programs returns known terminals."""
    terms = list_term_programs()
    assert isinstance(terms, tuple)
    assert 'alacritty' in terms
    assert 'vte' in terms
    assert 'xterm.js' in terms
    assert 'nonexistent' not in terms


def test_grapheme_override_invalid_term_names():
    """Grapheme override get() returns empty dict for invalid names."""
    assert grapheme_overrides.get(None) == {}
    assert grapheme_overrides.get('__init__') == {}
    assert grapheme_overrides.get('') == {}
    assert grapheme_overrides.get('../../etc') == {}


def test_grapheme_override_zwj_no_extpict_base():
    """ZWJ after non-ExtPict base does not trigger override scan."""
    text = 'a\u200D\u200D'
    assert wcwidth.wcstwidth(text, term_program='alacritty') == 1


@pytest.mark.parametrize('text,term,expected', [
    ('👨\u200d👦x', 'alacritty', 5),
    ('👨\u200da', 'alacritty', 2),
    ('👨\u200da', False, 2),
])
def test_grapheme_override_scanner_edges(text, term, expected):
    """Scanner edge cases for ZWJ chains."""
    assert wcwidth.wcstwidth(text, term_program=term) == expected


def test_grapheme_override_missing_module():
    """
    Returns None when registry hash points to missing _known_ module.

    This can occur during a program re-install when the registry and _known_* files are out of sync
    (filesystem vs. in-memory copy differ). The ImportError is caught so measurement can continue
    gracefully without per-terminal grapheme overrides.
    """
    saved = grapheme_overrides._REGISTRY.get('putty')
    try:
        grapheme_overrides._REGISTRY['putty'] = 'deadbeef'
        grapheme_overrides.get.cache_clear()
        assert grapheme_overrides.get('putty') == {}
    finally:
        grapheme_overrides._REGISTRY['putty'] = saved
        grapheme_overrides.get.cache_clear()


def test_no_terminal_has_vs15_narrower_overrides():
    """No terminal narrows VS15."""

    # VS15 (text presentation) narrows a wide character to width 1. There is no width below 1 !
    narrower_terminals = {
        term: data['narrower']
        for term, data in VS15_OVERRIDES.items()
        if data.get('narrower')
    }
    assert not narrower_terminals, (
        f'Unexpected: terminal(s) with VS15 narrower overrides detected: '
        f'{sorted(narrower_terminals)}.\n'
        f'VS15 cannot narrow a character below width 1. '
        f'This may indicate a ucs-detect measurement error or an unexpected terminal behavior.'
    )


def test_list_term_programs_includes_xterm():
    """Xterm is a recognized terminal program for explicit use."""
    assert 'xterm' in list_term_programs()


def test_resolve_terminal_xterm_explicit():
    """resolve_terminal returns 'xterm' when passed explicitly."""
    assert resolve_terminal('xterm') == 'xterm'


@pytest.mark.parametrize('env_var', ['TERM', 'TERM_PROGRAM'])
def test_resolve_terminal_xterm_auto_detected(env_var):
    """resolve_terminal returns 'xterm' for xterm via auto-detection from env."""
    os.environ[env_var] = 'xterm'
    resolve_terminal.cache_clear()
    assert resolve_terminal(True) == 'xterm'


@pytest.mark.parametrize('func,text,expected_default,expected_xterm', [
    (wcwidth.wcstwidth, '\U0001f1e6', 2, 1),
    (wcwidth.width, '\U0001f1e6', 2, 1),
    (wcwidth.wcstwidth, '\u231a\ufe0e', 1, 2),
    (wcwidth.width, '\u231a\ufe0e', 1, 2),
])
def test_xterm_overrides_applied(func, text, expected_default, expected_xterm):
    """Xterm overrides are applied when term_program='xterm' is explicit."""
    assert func(text) == expected_default
    assert func(text, term_program='xterm') == expected_xterm


@pytest.mark.parametrize('func', [wcwidth.wcswidth, wcwidth.width])
def test_zwj_fallthrough_resets_base_for_vs16(func):
    """VS16 after ZWJ-skipped char does not connect to stale base (before fix, VS16 narrowed the
    watch)."""
    assert func('\u231a\u200d\u23f0\ufe0f') == 2


@pytest.mark.parametrize('func', [wcwidth.wcswidth, wcwidth.width])
def test_zwj_fallthrough_resets_base_for_vs15(func):
    """VS15 after ZWJ-skipped char does not connect to stale base (before fix, VS15 narrowed the
    watch)."""
    assert func('\u231a\u200d\u23f0\ufe0e') == 2


@pytest.mark.parametrize('func,text,dest_width,expected', [
    (wcwidth.ljust, '\u2630', 4, '\u2630   '),
    (wcwidth.rjust, '\u2630', 4, '   \u2630'),
    (wcwidth.center, '\u2630', 5, '  \u2630  '),
])
def test_align_term_program_vte(func, text, dest_width, expected):
    """Ljust/rjust/center pass term_program through to width()."""
    assert func(text, dest_width, term_program='VTE') == expected


def test_clip_term_program_vte():
    """Clip() passes term_program through to width()."""
    result = wcwidth.clip('\u2630\u2631', 0, 1, term_program='VTE')
    assert result == '\u2630'


def test_wrap_term_program_vte():
    """Wrap() passes term_program through to width()."""
    result = wcwidth.wrap('\u2630\u2631', width=2, term_program='VTE')
    assert result == ['\u2630\u2631']


@pytest.mark.parametrize('termenv,expected', [
    ({'TERM': 'xterm-kitty'}, 'kitty'),
    ({'TERM_PROGRAM': '', 'TERM': 'xterm-kitty'}, 'kitty'),
])
def test_resolve_terminal_from_env(termenv, expected):
    """resolve_terminal reads TERM when TERM_PROGRAM is unset or empty."""
    for var in ('TERM_PROGRAM', 'TERM'):
        os.environ.pop(var, None)
    os.environ.update(termenv)
    resolve_terminal.cache_clear()
    assert resolve_terminal(True) == expected


@pytest.mark.parametrize('args,expected', [
    ((), ()),
    ((((1, 5),),), ((1, 5),)),
    ((((1, 3),), ((6, 8),)), ((1, 3), (6, 8))),
    ((((1, 5),), ((4, 8),)), ((1, 8),)),
])
def test_merge_ranges(args, expected):
    """_merge_ranges merges sorted range tuples."""
    assert _merge_ranges(*args) == expected


def test_sfz_override_foot():
    """Foot narrows Fitzpatrick modifiers."""
    assert wcwidth.wcswidth('\U0001F3FB') == 2
    assert wcwidth.wcstwidth('\U0001F3FB', term_program='foot') == 1


@pytest.mark.parametrize('term_program,expected', [
    ('kitty', 0),
    ('bobcat', 0),
    (False, 2),
])
def test_sfz_zeroer(term_program, expected):
    """Standalone Fitzpatrick modifiers zeroed per terminal."""
    assert wcwidth.wcswidth('\U0001F3FB') == 2
    assert wcwidth.wcstwidth('\U0001F3FB', term_program=term_program) == expected


@pytest.mark.parametrize('kwargs,expected', [
    ({}, 0),
    ({'control_codes': 'ignore'}, 0),
])
def test_width_zeroer(kwargs, expected):
    """Width() zeroes standalone Fitzpatrick modifiers for kitty."""
    assert wcwidth.width('\U0001F3FB', term_program='kitty', **kwargs) == expected


def test_empty_overrides_includes_zeroer():
    """_EMPTY_OVERRIDES has six empty tuple fields."""
    assert _EMPTY_OVERRIDES.narrower == ()
    assert _EMPTY_OVERRIDES.vs16_narrower == ()
    assert _EMPTY_OVERRIDES.vs15_wider == ()
    assert _EMPTY_OVERRIDES.zeroer == ()
    assert _EMPTY_OVERRIDES.narrow_wider == ()
    assert _EMPTY_OVERRIDES.narrow_zeroer == ()


def test_get_term_overrides_returns_empty_when_no_overrides():
    """get_term_overrides returns _EMPTY_OVERRIDES when terminal has no override data."""
    get_term_overrides.cache_clear()
    overrides = get_term_overrides('no-such-terminal')
    assert overrides is _EMPTY_OVERRIDES


def test_get_term_overrides_reads_narrow_zeroer_key():
    """get_term_overrides reads 'narrow_zeroer' key from NARROW_OVERRIDES."""
    get_term_overrides.cache_clear()
    overrides = get_term_overrides('kitty')
    assert len(overrides.narrow_zeroer) == 9
    assert overrides.narrow_zeroer[0] == (0x00AD, 0x00AD)


def test_get_term_overrides_narrow_wider_still_empty():
    """get_term_overrides narrow_wider is empty when no 'wider' entries exist."""
    get_term_overrides.cache_clear()
    overrides = get_term_overrides('konsole')
    assert overrides.narrow_wider == ()


@pytest.mark.parametrize('codepoint', [
    '\u00ad',
    '\u0600',
    '\u0605',
    '\u06dd',
    '\u070f',
    '\u0890',
    '\u0891',
    '\u08e2',
    '\U000110bd',
    '\U000110cd',
])
def test_narrow_zeroer_cf_codepoints(codepoint):
    """Cf format characters are zeroed by kitty/konsole/wezterm narrow_zeroer."""
    assert wcwidth.wcswidth(codepoint) == 1
    assert wcwidth.wcstwidth(codepoint, term_program='kitty') == 0
    assert wcwidth.wcstwidth(codepoint, term_program='konsole') == 0
    assert wcwidth.wcstwidth(codepoint, term_program='wezterm') == 0


@pytest.mark.parametrize('func', [wcwidth.wcstwidth, wcwidth.width])
def test_narrow_zeroer_not_applied_for_other_terminals(func):
    """Terminals without narrow overrides keep width 1 for Cf characters."""
    assert func('\u0600', term_program='xterm') == 1
    assert func('\u0600', term_program='ghostty') == 1
    assert func('\u0600', term_program='') == 1


@pytest.mark.parametrize('func,term_program,expected', [
    (wcwidth.wcstwidth, 'kitty', 0),
    (wcwidth.width, 'kitty', 0),
    (wcwidth.wcstwidth, 'konsole', 0),
    (wcwidth.width, 'konsole', 0),
    (wcwidth.wcstwidth, 'wezterm', 0),
    (wcwidth.width, 'wezterm', 0),
])
def test_narrow_zeroer_width(func, term_program, expected):
    """Width() matches wcstwidth() for narrow_zeroer overrides."""
    assert func('\u0600', term_program=term_program) == expected


@pytest.mark.parametrize('value,expected', [
    ('  KITTY  ', 'kitty'),
    ('   ', None),
])
def test_resolve_terminal_strips_whitespace(value, expected):
    """resolve_terminal strips, lowercases, and returns None for whitespace-only."""
    assert resolve_terminal(value) == expected


@pytest.mark.parametrize('text,term_program,expected', [
    ('\u1000\u1031', False, 2),   # MYANMAR LETTER KA + MYANMAR VOWEL SIGN E (Burmese)
    ('\u1000\u1031', '', 2),
    ('\u1000\u1031', 'kitty', 1),
    ('\u1000\u1031', 'foot', 1),
    ('\u1000\u1031', 'alacritty', 2),
    ('\u0c05\u0c02', False, 2),   # TELUGU LETTER A + TELUGU SIGN ANUSVARA
    ('\u0c05\u0c02', 'kitty', 1),
    ('\u0e01\u0e33', False, 2),   # THAI CHARACTER KO KAI + THAI CHARACTER SARA AM
    ('\u0e01\u0e33', 'kitty', 1),
    ('\u0985\u0982', False, 2),   # BENGALI LETTER A + BENGALI SIGN ANUSVARA
    ('\u0985\u0982', 'kitty', 1),
    ('\u0915\u093e', False, 2),   # DEVANAGARI LETTER KA + DEVANAGARI VOWEL SIGN AA
    ('\u0915\u093e', 'kitty', 1),
    ('\u0915\u093e', 'foot', 1),
    ('\u0915\u093e', 'alacritty', 2),
])
def test_wcswidth_language_grapheme(text, term_program, expected):
    """Language grapheme clusters use per-terminal override tables."""
    assert wcwidth.wcstwidth(text, term_program=term_program) == expected


@pytest.mark.parametrize('text,term_program,expected', [
    ('\u1000\u1031', 'kitty', 1),  # MYANMAR LETTER KA + VOWEL SIGN E
    ('\u1000\u1031', 'foot', 1),
    ('\u0915\u093e', 'kitty', 1),  # DEVANAGARI LETTER KA + VOWEL SIGN AA
    ('\u0915\u093e', 'foot', 1),
    ('\u0c05\u0c02', 'kitty', 1),  # TELUGU LETTER A + SIGN ANUSVARA
])
def test_width_language_grapheme(text, term_program, expected):
    """Width() applies language grapheme overrides."""
    assert wcwidth.width(text, term_program=term_program) == expected


@pytest.mark.parametrize('text,term_program,expected', [
    ('hello', 'kitty', 5),
    ('hello', 'foot', 5),
    ('hello\u1000\u1031', 'kitty', 6),   # ASCII + Burmese KA + VOWEL SIGN E
    ('\u1000\u1031hello', 'kitty', 6),   # Burmese KA + VOWEL SIGN E + ASCII
    ('\u1000\u1031\u1000\u1031', 'kitty', 2),  # two Burmese KA+E clusters
    ('\u1000\u1031x\u0915\u093e', 'kitty', 3),  # Burmese + ASCII + Devanagari
])
def test_wcswidth_mixed_language_ascii(text, term_program, expected):
    """Language grapheme overrides do not affect ASCII and mix correctly."""
    assert wcwidth.wcstwidth(text, term_program=term_program) == expected


@pytest.mark.parametrize('text,term_program,expected', [
    ('\u1000\u1039\u1001', False, 2),   # MYANMAR KA + VIRAMA + KHA (conjunct)
    ('\u1000\u1039\u1001', 'kitty', 1),
    ('\u1000\u1039\u1001', 'foot', 2),
    ('\u1000\u103b\u102d\u102f', False, 2),  # MYANMAR KA + MEDIAL YA + VOWEL I + VOWEL U
    ('\u1000\u103b\u102d\u102f', 'kitty', 1),
    ('\u1000\u103b\u102d\u102f', 'foot', 1),
])
def test_wcswidth_virama_conjunct(text, term_program, expected):
    """Virama conjunct grapheme clusters use per-terminal overrides."""
    assert wcwidth.wcstwidth(text, term_program=term_program) == expected


@pytest.mark.parametrize('text,term_program,expected', [
    ('\u0915\u094D\u200D\u0937', False, 2),     # Devanagari C+Virama+ZWJ+C (explicit ZWJ conjunct)
    ('\u0915\u094D\u200D\u0937', 'xterm', 2),
])
def test_wcswidth_virama_zwj_conjunct(text, term_program, expected):
    """Virama+ZWJ conjunct skips ZWJ and forms a capped conjunct."""
    assert wcwidth.wcstwidth(text, term_program=term_program) == expected


@pytest.mark.parametrize('text,term_program,expected', [
    ('\u1000\u1031', 'xterm', 2),  # Burmese: no xterm override
    ('\u0915\u093e', 'xterm', 2),  # Devanagari: no xterm override
    ('\u0c05\u0c02', 'xterm', 2),  # Telugu: no xterm override
])
def test_wcswidth_language_no_override(text, term_program, expected):
    """Terminals without language overrides return spec width."""
    assert wcwidth.wcstwidth(text, term_program=term_program) == expected


@pytest.mark.parametrize('text,term_program,expected', [
    ('\u1000\u1031X', 'kitty', 2),  # Burmese C+Mc mid-string flush via cluster_text
    ('X\u1000\u1031', 'kitty', 2),  # ASCII then Burmese C+Mc append
    ('\u1000\u1031\u1000\u1031', 'kitty', 2),  # two Burmese C+Mc with flush between
    ('\u0915\u093eX', 'kitty', 2),  # Devanagari C+Mc mid-string flush
])
def test_width_cluster_text_override_mid_string(text, term_program, expected):
    """Width() flushes cluster via cluster_text override when followed by another char."""
    assert wcwidth.width(text, term_program=term_program) == expected


@pytest.mark.parametrize('text,term_program,expected', [
    ('\u0e01\u0e33X', 'kitty', 2),  # Thai C+Lo override pair then ASCII
    ('X\u0e01\u0e33', 'kitty', 2),  # ASCII then Thai override pair
    ('\u0e01\u0e33\u0e01\u0e33', 'kitty', 2),  # two Thai override pairs
])
def test_width_candidate_override_mid_string(text, term_program, expected):
    """Width() flushes cluster via candidate override when two Lo chars form a pair."""
    assert wcwidth.width(text, term_program=term_program) == expected


@pytest.mark.parametrize('text,term_program,expected', [
    ('\r\u1000\u1031', 'xterm', 2),  # CR reset, cluster extends beyond prior max_extent
    ('\r\u0915\u093e', 'xterm', 2),  # Devanagari same pattern
    ('XX\b\b\u1000\u1031', 'xterm', 2),  # backspace then cluster does not exceed prior max
])
def test_width_epilogue_max_extent_update(text, term_program, expected):
    """Width() updates max_extent in epilogue when final cluster extends beyond prior max."""
    assert wcwidth.width(text, term_program=term_program) == expected


def test_wcstwidth_ambiguous_width_2():
    """Wcstwidth respects ambiguous_width parameter."""
    assert wcwidth.wcstwidth('\u00b1', ambiguous_width=2, term_program='VTE') == 2
    assert wcwidth.wcstwidth('\u00b1', ambiguous_width=2, term_program='') == 2


def test_wcstwidth_control_character():
    """Wcstwidth returns -1 for C0 control characters."""
    assert wcwidth.wcstwidth('hello\x01world', term_program='VTE') == -1
    assert wcwidth.wcstwidth('\x01', term_program='') == -1
