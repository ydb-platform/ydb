"""Tests for SGR state tracking and propagation."""
from __future__ import annotations

# std imports
import re

# local
from wcwidth import clip, wrap
from wcwidth.sgr_state import (_SGR_STATE_DEFAULT,
                               _SGRState,
                               propagate_sgr,
                               _parse_sgr_params,
                               _sgr_state_update,
                               _sgr_state_is_active,
                               _sgr_state_to_sequence)


def test_wrap_propagates_sgr():
    """Wrap() propagates SGR codes across lines by default."""
    assert wrap('\x1b[1;34mHello world\x1b[0m', width=6) == [
        '\x1b[1;34mHello\x1b[0m', '\x1b[1;34mworld\x1b[0m']
    assert wrap('\x1b[1mHello world\x1b[0m', width=6) == [
        '\x1b[1mHello\x1b[0m', '\x1b[1mworld\x1b[0m']
    assert wrap('\x1b[31mred text here\x1b[0m', width=5) == [
        '\x1b[31mred\x1b[0m', '\x1b[31mtext\x1b[0m', '\x1b[31mhere\x1b[0m']
    assert wrap('\x1b[38;5;208morange text\x1b[0m', width=7) == [
        '\x1b[38;5;208morange\x1b[0m', '\x1b[38;5;208mtext\x1b[0m']
    assert wrap('\x1b[38;2;255;0;0mred text\x1b[0m', width=5) == [
        '\x1b[38;2;255;0;0mred\x1b[0m', '\x1b[38;2;255;0;0mtext\x1b[0m']
    # multiple attributes combined
    result = wrap('\x1b[1;3;34mbold italic blue\x1b[0m', width=5)
    assert result[0] == '\x1b[1;3;34mbold\x1b[0m'
    assert result[1].startswith('\x1b[') and result[1].endswith('\x1b[0m')


def test_wrap_reset_and_no_sgr():
    """Wrap() handles reset and plain text."""
    assert wrap('\x1b[31mred\x1b[0m plain text', width=6) == ['\x1b[31mred\x1b[0m', 'plain', 'text']
    assert wrap('hello world', width=6) == ['hello', 'world']


def test_wrap_propagate_sgr_disabled():
    """Wrap() with propagate_sgr=False returns old behavior."""
    assert wrap('\x1b[31mhello world\x1b[0m', width=6, propagate_sgr=False) == [
        '\x1b[31mhello', 'world\x1b[0m']


def test_wrap_preserves_non_sgr_sequences():
    """Wrap() preserves non-SGR sequences (OSC hyperlinks)."""
    result = wrap('\x1b]8;;url\x07long link text\x1b]8;;\x07', width=5)
    # Hyperlinks get IDs added for identity preservation across lines
    osc_pattern = re.compile(r'\x1b]8;[^;]*;url\x07')
    assert all(osc_pattern.search(line) for line in result)


def test_clip_propagates_sgr():
    """Clip() restores SGR state at start and reset at end."""
    assert clip('\x1b[1;34mHello world\x1b[0m', 6, 11) == '\x1b[1;34mworld\x1b[0m'
    assert clip('\x1b[1mHello world\x1b[0m', 6, 11) == '\x1b[1mworld\x1b[0m'
    assert clip('\x1b[31mHello world\x1b[0m', 6, 11) == '\x1b[31mworld\x1b[0m'
    assert clip('\x1b[31mHello\x1b[0m', 0, 5) == '\x1b[31mHello\x1b[0m'


def test_clip_no_active_style_and_plain():
    """Clip() handles reset and plain text."""
    assert clip('\x1b[31mred\x1b[0m plain', 4, 9) == 'plain'
    assert clip('Hello world', 6, 11) == 'world'


def test_clip_propagate_sgr_disabled():
    """Clip() with propagate_sgr=False returns old behavior."""
    assert clip('\x1b[1;34mHello world\x1b[0m', 6, 11, propagate_sgr=False) == '\x1b[1;34mworld\x1b[0m'


def test_clip_preserves_non_sgr_sequences():
    """Clip() preserves non-SGR sequences (OSC hyperlinks)."""
    result = clip('\x1b]8;;url\x07link\x1b]8;;\x07', 0, 4)
    assert '\x1b]8;;url\x07' in result and '\x1b]8;;\x07' in result


def test_clip_sgr_only_no_visible_content():
    """Clip() returns empty when only SGR sequences exist."""
    assert clip('\x1b[31m\x1b[0m', 0, 10) == ''


def test_sgr_state_parse_boolean_attributes_on():
    """_sgr_state_update parses all boolean attribute on codes."""
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[1m').bold is True
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[2m').dim is True
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[3m').italic is True
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[4m').underline is True
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[5m').blink is True
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[6m').rapid_blink is True
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[7m').inverse is True
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[8m').hidden is True
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[9m').strikethrough is True
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[21m').double_underline is True


def test_sgr_state_parse_boolean_attributes_off():
    """_sgr_state_update parses all boolean attribute off codes."""
    assert _sgr_state_update(_SGRState(italic=True), '\x1b[23m').italic is False
    assert _sgr_state_update(_SGRState(inverse=True), '\x1b[27m').inverse is False
    assert _sgr_state_update(_SGRState(hidden=True), '\x1b[28m').hidden is False
    assert _sgr_state_update(_SGRState(strikethrough=True), '\x1b[29m').strikethrough is False
    # code 22 resets both bold and dim
    state = _sgr_state_update(_SGRState(bold=True, dim=True), '\x1b[22m')
    assert state.bold is False and state.dim is False
    # code 24 resets both underline and double_underline
    state = _sgr_state_update(_SGRState(underline=True, double_underline=True), '\x1b[24m')
    assert state.underline is False and state.double_underline is False
    # code 25 resets both blink and rapid_blink
    state = _sgr_state_update(_SGRState(blink=True, rapid_blink=True), '\x1b[25m')
    assert state.blink is False and state.rapid_blink is False


def test_sgr_state_parse_colors():
    """_sgr_state_update parses all color formats."""
    # basic foreground/background
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[31m').foreground == (31,)
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[41m').background == (41,)
    # bright foreground/background
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[91m').foreground == (91,)
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[101m').background == (101,)
    # 256-color (semicolon format)
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38;5;208m').foreground == (38, 5, 208)
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[48;5;208m').background == (48, 5, 208)
    # RGB (semicolon format)
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38;2;255;128;0m').foreground == (38, 2, 255, 128, 0)
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[48;2;255;128;0m').background == (48, 2, 255, 128, 0)


def test_sgr_state_parse_colors_colon_format():
    """_sgr_state_update parses ITU T.416 colon-separated color format."""
    # 256-color (colon format)
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38:5:208m').foreground == (38, 5, 208)
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[48:5:208m').background == (48, 5, 208)
    # RGB with empty colorspace (colon format): 38:2::R:G:B
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38:2::255:128:0m').foreground == (38, 2, 0, 255, 128, 0)
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[48:2::255:128:0m').background == (48, 2, 0, 255, 128, 0)
    # RGB with colorspace (colon format): 38:2:colorspace:R:G:B
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38:2:1:255:128:0m').foreground == (38, 2, 1, 255, 128, 0)
    # Mixed: colon color with semicolon attributes
    state = _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[1;38:2::255:0:0;4m')
    assert state.bold is True
    assert state.underline is True
    assert state.foreground == (38, 2, 0, 255, 0, 0)


def test_sgr_state_color_override():
    """Newer color replaces older regardless of format."""
    state = _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38;5;208m')
    assert state.foreground == (38, 5, 208)
    state = _sgr_state_update(state, '\x1b[31m')
    assert state.foreground == (31,)
    state = _sgr_state_update(state, '\x1b[38;2;0;255;0m')
    assert state.foreground == (38, 2, 0, 255, 0)
    state = _sgr_state_update(state, '\x1b[38;5;99m')
    assert state.foreground == (38, 5, 99)


def test_sgr_state_parse_default_colors():
    """_sgr_state_update parses default color codes (39, 49)."""
    assert _sgr_state_update(_SGRState(foreground=(31,)), '\x1b[39m').foreground is None
    assert _sgr_state_update(_SGRState(background=(41,)), '\x1b[49m').background is None


def test_sgr_state_parse_compound_and_reset():
    """_sgr_state_update handles compound sequences and reset."""
    state = _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[1;34;3m')
    assert state.bold is True and state.italic is True and state.foreground == (34,)
    # reset with 0
    state = _sgr_state_update(_SGRState(bold=True, foreground=(31,)), '\x1b[0m')
    assert state == _SGR_STATE_DEFAULT
    # empty is reset
    state = _sgr_state_update(_SGRState(bold=True), '\x1b[m')
    assert state == _SGR_STATE_DEFAULT
    # empty param in compound treated as 0
    state = _sgr_state_update(_SGRState(bold=True), '\x1b[;1m')
    assert state.bold is True


def test_sgr_state_to_sequence():
    """_sgr_state_to_sequence generates correct sequences."""
    assert _sgr_state_to_sequence(_SGR_STATE_DEFAULT) == ''
    assert _sgr_state_to_sequence(_SGRState(bold=True)) == '\x1b[1m'
    assert _sgr_state_to_sequence(_SGRState(rapid_blink=True)) == '\x1b[6m'
    assert _sgr_state_to_sequence(_SGRState(double_underline=True)) == '\x1b[21m'
    assert _sgr_state_to_sequence(_SGRState(foreground=(31,))) == '\x1b[31m'
    assert _sgr_state_to_sequence(_SGRState(foreground=(38, 5, 208))) == '\x1b[38;5;208m'
    assert _sgr_state_to_sequence(_SGRState(foreground=(38, 2, 255, 128, 0))) == '\x1b[38;2;255;128;0m'
    assert _sgr_state_to_sequence(_SGRState(bold=True, italic=True, foreground=(34,))) == '\x1b[1;3;34m'


def test_sgr_state_to_sequence_all_attributes():
    """_sgr_state_to_sequence handles all attributes."""
    state = _SGRState(bold=True, italic=True, underline=True, inverse=True,
                      foreground=(31,), background=(44,))
    seq = _sgr_state_to_sequence(state)
    assert '\x1b[' in seq and 'm' in seq
    assert all(code in seq for code in ('1', '3', '4', '7', '31', '44'))


def test_sgr_state_is_active():
    """_sgr_state_is_active detects active state."""
    assert _sgr_state_is_active(_SGR_STATE_DEFAULT) is False
    assert _sgr_state_is_active(_SGRState(bold=True)) is True
    assert _sgr_state_is_active(_SGRState(foreground=(31,))) is True


def test_propagate_sgr():
    """propagate_sgr handles various input cases."""
    assert len(propagate_sgr([])) == 0
    assert propagate_sgr(['hello', 'world']) == ['hello', 'world']
    assert propagate_sgr(['\x1b[31mhello\x1b[0m']) == ['\x1b[31mhello\x1b[0m']
    assert propagate_sgr(['\x1b[31mhello', 'world\x1b[0m']) == [
        '\x1b[31mhello\x1b[0m', '\x1b[31mworld\x1b[0m']
    assert propagate_sgr(['\x1b[31mred\x1b[0m', 'plain']) == ['\x1b[31mred\x1b[0m', 'plain']


def test_propagate_sgr_empty_lines():
    """propagate_sgr handles empty lines."""
    result = propagate_sgr(['\x1b[31mred', '', 'text\x1b[0m'])
    assert result[0] == '\x1b[31mred\x1b[0m'
    assert result[1] == '\x1b[31m\x1b[0m'
    assert result[2] == '\x1b[31mtext\x1b[0m'


def test_sgr_malformed_sequences():
    """Malformed SGR sequences are ignored."""
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38;5m').foreground is None
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38;2;255m').foreground is None
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[999m') == _SGR_STATE_DEFAULT
    # malformed background extended colors
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[48;5m').background is None
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[48;2;255m').background is None
    # invalid mode (not 2 or 5) for extended color
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38;3;128m').foreground is None
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[48;3;128m').background is None


def test_parse_sgr_params_invalid():
    """_parse_sgr_params returns empty list for invalid sequences."""
    assert not _parse_sgr_params('invalid')


def test_extended_color_mixed_format_edge_cases():
    """Extended color parsing handles mixed semicolon/colon format edge cases."""
    # 38=FG_EXTENDED, colon tuple (48, 2, 255, 0, 0) consumed as mode - returns None
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38;48:2:255:0:0m').foreground is None
    # 38=FG_EXTENDED, 5=256-color mode, colon tuple consumed as index - returns None
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38;5;48:5:99m').foreground is None
    # 38=FG_EXTENDED, 2=RGB mode, r=255, g=128, colon tuple consumed as b - returns None
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[38;2;255;128;48:2:0:0:0m').foreground is None
    # colon tuple with invalid base (99) is ignored
    assert _sgr_state_update(_SGR_STATE_DEFAULT, '\x1b[99:2:255:0:0m') == _SGR_STATE_DEFAULT
