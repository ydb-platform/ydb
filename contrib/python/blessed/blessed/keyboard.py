"""Sub-module providing 'keyboard awareness'."""
# pylint: disable=too-many-lines
# std imports
import os
import re
import time
import typing
import platform
import functools
from typing import TYPE_CHECKING, Set, Dict, Match, Tuple, TypeVar, Optional
from collections import OrderedDict, namedtuple

if TYPE_CHECKING:  # pragma: no cover
    # local
    from blessed.terminal import Terminal

# local
from blessed.mouse import (RE_PATTERN_MOUSE_SGR,
                           RE_PATTERN_MOUSE_LEGACY,
                           MouseEvent,
                           MouseSGREvent,
                           MouseLegacyEvent)
from blessed.dec_modes import DecPrivateMode

_T = TypeVar('_T', bound='Keystroke')


# isort: off
# curses
if platform.system() == 'Windows':
    # pylint: disable=import-error
    import jinxed as curses
    from jinxed.has_key import _capability_names as capability_names
else:
    import curses
    from curses.has_key import _capability_names as capability_names


# DEC event namedtuples
BracketedPasteEvent = namedtuple('BracketedPasteEvent', 'text')

FocusEvent = namedtuple('FocusEvent', 'gained')
SyncEvent = namedtuple('SyncEvent', 'begin')
ResizeEvent = namedtuple('ResizeEvent', 'height_chars width_chars height_pixels width_pixels')


# Keyboard protocol namedtuples
KittyKeyEvent = namedtuple('KittyKeyEvent',
                           'unicode_key shifted_key base_key modifiers event_type int_codepoints')
ModifyOtherKeysEvent = namedtuple('ModifyOtherKeysEvent', 'key modifiers')
LegacyCSIKeyEvent = namedtuple('LegacyCSIKeyEvent', 'kind key_id modifiers event_type')

# Regex patterns for keyboard protocols

# Kitty keyboard protocol: ESC [ unicode_key [: shifted_key : base_key]
#                              [; modifiers [: event_type]] [; text_codepoints] u
RE_PATTERN_KITTY_KB_PROTOCOL = re.compile(
    r'\x1b\[(?P<unicode_key>\d+)'
    r'(?::(?P<shifted_key>\d*))?'
    r'(?::(?P<base_key>\d*))?'
    r'(?:;(?P<modifiers>\d*))?'
    r'(?::(?P<event_type>\d+))?'
    r'(?:;(?P<text_codepoints>[\d:]+))?'
    r'u')
# Legacy CSI modifiers: ESC [ 1 ; modifiers [ABCDEFHPQRS]
RE_PATTERN_LEGACY_CSI_MODIFIERS = re.compile(
    r'\x1b\[1;(?P<mod>\d+)(?::(?P<event>\d+))?(?P<final>[ABCDEFHPQRS])')
RE_PATTERN_LEGACY_CSI_TILDE = re.compile(
    r'\x1b\[(?P<key_num>\d+);(?P<mod>\d+)(?::(?P<event>\d+))?~')
RE_PATTERN_LEGACY_SS3_FKEYS = re.compile(r'\x1bO(?P<mod>\d)(?P<final>[PQRS])')
# ModifyOtherKeys: ESC [ 27 ; modifiers ; key [~]
RE_PATTERN_MODIFY_OTHER = re.compile(
    r'\x1b\[27;(?P<modifiers>\d+);(?P<key>\d+)(?P<tilde>~?)')

# Bracketed paste (mode 2004): ESC [ 200 ~ text ESC [ 201 ~
RE_PATTERN_BRACKETED_PASTE = re.compile(r'\x1b\[200~(?P<text>.*?)\x1b\[201~', re.DOTALL)
# Focus tracking (mode 1004): ESC [ I or ESC [ O
RE_PATTERN_FOCUS = re.compile(r'\x1b\[(?P<io>[IO])')
# Resize notification (mode 2048): ESC [ 48 ; height ; width ; height_px ; width_px t
RE_PATTERN_RESIZE = re.compile(r'\x1b\[48;(?P<height_chars>\d+);(?P<width_chars>\d+)'
                               r';(?P<height_pixels>\d+);(?P<width_pixels>\d+)t')

# DEC event pattern container
DECEventPattern = namedtuple("DECEventPattern", ["mode", "pattern"])

# DEC event patterns - compiled regexes with metadata of the 'mode' that
# triggered it, this prevents searching for bracketed paste or mouse modes
# unless it is enabled, and, when enabled, to supplant the match with the DEC
# mode that triggered it. For Mouse modes, there is some order of precedence.
DEC_EVENT_PATTERNS = [
    DECEventPattern(mode=DecPrivateMode.BRACKETED_PASTE, pattern=RE_PATTERN_BRACKETED_PASTE),
    DECEventPattern(mode=DecPrivateMode.MOUSE_SGR_PIXELS, pattern=RE_PATTERN_MOUSE_SGR),
    DECEventPattern(mode=DecPrivateMode.MOUSE_EXTENDED_SGR, pattern=RE_PATTERN_MOUSE_SGR),
    DECEventPattern(mode=DecPrivateMode.MOUSE_ALL_MOTION, pattern=RE_PATTERN_MOUSE_LEGACY),
    DECEventPattern(mode=DecPrivateMode.MOUSE_REPORT_DRAG, pattern=RE_PATTERN_MOUSE_LEGACY),
    DECEventPattern(mode=DecPrivateMode.MOUSE_REPORT_CLICK, pattern=RE_PATTERN_MOUSE_LEGACY),
    DECEventPattern(mode=DecPrivateMode.FOCUS_IN_OUT_EVENTS, pattern=RE_PATTERN_FOCUS),
    DECEventPattern(mode=DecPrivateMode.IN_BAND_WINDOW_RESIZE, pattern=RE_PATTERN_RESIZE),
]

# Control character mappings
# Note: Ctrl+Space (code 0) is handled specially as 'SPACE', not '@' or ' '.
SYMBOLS_MAP_CTRL_CHAR = {'[': 27, '\\': 28, ']': 29, '^': 30, '_': 31, '?': 127}
SYMBOLS_MAP_CTRL_VALUE = {v: k for k, v in SYMBOLS_MAP_CTRL_CHAR.items()}

# Event type tokens for keystroke predicates
_EVENT_TYPE_TOKENS = {'pressed', 'repeated', 'released'}

# PUA keypad key names mapping (for keys without legacy non-PUA versions)
_PUA_KEYPAD_NAMES = {
    57399: 'KEY_KP_0', 57400: 'KEY_KP_1', 57401: 'KEY_KP_2', 57402: 'KEY_KP_3',
    57403: 'KEY_KP_4', 57404: 'KEY_KP_5', 57405: 'KEY_KP_6', 57406: 'KEY_KP_7',
    57407: 'KEY_KP_8', 57408: 'KEY_KP_9', 57409: 'KEY_KP_DECIMAL',
    57410: 'KEY_KP_DIVIDE', 57411: 'KEY_KP_MULTIPLY', 57412: 'KEY_KP_SUBTRACT',
    57413: 'KEY_KP_ADD', 57415: 'KEY_KP_EQUAL', 57416: 'KEY_KP_SEPARATOR',
}

# Alt-only control character name mappings
ALT_CONTROL_NAMES = {
    0x1b: 'KEY_ALT_ESCAPE',     # ESC
    0x7f: 'KEY_ALT_BACKSPACE',  # DEL
    0x0d: 'KEY_ALT_ENTER',      # CR
    0x09: 'KEY_ALT_TAB',        # TAB
    0x5b: 'CSI'                 # CSI '['
}


class KittyModifierBits:
    """Standard modifier bit flags (compatible with Kitty keyboard protocol)."""
    # pylint: disable=too-few-public-methods

    shift = 0b1
    alt = 0b10
    ctrl = 0b100
    super = 0b1000
    hyper = 0b10000
    meta = 0b100000
    caps_lock = 0b1000000
    num_lock = 0b10000000

    #: Names of bitwise flags attached to this class
    names = ('shift', 'alt', 'ctrl', 'super', 'hyper', 'meta',
             'caps_lock', 'num_lock')

    #: Modifiers only, in the generally preferred order in phrasing
    names_modifiers_only = ('ctrl', 'alt', 'shift', 'super', 'hyper', 'meta')


class Keystroke(str):
    """
    A unicode-derived class for describing a single "keystroke".

    A class instance describes a single keystroke received on input, which may
    contain multiple characters as a multibyte sequence, which is indicated by
    properties :attr:`is_sequence` returning ``True``. Note that keystrokes may
    also represent mouse input, bracketed paste, or focus in/out events
    depending on enabled terminal modes.

    The string :attr:`name` of the sequence is used to identify in code logic,
    such as ``'KEY_LEFT'`` to represent a common and human-readable form of
    the Keystroke this class instance represents.
    """
    _name: Optional[str] = None
    _code: Optional[int] = None
    _mode: Optional[int] = None
    _match: typing.Any = None
    _modifiers: int = 1

    def __new__(cls: typing.Type[_T], ucs: str = '', code: Optional[int] = None,
                name: Optional[str] = None, mode: Optional[int] = None,
                match: typing.Any = None) -> _T:
        # pylint: disable=too-many-positional-arguments
        """Class constructor."""
        new = str.__new__(cls, ucs)
        new._name = name
        new._code = code  # curses keycode is exposed for legacy API
        new._mode = mode  # Internal mode indicator for different protocols
        new._match = match  # regex match object for protocol-specific data
        new._modifiers = cls._infer_modifiers(ucs, mode, match)
        return new

    @staticmethod
    def _infer_modifiers(ucs: str, mode: Optional[int], match: typing.Any) -> int:
        """
        Infer modifiers from keystroke data.

        Returns modifiers in standard format: 1 + bitwise OR of modifier flags.
        """
        # ModifyOtherKeys or Legacy CSI modifiers
        if mode is not None and mode < 0 and match is not None:
            return match.modifiers

        # Legacy sequences starting with ESC (metaSendsEscape)
        if len(ucs) == 2 and ucs[0] == '\x1b':
            char_code = ord(ucs[1])

            # Special C0 controls that should be Alt-only per legacy spec
            # These represent common Alt+key combinations that are unambiguous
            # (Enter, Escape, DEL, Tab)
            if char_code in {0x0d, 0x1b, 0x7f, 0x09}:
                return 1 + KittyModifierBits.alt  # 1 + alt flag = 3

            # Other control characters represent Ctrl+Alt combinations
            # (ESC prefix for Alt + control char from Ctrl+letter mapping)
            if 0 <= char_code <= 31 or char_code == 127:
                # 1 + alt flag + ctrl flag = 7
                return 1 + KittyModifierBits.alt + KittyModifierBits.ctrl

            # Printable characters - Alt-only unless uppercase letter (which adds Shift)
            if 32 <= char_code <= 126:
                ch = ucs[1]
                shift = KittyModifierBits.shift if ch.isalpha() and ch.isupper() else 0
                return 1 + KittyModifierBits.alt + shift

        # Legacy Ctrl: single control character
        if len(ucs) == 1:
            char_code = ord(ucs)
            if 0 <= char_code <= 31 or char_code == 127:
                return 1 + KittyModifierBits.ctrl  # 1 + ctrl flag = 5

        # No modifiers detected
        return 1

    @property
    def is_sequence(self) -> bool:
        """Whether the value represents a multibyte sequence (bool)."""
        return self._code is not None or self._mode is not None or len(self) > 1

    def __repr__(self) -> str:
        """Docstring overwritten."""
        return (str.__repr__(self) if self._name is None else
                self._name)
    __repr__.__doc__ = str.__doc__

    def _get_modified_keycode_name(self) -> Optional[str]:
        """
        Get name for modern/legacy CSI sequence with modifiers.

        Returns name like 'KEY_CTRL_ALT_F1' or 'KEY_SHIFT_UP_RELEASED'. Also handles release/repeat
        events for keys without modifiers.
        """
        # Check if this is a special keyboard protocol mode
        if not (self.uses_keyboard_protocol and self._code is not None):
            return None

        # turn keycode value into 'base name', eg.
        # self._code of 265 -> 'KEY_F1' -> 'F1' base_name
        keycodes = get_keyboard_codes()
        base_name = keycodes.get(self._code)

        # handle PUA keypad keys that aren't in get_keyboard_codes()
        if not base_name and 57399 <= self._code <= 57416:  # Keypad PUA range
            base_name = _PUA_KEYPAD_NAMES.get(self._code)

        if not base_name or not base_name.startswith('KEY_'):
            return None

        # get "base name" name by, 'KEY_F1' -> 'F1'
        base_name = base_name[4:]

        # Build possible modifier prefix series (excludes num/capslock)
        # "Ctrl + Alt + Shift + Super / Meta"
        mod_parts = []
        for mod_name in KittyModifierBits.names_modifiers_only:
            if getattr(self, f'_{mod_name}'):        # 'if self._shift'
                mod_parts.append(mod_name.upper())   # -> 'SHIFT'

        # For press events with no modifiers, check if this is a PUA functional
        # key or a control character key (Escape, Tab, Enter, Backspace).
        is_control_char_key = self._code in _KITTY_CONTROL_CHAR_TO_KEYCODE.values()
        if (not mod_parts
                and not (self.released or self.repeated)
                and not _is_kitty_functional_key(self._code)
                and not is_control_char_key):
            return None

        # Build base result with modifiers (if any)
        base_result = (f"KEY_{'_'.join(mod_parts)}_{base_name}"
                       if mod_parts
                       else f"KEY_{base_name}")

        # Append event type suffix if not a press event
        if self.repeated:
            return f"{base_result}_REPEATED"
        if self.released:
            return f"{base_result}_RELEASED"
        return base_result  # pressed (no suffix)

    def _get_kitty_protocol_name(self) -> Optional[str]:
        """
        Get name for Kitty keyboard protocol letter/digit/symbol.

        Returns name like 'KEY_CTRL_ALT_A', 'KEY_ALT_SHIFT_5', 'KEY_CTRL_J_RELEASED', etc.
        """
        if self._mode != DecPrivateMode.SpecialInternalKitty:
            return None

        # Determine the base key - prefer base_key if available
        base_codepoint = (self._match.base_key if self._match.base_key is not None
                          else self._match.unicode_key)

        # Special case: '[' always returns 'CSI' regardless of modifiers
        if base_codepoint == 91:  # '['
            return 'CSI'

        # Only proceed if it's an ASCII letter or digit
        if not ((65 <= base_codepoint <= 90) or   # A-Z
                (97 <= base_codepoint <= 122) or  # a-z
                (48 <= base_codepoint <= 57)):    # 0-9
            return None

        # For letters: convert to uppercase for consistent naming
        # For digits: use as-is
        char = (chr(base_codepoint).upper()
                if (65 <= base_codepoint <= 90 or 97 <= base_codepoint <= 122)
                else chr(base_codepoint))

        # Build modifier prefix list in order: CTRL, ALT, SHIFT, SUPER, HYPER, META
        mod_parts = []
        for mod_name in KittyModifierBits.names_modifiers_only:
            if getattr(self, f'_{mod_name}'):
                mod_parts.append(mod_name.upper())

        # Only synthesize name if at least one modifier is present
        if not mod_parts:
            return None

        base_result = f"KEY_{'_'.join(mod_parts)}_{char}"

        # Append event type suffix if not a press event
        if self.repeated:
            return f"{base_result}_REPEATED"
        if self.released:
            return f"{base_result}_RELEASED"
        return base_result  # pressed (no suffix)

    def _get_control_char_name(self) -> Optional[str]:
        """
        Get name for single-character control sequences.

        Returns name like 'KEY_CTRL_A' or 'KEY_CTRL_SPACE'.
        """
        if len(self) != 1:
            return None

        char_code = ord(self)
        if char_code == 0:
            return 'KEY_CTRL_SPACE'
        if 1 <= char_code <= 26:
            # Ctrl+A through Ctrl+Z
            return f'KEY_CTRL_{chr(char_code + ord("A") - 1)}'
        if char_code in SYMBOLS_MAP_CTRL_VALUE:
            return f'KEY_CTRL_{SYMBOLS_MAP_CTRL_VALUE[char_code]}'
        return None

    def _get_control_symbol(self, char_code: int) -> str:
        """
        Get control symbol for a character code.

        Returns symbol like 'A' for Ctrl+A, 'SPACE' for Ctrl+Space, 'BACKSPACE' for Ctrl+H, etc.
        """
        if char_code == 0:
            return 'SPACE'
        # Special case: Ctrl+H (Backspace) sends \x08
        if char_code == 8:
            return 'BACKSPACE'
        if 1 <= char_code <= 26:
            # Ctrl+A through Ctrl+Z
            return chr(char_code + ord("A") - 1)
        # Ctrl+symbol
        return SYMBOLS_MAP_CTRL_VALUE[char_code]

    def _get_alt_only_control_name(self, char_code: int) -> Optional[str]:
        """
        Get name for Alt-only special control characters.

        Returns names like 'KEY_ALT_ESCAPE', 'KEY_ALT_BACKSPACE', etc.
        """
        return ALT_CONTROL_NAMES.get(char_code)

    def _get_meta_escape_name(self) -> Optional[str]:
        """
        Get name for metaSendsEscape sequences (ESC + char).

        Returns name like 'KEY_ALT_A', 'KEY_ALT_SHIFT_Z', 'KEY_CTRL_ALT_C', or 'KEY_ALT_ESCAPE'.
        """
        # pylint: disable=too-many-return-statements
        if not self._is_escape_sequence():
            return None

        char_code = ord(self[1])

        # Check for ESC + control char
        if 0 <= char_code <= 31 or char_code == 127:
            symbol = self._get_control_symbol(char_code)
            # Check if this is Alt-only or Ctrl+Alt based on modifiers
            if self.modifiers == 3:  # Alt-only (1 + 2)
                # Special C0 controls that are Alt-only
                return self._get_alt_only_control_name(char_code)
            if self.modifiers == 7:  # Ctrl+Alt (1 + 2 + 4)
                return f'KEY_CTRL_ALT_{symbol}'

        # return KEY_ALT_ for "metaSendsEscape"
        ch = self[1]
        if ch.isalpha():
            if ch.isupper():
                return f'KEY_ALT_SHIFT_{ch}'
            return f'KEY_ALT_{ch.upper()}'
        if ch == '[':
            return 'CSI'
        if ch == ' ':
            return 'KEY_ALT_SPACE'
        return f'KEY_ALT_{ch}'

    def _get_mouse_event_name(self) -> Optional[str]:
        """
        Get name for mouse events.

        Returns name like 'MOUSE_LEFT', 'MOUSE_CTRL_LEFT', 'MOUSE_SCROLL_UP', 'MOUSE_LEFT_RELEASED',
        'MOUSE_MOTION', 'MOUSE_RIGHT_MOTION', etc.
        """
        # Check if this is a mouse mode
        if self._mode not in (DecPrivateMode.MOUSE_EXTENDED_SGR,
                              DecPrivateMode.MOUSE_SGR_PIXELS,
                              DecPrivateMode.MOUSE_REPORT_CLICK,
                              DecPrivateMode.MOUSE_HILITE_TRACKING,
                              DecPrivateMode.MOUSE_REPORT_DRAG,
                              DecPrivateMode.MOUSE_ALL_MOTION):
            return None

        # Get the button name from _mode_values
        mouse_event = self._mode_values
        if not isinstance(mouse_event, MouseEvent):
            return None

        # Return MOUSE_ prefix + button name
        return f'MOUSE_{mouse_event.button}'

    def _get_focus_event_name(self) -> Optional[str]:
        """
        Get name for focus events.

        Returns 'FOCUS_IN' or 'FOCUS_OUT'.
        """
        if self._mode != DecPrivateMode.FOCUS_IN_OUT_EVENTS:
            return None

        # Check the io group to determine if focus was gained or lost
        if self._match is not None and self._match.group('io') == 'I':
            return 'FOCUS_IN'
        if self._match is not None and self._match.group('io') == 'O':
            return 'FOCUS_OUT'

        return None

    def _get_bracketed_paste_name(self) -> Optional[str]:
        """
        Get name for bracketed paste events.

        Returns 'BRACKETED_PASTE'.
        """
        if self._mode == DecPrivateMode.BRACKETED_PASTE:
            return 'BRACKETED_PASTE'
        return None

    @property
    def name(self) -> Optional[str]:  # pylint: disable=too-many-return-statements
        r"""
        Special application key name.

        This is the best equality attribute to use for special keys, as raw string value of the 'F1'
        key can be received in many different values.

        The 'name' property will return a reliable constant, eg. ``'KEY_F1'``.

        The name supports "modifiers", such as ``'KEY_CTRL_F1'``,
        ``'KEY_CTRL_ALT_F1'``, ``'KEY_CTRL_ALT_SHIFT_F1'``

        For mouse events, the name includes the ``'MOUSE_'`` prefix followed by the
        button/action name, such as ``'MOUSE_LEFT'``, ``'MOUSE_MOTION'``,
        ``'MOUSE_RIGHT_MOTION'``, ``'MOUSE_LEFT_RELEASED'``.

        For other DEC events:
        - Focus events: 'FOCUS_IN' or 'FOCUS_OUT'
        - Bracketed paste: 'BRACKETED_PASTE'
        - Resize events: 'RESIZE_EVENT'

        When non-None, all phrases begin with either 'KEY', 'MOUSE', 'FOCUS_IN', 'FOCUS_OUT',
        'BRACKETED_PASTE', or 'RESIZE_EVENT', with one exception: 'CSI' is returned for '\\x1b['
        to indicate the beginning of a presumed unsupported input sequence. The phrase 'KEY_ALT_['
        is never returned and unsupported.

        If this value is None, then it can probably be assumed that the value is an unsurprising
        textual character without any modifiers, like the letter ``'a'``.
        """
        if self._name is not None:
            return self._name

        # Try each helper method in sequence
        # DEC events first
        result = self._get_mouse_event_name()
        if result is not None:
            return result

        result = self._get_focus_event_name()
        if result is not None:
            return result

        result = self._get_bracketed_paste_name()
        if result is not None:
            return result

        # Inline resize event check
        if self._mode == DecPrivateMode.IN_BAND_WINDOW_RESIZE:
            return 'RESIZE_EVENT'

        # Keyboard events
        result = self._get_modified_keycode_name()
        if result is not None:
            return result

        result = self._get_kitty_protocol_name()
        if result is not None:
            return result

        result = self._get_control_char_name()
        if result is not None:
            return result

        result = self._get_meta_escape_name()
        if result is not None:
            return result

        return self._name

    @property
    def code(self) -> Optional[int]:
        """Legacy curses-alike keycode value (int)."""
        return self._code

    @property
    def modifiers(self) -> int:
        """
        Modifier flags in standard keyboard protocol format.

        :rtype: int
        :returns: Standard-style modifiers value (1 means no modifiers)

        The value is 1 + bitwise OR of modifier flags:

        - shift: 0b1 (1)
        - alt: 0b10 (2)
        - ctrl: 0b100 (4)
        - super: 0b1000 (8)
        - hyper: 0b10000 (16)
        - meta: 0b100000 (32)
        - caps_lock: 0b1000000 (64)
        - num_lock: 0b10000000 (128)
        """
        return self._modifiers

    @property
    def modifiers_bits(self) -> int:
        """
        Raw modifier bit flags without the +1 offset.

        :rtype: int
        :returns: Raw bitwise OR of modifier flags (0 means no modifiers)
        """
        return max(0, self._modifiers - 1)

    # Private modifier flag properties (internal use)
    @property
    def _shift(self) -> bool:
        """Whether the shift modifier is active."""
        return bool(self.modifiers_bits & KittyModifierBits.shift)

    @property
    def _alt(self) -> bool:
        """Whether the alt modifier is active."""
        return bool(self.modifiers_bits & KittyModifierBits.alt)

    @property
    def _ctrl(self) -> bool:
        """Whether the ctrl modifier is active."""
        return bool(self.modifiers_bits & KittyModifierBits.ctrl)

    @property
    def _super(self) -> bool:
        """Whether the super (Windows/Cmd) modifier is active."""
        return bool(self.modifiers_bits & KittyModifierBits.super)

    @property
    def _hyper(self) -> bool:
        """Whether the hyper modifier is active."""
        return bool(self.modifiers_bits & KittyModifierBits.hyper)

    @property
    def _meta(self) -> bool:
        """Whether the meta modifier is active."""
        return bool(self.modifiers_bits & KittyModifierBits.meta)

    @property
    def _caps_lock(self) -> bool:
        """Whether caps lock was known to be active during this sequence."""
        return bool(self.modifiers_bits & KittyModifierBits.caps_lock)

    @property
    def _num_lock(self) -> bool:
        """Whether num lock was known to be active during this sequence."""
        return bool(self.modifiers_bits & KittyModifierBits.num_lock)

    @property
    def uses_keyboard_protocol(self) -> bool:
        """
        Whether this keystroke uses a special keyboard protocol mode.

        Returns True for Kitty, ModifyOtherKeys, or LegacyCSIModifier protocols, which use negative
        mode values (SpecialInternalKitty=-1, SpecialInternalModifyOtherKeys=-2,
        SpecialInternalLegacyCSIModifier=-3).

        :rtype: bool
        :returns: True if using special keyboard protocol mode
        """
        return self._mode is not None and self._mode < 0

    @property
    def pressed(self) -> bool:
        """
        Whether this is a key press event.

        :rtype: bool
        :returns: True if this is a key press event (event_type=1 or not specified), False for
            repeat or release events
        """
        if self.uses_keyboard_protocol:
            # Check if _match has event_type (Kitty, LegacyCSI, ModifyOtherKeys),
            # defaulting to 1 (pressed) if not present.
            return getattr(self._match, 'event_type', 1) == 1
        # Default: always a 'pressed' event
        return True

    @property
    def repeated(self) -> bool:
        """
        Whether this is a key repeat event.

        :rtype: bool
        :returns: True if this is a key repeat event (event_type=2), False otherwise
        """
        if self.uses_keyboard_protocol:
            return getattr(self._match, 'event_type', 1) == 2
        # Default: not a repeat event
        return False

    @property
    def released(self) -> bool:
        """
        Whether this is a key release event.

        :rtype: bool
        :returns: True if this is a key release event (event_type=3), False otherwise
        """
        if self.uses_keyboard_protocol:
            return getattr(self._match, 'event_type', 1) == 3
        # Default: not a release event
        return False

    def _is_escape_sequence(self, length: int = 2) -> bool:
        """
        Check if keystroke is an escape sequence of given length.

        :arg int length: Expected length of escape sequence (default 2)
        :rtype: bool
        :returns: True if keystroke matches ESC + (length-1) chars pattern
        """
        return len(self) == length and self[0] == '\x1b'

    @staticmethod
    def _make_expected_bits(tokens_modifiers: typing.List[str]) -> int:
        """Build expected modifier bits from token list."""
        expected_bits = 0
        for token in tokens_modifiers:
            expected_bits |= getattr(KittyModifierBits, token)
        return expected_bits

    def _make_effective_bits(self) -> int:
        """Returns modifier bits stripped of caps_lock and num_lock."""
        stripped_bits = KittyModifierBits.caps_lock | KittyModifierBits.num_lock
        return self.modifiers_bits & ~(stripped_bits)

    @staticmethod
    def _get_keycode_by_name(key_name: str) -> Optional[int]:
        """Get keycode value for a given key name."""
        keycodes = get_keyboard_codes()
        expected_key_constant = f'KEY_{key_name.upper()}'
        for code, name in keycodes.items():
            if name == expected_key_constant:
                return code
        return None

    def _build_appkeys_predicate(self, tokens_modifiers: typing.List[str], key_name: str,
                                 event_type: Optional[str] = None
                                 ) -> typing.Callable[[Optional[str], bool], bool]:
        """Build a predicate function for application keys."""
        def keycode_predicate(char: Optional[str] = None, ignore_case: bool = True) -> bool:
            # pylint: disable=unused-argument
            # char and ignore_case parameters are accepted but not used for application keys

            # Application keys never match when 'char' is non-None/non-Empty
            if char:
                return False

            # Get expected keycode from key name
            expected_code = Keystroke._get_keycode_by_name(key_name)
            if expected_code is None or self._code != expected_code:
                return False

            # Validate modifiers
            if self._make_expected_bits(tokens_modifiers) != self._make_effective_bits():
                return False

            # Check event type if specified
            if event_type is not None:
                event_type_map = {
                    'pressed': self.pressed,
                    'repeated': self.repeated,
                    'released': self.released
                }
                return event_type_map.get(event_type, False)

            return True

        return keycode_predicate

    def _build_alphanum_predicate(self, tokens_modifiers: typing.List[str]
                                  ) -> typing.Callable[[Optional[str], bool], bool]:
        """Build a predicate function for modifier checking of alphanumeric input."""
        def modifier_predicate(char: Optional[str] = None, ignore_case: bool = True) -> bool:
            # Build expected modifier bits from tokens,
            # Stripped to ignore caps_lock and num_lock
            expected_bits = self._make_expected_bits(tokens_modifiers)
            effective_bits = self._make_effective_bits()

            # When matching with a character and it's alphabetic, be lenient
            # about Shift because it is implicit in the case of the letter
            if char and len(char) == 1 and char.isalpha():
                # Strip shift from both sides for letter matching
                effective_bits_no_shift = effective_bits & ~KittyModifierBits.shift
                expected_bits_no_shift = expected_bits & ~KittyModifierBits.shift
                if effective_bits_no_shift != expected_bits_no_shift:
                    return False
            elif effective_bits != expected_bits:
                # Exact matching (no char, or non-alpha char)
                return False

            # If no character specified, always return False
            # Text keys need char argument: is_ctrl('a')
            # Application keys need specific predicate: is_ctrl_up()
            if char is None:
                return False

            # Check character match using value property
            keystroke_char = self.value

            # Compare characters
            if ignore_case:
                return keystroke_char.lower() == char.lower()
            return keystroke_char == char

        return modifier_predicate

    # pylint: disable=too-complex
    def __getattr__(self, attr: str) -> typing.Callable[[Optional[str], bool], bool]:
        """
        Dynamic compound modifier and application key predicates via __getattr__.

        Recognizes attributes starting with "is_" and parses underscore-separated
        tokens to create dynamic predicate functions.

        :arg str attr: Attribute name being accessed
        :rtype: callable or raises AttributeError
        :returns: Callable predicate function with signature
            ``Callable[[Optional[str], bool], bool]``.

            All predicates accept the same parameters:

            - ``char`` (Optional[str]): Character to match against keystroke value
            - ``ignore_case`` (bool): Whether to ignore case when matching characters

            For event predicates, application key predicates, and mouse button predicates,
            these parameters are accepted but not used.

            Mouse button predicates use the pattern ``is_mouse_<button>()`` where
            ``<button>`` matches the button name: ``is_mouse_left()``, ``is_mouse_ctrl_left()``,
            ``is_mouse_scroll_up()``, ``is_mouse_left_released()``, etc.
        """
        if not attr.startswith('is_'):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

        # Extract tokens after 'is_'
        tokens_str = attr[3:]  # Remove 'is_' prefix
        if not tokens_str:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

        # Parse tokens to separate modifiers from potential key name
        tokens = tokens_str.split('_')

        # Check for mouse button predicates (is_mouse_*)
        if tokens and tokens[0] == 'mouse':
            # Build expected mouse event name from remaining tokens
            # is_mouse_left -> MOUSE_LEFT
            # is_mouse_ctrl_left -> MOUSE_CTRL_LEFT
            # is_mouse_scroll_up -> MOUSE_SCROLL_UP
            expected_name = 'MOUSE_' + '_'.join(tokens[1:]).upper()

            def mouse_predicate(char: Optional[str] = None, ignore_case: bool = False) -> bool:
                # pylint: disable=unused-argument
                return self.name == expected_name

            return mouse_predicate

        # Check for event type suffix at the end (pressed, repeated, released)
        event_type_token = None
        if tokens and tokens[-1] in _EVENT_TYPE_TOKENS:
            event_type_token = tokens[-1]
            tokens = tokens[:-1]  # Remove event type from tokens list

        # Separate modifiers from potential key name
        tokens_modifiers = []
        tokens_key_names = []

        # a mini 'getopt' for breaking modifiers_key_names -> [modifiers], [key_name_tokens]
        for i, token in enumerate(tokens):
            if token in KittyModifierBits.names_modifiers_only:
                tokens_modifiers.append(token)
            else:
                # Remaining tokens could be a key name
                tokens_key_names = tokens[i:]
                break

        # If we have any non-modifier tokens,
        if tokens_key_names:
            # check if they form a valid application key,
            key_name = '_'.join(tokens_key_names)
            keycodes = get_keyboard_codes()
            expected_key_constant = f'KEY_{key_name.upper()}'
            if expected_key_constant in keycodes.values():
                # Return predicate with optional event type
                return self._build_appkeys_predicate(
                    tokens_modifiers, key_name, event_type_token)

        # Event type suffix without valid application key is invalid
        if event_type_token:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{attr}' "
                f"(event type suffix '{event_type_token}' only valid with application keys)")

        # No valid key name was found by 'tokens_key_names', this could just as
        # easily be asking for an attribute that doesn't exist, or a spelling
        # error of application key or modifier, report as 'invalid' token
        invalid_tokens = [token for token in tokens
                          if token not in KittyModifierBits.names_modifiers_only]
        if invalid_tokens:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{attr}' "
                f"(invalid modifier or application key tokens: {invalid_tokens})")

        # Return modifier predicate for alphanumeric keys
        return self._build_alphanum_predicate(tokens_modifiers)

    def _get_plain_char_value(self) -> Optional[str]:
        """
        Get value for plain printable characters.

        Returns the character as-is if it's a single printable character.
        """
        if len(self) == 1 and not self[0] == '\x1b' and self[0].isprintable():
            return str(self)
        return None

    def _get_escape_sequence_value(self) -> Optional[str]:
        """
        Get value for ESC+char sequences (Alt, Ctrl+Alt combinations).

        Handles Alt+printable, Alt-only special keys, and Ctrl+Alt sequences. Returns the base
        character or empty string for application keys.
        """
        if not self._is_escape_sequence():
            return None

        # Alt+printable: return the printable character as-is
        if self._alt and not self._ctrl and self[1].isprintable():
            return self[1]  # Preserves case and supports Unicode

        # Alt-only control sequences
        if self._alt and not self._ctrl:
            char_code = ord(self[1])
            # Special application keys with Alt modifier return empty string
            # These are: Escape (0x1b), Backspace/DEL (0x7f), Enter (0x0d), Tab (0x09)
            if char_code in {0x1b, 0x7f, 0x0d, 0x09}:
                return ''

        # Ctrl+Alt sequences: return base character
        if self._ctrl and self._alt:
            char_code = ord(self[1])
            # Ctrl+Space (code 0)
            if char_code == 0:
                return ' '
            # Special case: Ctrl+Alt+Backspace sends ESC + \x08
            # This is an application key, not a text key, so return empty string
            if char_code == 8:
                return ''
            # Ctrl+A through Ctrl+Z (codes 1-26)
            if 1 <= char_code <= 26:
                return chr(char_code + ord('a') - 1)  # lowercase

        return None

    def _get_ctrl_sequence_value(self) -> Optional[str]:
        """
        Get value for Ctrl+char sequences.

        Maps control characters back to their base characters.
        """
        if not (len(self) == 1 and self._ctrl and not self._alt):
            return None

        char_code = ord(self)

        # Ctrl+Space (code 0)
        if char_code == 0:
            return ' '
        # Ctrl+A through Ctrl+Z (codes 1-26)
        if 1 <= char_code <= 26:
            return chr(char_code + ord('a') - 1)  # lowercase

        # Ctrl+symbol mappings
        if char_code in SYMBOLS_MAP_CTRL_VALUE:
            return SYMBOLS_MAP_CTRL_VALUE[char_code]

        return None

    def _get_protocol_value(self) -> Optional[str]:
        """
        Get value for Kitty or ModifyOtherKeys protocol sequences.

        Extracts the character from modern keyboard protocols.
        """
        # Kitty protocol
        if self._mode == DecPrivateMode.SpecialInternalKitty:
            # prefer text_codepoints if available
            if self._match.int_codepoints:
                return ''.join(chr(cp) for cp in self._match.int_codepoints)

            # Check if this is a PUA functional key (which don't produce text)
            # This includes: keypad keys, lock keys, F13-F35, media keys, modifier
            # keys, ISO shift keys
            unicode_key = self._match.unicode_key
            if _is_kitty_functional_key(unicode_key):
                return ''

            # For control characters (Escape, Tab, Enter, Backspace), use the
            # ASCII value mapping instead of the raw unicode character
            if unicode_key in _KITTY_CONTROL_CHAR_TO_KEYCODE and self._code is not None:
                return self._get_ascii_value()

            return chr(self._match.unicode_key)

        # ModifyOtherKeys protocol - extract character from key
        if self._mode == DecPrivateMode.SpecialInternalModifyOtherKeys:
            return chr(self._match.key)

        return None

    def _get_ascii_value(self) -> Optional[str]:
        """Get value for keys matched by curses-imitated keycodes."""
        return {
            curses.KEY_ENTER: '\n',
            KEY_TAB: '\t',
            curses.KEY_BACKSPACE: '\x08',
            curses.KEY_EXIT: '\x1b',
        }.get(self._code)

    @property
    def value(self) -> str:
        r"""
        The textual character represented by this keystroke.

        :rtype: str
        :returns: For text keys, returns the base character (ignoring modifiers).
                  For application keys and sequences, returns empty string ''.
                  For release events, always returns empty string.

        Some Examples,

        - Plain text: 'a', 'A', '1', ';', ' ', 'Ω', emoji with ZWJ sequences
        - Alt+printable: Alt+a → 'a', Alt+A → 'A'
        - Ctrl+letter: Ctrl+A → 'a', Ctrl+Z → 'z'
        - Ctrl+symbol: Ctrl+@ → '@', Ctrl+? → '?', Ctrl+[ → '['
        - Control chars: '\t', '\n', '\x08', '\x1b' (for Enter/Tab/Backspace/Escape keycodes)
        - Application keys: KEY_UP, KEY_F1, etc. → ''
        - Release events: always → ''
        """
        # Release events never have text
        if self.released:
            return ''

        return (self._get_plain_char_value()
                or self._get_escape_sequence_value()
                or self._get_ctrl_sequence_value()
                or self._get_protocol_value()
                or self._get_ascii_value()
                or '')

    @property
    def mode(self) -> Optional['DecPrivateMode']:
        """
        DEC Private Mode associated with this keystroke, if any.

        :rtype: blessed.dec_modes.DecPrivateMode or None
        :returns: The :class:`~blessed.dec_modes.DecPrivateMode` enum value
            associated with this keystroke, or ``None`` if this is not a DEC mode event.

        .. note:: Mode names beginning SpecialInternal with negative values (-1,
            -2, -3) are used to track how some kinds of application keys are
            matched.
        """
        if self._mode is not None:
            return DecPrivateMode(self._mode)
        return None

    @property
    def mouse_yx(self) -> Tuple[int, int]:
        """
        Mouse position as (y, x) tuple for mouse events.

        This is particularly useful with terminal movement functions:
        ``term.move_yx(*keystroke.mouse_yx)``

        :rtype: tuple of (int, int)
        :returns: (y, x) coordinate tuple (0-indexed) for mouse events,
                  or ``(-1, -1)`` if not a mouse event
        """
        mouse_event = self._mode_values
        if isinstance(mouse_event, MouseEvent):
            return (mouse_event.y, mouse_event.x)
        return (-1, -1)

    @property
    def mouse_xy(self) -> Tuple[int, int]:
        """
        Mouse position as (x, y) tuple for mouse events.

        :rtype: tuple of (int, int)
        :returns: (x, y) coordinate tuple (0-indexed) for mouse events,
                  or ``(-1, -1)`` if not a mouse event
        """
        mouse_event = self._mode_values
        if isinstance(mouse_event, MouseEvent):
            return (mouse_event.x, mouse_event.y)
        return (-1, -1)

    @property
    def text(self) -> Optional[str]:
        """
        Pasted text for bracketed paste events.

        :rtype: str or None
        :returns: The pasted text for ``BRACKETED_PASTE`` events,
                  or ``None`` if not a bracketed paste event
        """
        paste_event = self._mode_values
        if isinstance(paste_event, BracketedPasteEvent):
            return paste_event.text
        return None

    @property
    def _mode_values(self) -> Optional[typing.Union[BracketedPasteEvent,
                                                    MouseSGREvent, MouseLegacyEvent, FocusEvent,
                                                    ResizeEvent]]:
        """
        Return structured data for DEC private mode events (private API).

        Returns a namedtuple with parsed event data for supported
        :class:`~blessed.dec_modes.DecPrivateMode` modes:

        - ``BRACKETED_PASTE``: :class:`BracketedPasteEvent` with ``text`` field
        - ``MOUSE_EXTENDED_SGR``, ``MOUSE_ALL_MOTION``,  ``MOUSE_REPORT_DRAG``,
          and ``MOUSE_REPORT_CLICK`` events: :class:`MouseEvent` with button,
          coordinates, and modifier flags
        - ``FOCUS_IN_OUT_EVENTS``: :class:`FocusEvent` with ``gained`` boolean field
        - ``IN_BAND_WINDOW_RESIZE``: :class:`ResizeEvent` with dimension fields

        :rtype: namedtuple or None
        :returns: Structured event data for this DEC mode event, or ``None`` if this
            keystroke is not a DEC mode event or the mode is not supported
        """
        if self._mode is None or self._match is None:
            return None

        # Parse based on mode type
        if self._mode in (DecPrivateMode.MOUSE_REPORT_CLICK,
                          DecPrivateMode.MOUSE_HILITE_TRACKING,
                          DecPrivateMode.MOUSE_REPORT_DRAG,
                          DecPrivateMode.MOUSE_ALL_MOTION):
            return MouseEvent.from_legacy_match(self._match)
        if self._mode in (DecPrivateMode.MOUSE_EXTENDED_SGR,
                          DecPrivateMode.MOUSE_SGR_PIXELS):
            return MouseEvent.from_sgr_match(self._match)
        if self._mode == DecPrivateMode.FOCUS_IN_OUT_EVENTS:
            return self._parse_focus()
        if self._mode == DecPrivateMode.BRACKETED_PASTE:
            return self._parse_bracketed_paste()
        if self._mode == DecPrivateMode.IN_BAND_WINDOW_RESIZE:
            return self._parse_resize()

        # Unknown DEC mode or unsupported mode
        return None

    def _parse_focus(self) -> FocusEvent:
        """Parse focus event from stored regex match."""
        gained = bool(self._match.group('io') == 'I')
        return FocusEvent(gained=gained)

    def _parse_bracketed_paste(self) -> BracketedPasteEvent:
        """Parse bracketed paste event from stored regex match."""
        return BracketedPasteEvent(text=self._match.group('text'))

    def _parse_resize(self) -> ResizeEvent:
        """Parse resize event from stored regex match."""
        return ResizeEvent(
            height_chars=int(self._match.group('height_chars')),
            width_chars=int(self._match.group('width_chars')),
            height_pixels=int(self._match.group('height_pixels')),
            width_pixels=int(self._match.group('width_pixels'))
        )


def get_curses_keycodes() -> Dict[str, int]:
    """
    Return mapping of curses key-names paired by their keycode integer value.

    :rtype: dict
    :returns: Dictionary of (name, code) pairs for curses keyboard constant
        values and their mnemonic name. Such as code ``260``, with the value of
        its key-name identity, ``'KEY_LEFT'``.
    """
    _keynames = [attr for attr in dir(curses)
                 if attr.startswith('KEY_')]
    return {keyname: getattr(curses, keyname) for keyname in _keynames}


def get_keyboard_codes() -> Dict[int, str]:
    """
    Return mapping of keycode integer values paired by their curses key- name.

    :rtype: dict
    :returns: Dictionary of (code, name) pairs for curses keyboard constant
        values and their mnemonic name. Such as key ``260``, with the value of
        its identity, ``'KEY_LEFT'``.

    These keys are derived from the attributes by the same of the curses module,
    with the following exceptions:

    * ``KEY_DELETE`` in place of ``KEY_DC``
    * ``KEY_INSERT`` in place of ``KEY_IC``
    * ``KEY_PGUP`` in place of ``KEY_PPAGE``
    * ``KEY_PGDOWN`` in place of ``KEY_NPAGE``
    * ``KEY_ESCAPE`` in place of ``KEY_EXIT``
    * ``KEY_SUP`` in place of ``KEY_SR``
    * ``KEY_SDOWN`` in place of ``KEY_SF``

    This function is the inverse of :func:`get_curses_keycodes`.  With the
    given override "mixins" listed above, the keycode for the delete key will
    map to our imaginary ``KEY_DELETE`` mnemonic, effectively erasing the
    phrase ``KEY_DC`` from our code vocabulary for anyone that wishes to use
    the return value to determine the key-name by keycode.
    """
    keycodes = OrderedDict(get_curses_keycodes())
    keycodes.update(CURSES_KEYCODE_OVERRIDE_MIXIN)

    # Merge in homemade KEY_TAB, KEY_KP_*, KEY_MENU added to our module space
    # Exclude *_PUA constants since they're internal implementation details
    # that will be remapped via KITTY_PUA_KEYCODE_OVERRIDE_MIXIN.
    # Make sure to copy globals() since other threads might create or
    # destroy globals while we iterate.
    keycodes.update((k, v) for k, v in globals().copy().items()
                    if k.startswith('KEY_') and not k.endswith('_PUA'))

    # Apply PUA keycode overrides to strip _PUA suffix from names
    # This ensures users see clean names like 'KEY_CAPS_LOCK' instead of 'KEY_CAPS_LOCK_PUA'
    keycodes.update(KITTY_PUA_KEYCODE_OVERRIDE_MIXIN)

    # invert dictionary (key, values) => (values, key), preferring the
    # last-most inserted value ('KEY_DELETE' over 'KEY_DC').
    return dict(zip(keycodes.values(), keycodes.keys()))


def _alternative_left_right(term: 'Terminal') -> typing.Dict[str, int]:
    r"""
    Determine and return mapping of left and right arrow keys sequences.

    :arg blessed.Terminal term: :class:`~.Terminal` instance.
    :rtype: dict
    :returns: Dictionary of sequences ``term._cuf1``, and ``term._cub1``,
        valued as ``KEY_RIGHT``, ``KEY_LEFT`` (when appropriate).

    This function supports :func:`get_terminal_sequences` to discover
    the preferred input sequence for the left and right application keys.

    It is necessary to check the value of these sequences to ensure we do not
    use ``' '`` and ``'\b'`` for ``KEY_RIGHT`` and ``KEY_LEFT``,
    preferring their true application key sequence, instead.
    """
    # pylint: disable=protected-access
    keymap: typing.Dict[str, int] = {}
    if term._cuf1 and term._cuf1 != ' ':
        keymap[term._cuf1] = curses.KEY_RIGHT
    if term._cub1 and term._cub1 != '\b':
        keymap[term._cub1] = curses.KEY_LEFT
    return keymap


def get_keyboard_sequences(term: 'Terminal') -> typing.OrderedDict[str, int]:
    r"""
    Return mapping of keyboard sequences paired by keycodes.

    :arg blessed.Terminal term: :class:`~.Terminal` instance.
    :returns: mapping of keyboard unicode sequences paired by keycodes
        as integer.  This is used as the argument ``mapper`` to
        the supporting function :func:`resolve_sequence`.
    :rtype: OrderedDict

    Initialize and return a keyboard map and sequence lookup table,
    (sequence, keycode) from :class:`~.Terminal` instance ``term``,
    where ``sequence`` is a multibyte input sequence of unicode
    characters, such as ``'\x1b[D'``, and ``keycode`` is an integer
    value, matching curses constant such as term.KEY_LEFT.

    The return value is an OrderedDict instance, with their keys
    sorted longest-first.
    """
    # A small gem from curses.has_key that makes this all possible,
    # _capability_names: a lookup table of terminal capability names for
    # keyboard sequences (fe. kcub1, key_left), keyed by the values of
    # constants found beginning with KEY_ in the main curses module
    # (such as KEY_LEFT).
    #
    # latin1 encoding is used so that bytes in 8-bit range of 127-255
    # have equivalent chr() and unichr() values, so that the sequence
    # of a kermit or avatar terminal, for example, remains unchanged
    # in its byte sequence values even when represented by unicode.
    #
    sequence_map = {
        seq.decode('latin1'): val for seq, val in (
            (curses.tigetstr(cap), val) for (val, cap) in capability_names.items()
        ) if seq
    } if term.does_styling else {}

    sequence_map.update(_alternative_left_right(term))
    sequence_map.update(DEFAULT_SEQUENCE_MIXIN)

    # This is for fast lookup matching of sequences, preferring
    # full-length sequence such as ('\x1b[D', KEY_LEFT)
    # over simple sequences such as ('\x1b', KEY_EXIT).
    return OrderedDict(
        (seq, sequence_map[seq]) for seq in sorted(
            sequence_map.keys(), key=len, reverse=True))


def get_leading_prefixes(sequences: typing.Iterable[str]) -> Set[str]:
    """
    Return a set of proper prefixes for given sequence of strings.

    :arg iterable sequences
    :rtype: set
    :return: Set of all string prefixes

    Given an iterable of strings, all textparts leading up to the final
    string is returned as a unique set.  This function supports the
    :meth:`~.Terminal.inkey` method by determining whether the given
    input is a sequence that **may** lead to a final matching pattern.

    >>> prefixes(['abc', 'abdf', 'e', 'jkl'])
    set(['a', 'ab', 'abd', 'j', 'jk'])
    """
    return {seq[:i] for seq in sequences for i in range(1, len(seq))}


# pylint: disable=too-many-positional-arguments
def resolve_sequence(text: str,
                     mapper: typing.Mapping[str, int],
                     codes: typing.Mapping[int, str],
                     prefixes: Optional[Set[str]] = None,
                     final: bool = False,
                     dec_mode_cache: Optional[Dict[int, int]] = None) -> Keystroke:
    r"""
    Return a single :class:`Keystroke` instance for given sequence ``text``.

    :arg str text: string of characters received from terminal input stream.
    :arg OrderedDict mapper: unicode multibyte sequences, such as ``'\x1b[D'``
        paired by their integer value (260)
    :arg dict codes: a :type:`dict` of integer values (such as 260) paired
        by their mnemonic name, such as ``'KEY_LEFT'``.
    :arg set prefixes: Set of all valid sequence prefixes for quick matching
    :arg bool final: Whether this is the final resolution attempt (no more input expected)
    :arg dict dec_mode_cache: Dictionary of DEC private mode states (mode number -> state value)
    :rtype: Keystroke
    :returns: Keystroke instance for the given sequence

    The given ``text`` may extend beyond a matching sequence, such as
    ``u\x1b[Dxxx`` returns a :class:`Keystroke` instance of attribute
    :attr:`Keystroke.sequence` valued only ``u\x1b[D``.  It is up to
    calls to determine that ``xxx`` remains unresolved.

    In an ideal world, we could detect and resolve only for key sequences
    expected in the current terminal mode. For example, only the enabling of
    mode 1036 (META_SENDS_ESC) would match for 2-character ESC+char sequences.

    But terminals are unpredictable, the popular terminal "Konsole" does
    not negotiate about any DEC Private modes but transmits metaSendsEscape
    anyway, so exhaustive match is performed in all cases.
    """
    # Handle None prefixes (convert to empty set for compatibility)
    if prefixes is None:
        prefixes = set()

    # First try advanced keyboard protocol matchers and DEC events
    ks = None
    for match_fn in (
            functools.partial(_match_dec_event, dec_mode_cache=dec_mode_cache),
            _match_kitty_key,
            _match_modify_other_keys,
            _match_legacy_csi_letter_form,
            _match_legacy_csi_tilde_form,
            _match_legacy_ss3_fkey_form):
        ks = match_fn(text)
        if ks:
            break

    # Then try static sequence lookups from terminal capabilities
    if ks is None:
        # Note: mapper is sorted longest-first, so '\x1b[A' matches KEY_UP, not KEY_EXIT.
        for sequence, code in mapper.items():
            if text.startswith(sequence):
                ks = Keystroke(ucs=sequence, code=code, name=codes[code])
                break

    # Check for metaSendsEscape (Alt+key) or CSI fallback
    # Only fallback when no modern protocol has matched
    is_meta_escape = (
        text.startswith('\x1b')
        and len(text) >= 2
        and (final or text[:2] not in prefixes)
        # pylint:disable=protected-access
        and (ks is None or (ks.code == curses.KEY_EXIT and ks._mode is None))
    )
    if is_meta_escape:
        ks = Keystroke(ucs=text[:2])

    # final match is just simple resolution of the first codepoint of text
    if ks is None:
        ks = Keystroke(ucs=text and text[0] or '')
    return ks


def _time_left(stime: float, timeout: Optional[float]) -> Optional[float]:
    """
    Return time remaining since ``stime`` before given ``timeout``.

    This function assists determining the value of ``timeout`` for
    class method :meth:`~.Terminal.kbhit` and similar functions.

    :arg float stime: starting time for measurement
    :arg float timeout: timeout period, may be set to None to
       indicate no timeout (where None is always returned).
    :rtype: float or int
    :returns: time remaining as float. If no time is remaining,
       then the integer ``0`` is returned.
    """
    return max(0, timeout - (time.time() - stime)) if timeout else timeout


def _read_until(term: 'Terminal',
                pattern: str,
                timeout: typing.Optional[float]
                ) -> typing.Tuple[typing.Optional[Match[str]], str]:
    """
    Convenience read-until-pattern function, supporting :meth:`~.get_location`.

    :arg blessed.Terminal term: :class:`~.Terminal` instance.
    :arg float timeout: timeout period, may be set to None to indicate no
        timeout (where 0 is always returned).
    :arg str pattern: target regular expression pattern to seek.
    :rtype: tuple
    :returns: tuple in form of ``(match, str)``, *match*
        may be :class:`re.MatchObject` if pattern is discovered
        in input stream before timeout has elapsed, otherwise
        None. ``str`` is any remaining text received exclusive
        of the matching pattern).

    The reason a tuple containing non-matching data is returned, is that the
    consumer should push such data back into the input buffer by
    :meth:`~.Terminal.ungetch` if any was received.

    For example, when a user is performing rapid input keystrokes while its
    terminal emulator surreptitiously responds to this in-band sequence, we
    must ensure any such keyboard data is well-received by the next call to
    term.inkey() without delay.
    """
    # Maximum buffer size to prevent runaway condition -- one such example is automatic terminal
    # responses that get echoed back indefinitely in an accidental LINEMODE telnet server. 64KB is
    # far more than any legitimate automatic terminal response could be.
    max_buffer_size = 65536

    stime = time.time()
    match, buf = None, ''
    while True:  # pragma: no branch
        # block as long as necessary to ensure at least one character is
        # received on input or remaining timeout has elapsed.
        ucs = term.inkey(timeout=_time_left(stime, timeout), esc_delay=0)
        # while the keyboard buffer is "hot" (has input), we continue to
        # aggregate all awaiting data.  We do this to ensure slow I/O
        # calls do not unnecessarily give up within the first 'while' loop
        # for short timeout periods.
        while ucs:
            buf += ucs
            # Check buffer size limit to catch echo loops early
            if len(buf) > max_buffer_size:
                break
            ucs = term.inkey(timeout=0, esc_delay=0)

        match = re.search(pattern=pattern, string=buf)
        if match is not None:
            # match
            break

        if timeout is not None and not _time_left(stime, timeout):
            # timeout
            break

        if len(buf) > max_buffer_size:
            # buffer overflow - likely an echo loop or misbehaving terminal
            break

    return match, buf


def _match_dec_event(text: str,
                     dec_mode_cache: Optional[Dict[int,
                                                   int]] = None) -> Optional[Keystroke]:
    """
    Attempt to match text against DEC event patterns.

    Only matches patterns whose corresponding DEC modes are enabled in the cache.
    This prevents false positives like matching focus events when focus tracking is disabled.

    :arg str text: Input text to match against DEC patterns
    :arg dict dec_mode_cache: Dictionary of DEC private mode states (mode number -> state value)
    :rtype: Keystroke or None
    :returns: :class:`Keystroke` with DEC event data if matched, ``None`` otherwise
    """
    from blessed.dec_modes import DecModeResponse  # pylint: disable=import-outside-toplevel

    if dec_mode_cache is None:
        dec_mode_cache = {}

    for mode, pattern in DEC_EVENT_PATTERNS:
        # Only match if the mode is enabled (state == SET)
        mode_state = dec_mode_cache.get(int(mode))
        if mode_state != DecModeResponse.SET:
            continue

        match = pattern.match(text)
        if match:
            return Keystroke(ucs=match.group(0), mode=mode, match=match)
    return None


def _match_kitty_key(text: str) -> Optional[Keystroke]:
    """
    Attempt to match text against Kitty keyboard protocol patterns.

    :arg str text: Input text to match against Kitty patterns
    :rtype: Keystroke or None
    :returns: :class:`Keystroke` with Kitty key data if matched, ``None`` otherwise

    Supports Kitty keyboard protocol sequences of the form:
    CSI unicode-key-code u                                            # Basic form
    CSI unicode-key-code ; modifiers u                                # With modifiers
    CSI unicode-key-code : shifted-key : base-key ; modifiers u       # With alternate keys
    CSI unicode-key-code ; modifiers : event-type u                   # With event type
    CSI unicode-key-code ; modifiers : event-type ; text-codepoints u # Full form
    """
    match = RE_PATTERN_KITTY_KB_PROTOCOL.match(text)

    def int_when_non_empty(_m: Match[str], _key: str) -> Optional[int]:
        return int(_m.group(_key)) if _m.group(_key) else None

    def int_when_non_empty_otherwise_1(_m: Match[str], _key: str) -> int:
        return int(_m.group(_key)) if _m.group(_key) else 1

    if match:
        _int_codepoints: typing.Tuple[int, ...] = tuple()
        if match.group('text_codepoints'):
            _codepoints_text = match.group('text_codepoints').split(':')
            _int_codepoints = tuple(int(cp) for cp in _codepoints_text if cp)

        unicode_key = int(match.group('unicode_key'))

        # Create KittyKeyEvent namedtuple
        kitty_event = KittyKeyEvent(
            unicode_key=unicode_key,
            shifted_key=int_when_non_empty(match, 'shifted_key'),
            base_key=int_when_non_empty(match, 'base_key'),
            modifiers=int_when_non_empty_otherwise_1(match, 'modifiers'),
            event_type=int_when_non_empty_otherwise_1(match, 'event_type'),
            int_codepoints=_int_codepoints
        )

        # Map control characters and PUA functional key codes to their key constants
        keycode = (_KITTY_CONTROL_CHAR_TO_KEYCODE.get(unicode_key) or
                   (unicode_key if _is_kitty_functional_key(unicode_key) else None))

        # Create Keystroke with mode=-1 to indicate Kitty protocol
        return Keystroke(ucs=match.group(0),
                         code=keycode,
                         mode=-1,
                         match=kitty_event)

    return None


def _match_modify_other_keys(text: str) -> Optional['Keystroke']:
    """
    Attempt to match text against xterm ModifyOtherKeys patterns.

    :arg str text: Input text to match against ModifyOtherKeys patterns
    :rtype: Keystroke or None
    :returns: :class:`Keystroke` when matched, otherwise ``None``.

    Supports xterm ModifyOtherKeys sequences of the form:
    ESC [ 27 ; modifiers ; key ~     # Standard form
    ESC [ 27 ; modifiers ; key       # Alternative form without trailing ~
    """
    match = RE_PATTERN_MODIFY_OTHER.match(text)
    if match:
        # Create ModifyOtherKeysEvent namedtuple
        modify_event = ModifyOtherKeysEvent(
            key=int(match.group('key')),
            modifiers=int(match.group('modifiers')))
        # Create Keystroke with mode=-2 to indicate ModifyOtherKeys protocol
        return Keystroke(ucs=match.group(0),
                         mode=-2,
                         match=modify_event)

    return None


def _match_legacy_csi_letter_form(text: str) -> Optional[Keystroke]:
    """
    Match legacy CSI letter form: ESC [ 1 ; modifiers [ABCDEFHPQRS].

    :arg str text: Input text to match
    :rtype: Keystroke or None
    :returns: :class:`Keystroke` if matched, ``None`` otherwise

    Handles arrow keys, Home/End, F1-F4 with modifiers.
    """
    match = RE_PATTERN_LEGACY_CSI_MODIFIERS.match(text)
    if not match:
        return None

    modifiers = int(match.group('mod'))
    key_id = match.group('final')
    matched_text = match.group(0)
    event_type = int(match.group('event')) if match.group('event') else 1

    # guaranteed not to raise KeyError by regex (ABCDEFHPQRS)
    keycode = CSI_FINAL_CHAR_TO_KEYCODE[key_id]

    legacy_event = LegacyCSIKeyEvent(
        kind='letter',
        key_id=key_id,
        modifiers=modifiers,
        event_type=event_type)

    return Keystroke(ucs=matched_text, code=keycode, mode=-3, match=legacy_event)


def _match_legacy_csi_tilde_form(text: str) -> Optional[Keystroke]:
    """
    Match legacy CSI tilde form: ESC [ number ; modifiers ~.

    :arg str text: Input text to match
    :rtype: Keystroke or None
    :returns: :class:`Keystroke` if matched, ``None`` otherwise

    Handles function keys and navigation keys (Insert, Delete, Page Up/Down, etc.)
    with modifiers. See https://tomscii.sig7.se/zutty/doc/KEYS.html and
    https://invisible-island.net/xterm/xterm-function-keys.html for reference.
    """
    match = RE_PATTERN_LEGACY_CSI_TILDE.match(text)
    if not match:
        return None

    modifiers = int(match.group('mod'))
    key_id = int(match.group('key_num'))
    matched_text = match.group(0)
    event_type = int(match.group('event')) if match.group('event') else 1

    keycode = CSI_TILDE_NUM_TO_KEYCODE.get(key_id)
    if keycode is None:
        return None

    legacy_event = LegacyCSIKeyEvent(
        kind='tilde',
        key_id=key_id,
        modifiers=modifiers,
        event_type=event_type)

    return Keystroke(ucs=matched_text, code=keycode, mode=-3, match=legacy_event)


def _match_legacy_ss3_fkey_form(text: str) -> Optional[Keystroke]:
    """
    Match legacy SS3 F-key form: ESC O modifier [PQRS].

    :arg str text: Input text to match
    :rtype: Keystroke or None
    :returns: :class:`Keystroke` if matched, ``None`` otherwise

    Handles F1-F4 with modifiers in SS3 format (used by Konsole and others).
    """
    match = RE_PATTERN_LEGACY_SS3_FKEYS.match(text)
    if not match:
        return None

    modifiers = int(match.group('mod'))
    final_char = match.group('final')
    matched_text = match.group(0)

    # Modifier 0 is invalid - modifiers start from 1 (no modifiers)
    if modifiers == 0:
        return None

    # guaranteed not to raise KeyError by regex (PQRS)
    keycode = SS3_FKEY_TO_KEYCODE[final_char]

    # SS3 form doesn't support event_type, default to 1 (press)
    legacy_event = LegacyCSIKeyEvent(
        kind='ss3-fkey',
        key_id=final_char,
        modifiers=modifiers,
        event_type=1)

    return Keystroke(ucs=matched_text, code=keycode, mode=-3, match=legacy_event)


# We invent a few to fixup for missing keys in curses, these aren't especially
# required or useful except to survive as API compatibility for the earliest
# versions of this software. They must be these values in this order, used as
# constants for equality checks to Keystroke.code.
KEY_TAB = 512
KEY_KP_MULTIPLY = 513
KEY_KP_ADD = 514
KEY_KP_SEPARATOR = 515
KEY_KP_SUBTRACT = 516
KEY_KP_DECIMAL = 517
KEY_KP_DIVIDE = 518
KEY_KP_EQUAL = 519
KEY_KP_0 = 520
KEY_KP_1 = 521
KEY_KP_2 = 522
KEY_KP_3 = 523
KEY_KP_4 = 524
KEY_KP_5 = 525
KEY_KP_6 = 526
KEY_KP_7 = 527
KEY_KP_8 = 528
KEY_KP_9 = 529
KEY_MENU = 530

# Kitty protocol control character to keycode mapping
# Maps common control character unicode values to their curses keycodes
# so they get proper names when received via Kitty protocol
_KITTY_CONTROL_CHAR_TO_KEYCODE = {
    27: curses.KEY_EXIT,        # Escape
    9: KEY_TAB,                 # Tab
    13: curses.KEY_ENTER,       # Enter/Return
    127: curses.KEY_BACKSPACE,  # Backspace/Delete
}

# Kitty keyboard protocol PUA (Private Use Area) key codes
# These map to functional keys in the Kitty keyboard protocol spec
# PUA starts at 57344 (0xE000), keypad keys at 57399, modifier keys at 57441
KEY_KP_0_PUA = 57399            # 0xE047
KEY_KP_1_PUA = 57400            # 0xE048
KEY_KP_2_PUA = 57401            # 0xE049
KEY_KP_3_PUA = 57402            # 0xE04A
KEY_KP_4_PUA = 57403            # 0xE04B
KEY_KP_5_PUA = 57404            # 0xE04C
KEY_KP_6_PUA = 57405            # 0xE04D
KEY_KP_7_PUA = 57406            # 0xE04E
KEY_KP_8_PUA = 57407            # 0xE04F
KEY_KP_9_PUA = 57408            # 0xE050
KEY_KP_DECIMAL_PUA = 57409      # 0xE051
KEY_KP_DIVIDE_PUA = 57410       # 0xE052
KEY_KP_MULTIPLY_PUA = 57411     # 0xE053
KEY_KP_SUBTRACT_PUA = 57412     # 0xE054
KEY_KP_ADD_PUA = 57413          # 0xE055
KEY_KP_ENTER_PUA = 57414        # 0xE056
KEY_KP_EQUAL_PUA = 57415        # 0xE057
KEY_KP_SEPARATOR_PUA = 57416    # 0xE058
KEY_KP_LEFT_PUA = 57417         # 0xE059
KEY_KP_RIGHT_PUA = 57418        # 0xE05A
KEY_KP_UP_PUA = 57419           # 0xE05B
KEY_KP_DOWN_PUA = 57420         # 0xE05C
KEY_KP_PAGE_UP_PUA = 57421      # 0xE05D
KEY_KP_PAGE_DOWN_PUA = 57422    # 0xE05E
KEY_KP_HOME_PUA = 57423         # 0xE05F
KEY_KP_END_PUA = 57424          # 0xE060
KEY_KP_INSERT_PUA = 57425       # 0xE061
KEY_KP_DELETE_PUA = 57426       # 0xE062
KEY_KP_BEGIN_PUA = 57427        # 0xE063

# Lock and special function keys
KEY_CAPS_LOCK_PUA = 57358       # 0xE02E
KEY_SCROLL_LOCK_PUA = 57359     # 0xE02F
KEY_NUM_LOCK_PUA = 57360        # 0xE030
KEY_PRINT_SCREEN_PUA = 57361    # 0xE031
KEY_PAUSE_PUA = 57362           # 0xE032
KEY_MENU_PUA = 57363            # 0xE033

# Extended function keys F13-F35
KEY_F13_PUA = 57376             # 0xE040
KEY_F14_PUA = 57377             # 0xE041
KEY_F15_PUA = 57378             # 0xE042
KEY_F16_PUA = 57379             # 0xE043
KEY_F17_PUA = 57380             # 0xE044
KEY_F18_PUA = 57381             # 0xE045
KEY_F19_PUA = 57382             # 0xE046
KEY_F20_PUA = 57383             # 0xE047
KEY_F21_PUA = 57384             # 0xE048
KEY_F22_PUA = 57385             # 0xE049
KEY_F23_PUA = 57386             # 0xE04A
KEY_F24_PUA = 57387             # 0xE04B
KEY_F25_PUA = 57388             # 0xE04C
KEY_F26_PUA = 57389             # 0xE04D
KEY_F27_PUA = 57390             # 0xE04E
KEY_F28_PUA = 57391             # 0xE04F
KEY_F29_PUA = 57392             # 0xE050
KEY_F30_PUA = 57393             # 0xE051
KEY_F31_PUA = 57394             # 0xE052
KEY_F32_PUA = 57395             # 0xE053
KEY_F33_PUA = 57396             # 0xE054
KEY_F34_PUA = 57397             # 0xE055
KEY_F35_PUA = 57398             # 0xE056

# Media control keys
KEY_MEDIA_PLAY_PUA = 57428      # 0xE064
KEY_MEDIA_PAUSE_PUA = 57429     # 0xE065
KEY_MEDIA_PLAY_PAUSE_PUA = 57430  # 0xE066
KEY_MEDIA_REVERSE_PUA = 57431   # 0xE067
KEY_MEDIA_STOP_PUA = 57432      # 0xE068
KEY_MEDIA_FAST_FORWARD_PUA = 57433  # 0xE069
KEY_MEDIA_REWIND_PUA = 57434    # 0xE06A
KEY_MEDIA_TRACK_NEXT_PUA = 57435  # 0xE06B
KEY_MEDIA_TRACK_PREVIOUS_PUA = 57436  # 0xE06C
KEY_MEDIA_RECORD_PUA = 57437    # 0xE06D
KEY_LOWER_VOLUME_PUA = 57438    # 0xE06E
KEY_RAISE_VOLUME_PUA = 57439    # 0xE06F
KEY_MUTE_VOLUME_PUA = 57440     # 0xE070

# Modifier keys (left/right variants)
KEY_LEFT_SHIFT = 57441          # 0xE071 - Modifier keys
KEY_LEFT_CONTROL = 57442        # 0xE062
KEY_LEFT_ALT = 57443            # 0xE063
KEY_LEFT_SUPER = 57444          # 0xE064
KEY_LEFT_HYPER = 57445          # 0xE065
KEY_LEFT_META = 57446           # 0xE066
KEY_RIGHT_SHIFT = 57447         # 0xE067
KEY_RIGHT_CONTROL = 57448       # 0xE068
KEY_RIGHT_ALT = 57449           # 0xE069
KEY_RIGHT_SUPER = 57450         # 0xE06A
KEY_RIGHT_HYPER = 57451         # 0xE06B
KEY_RIGHT_META = 57452          # 0xE06C

# ISO level shift keys
KEY_ISO_LEVEL3_SHIFT_PUA = 57453  # 0xE07D
KEY_ISO_LEVEL5_SHIFT_PUA = 57454  # 0xE07E

#: Kitty PUA functional key ranges from the keyboard protocol specification
#: These define which unicode_key values should be treated as functional keys
#: Format: (start, end) tuples for inclusive ranges
_KITTY_FUNCTIONAL_KEY_RANGES = (
    (57358, 57363),  # Lock and special function keys (CAPS_LOCK, SCROLL_LOCK, NUM_LOCK, etc.)
    (57376, 57398),  # Extended F-keys (F13-F35)
    (57399, 57427),  # Keypad keys (KP_0 through KP_BEGIN)
    (57428, 57440),  # Media control keys (MEDIA_PLAY, LOWER_VOLUME, etc.)
    (57441, 57452),  # Modifier keys (LEFT_SHIFT, RIGHT_CONTROL, etc.)
    (57453, 57454),  # ISO level shift keys (ISO_LEVEL3_SHIFT, ISO_LEVEL5_SHIFT)
)


def _is_kitty_functional_key(unicode_key: int) -> bool:
    """Check if a unicode key code is a Kitty protocol PUA functional key."""
    return any(start <= unicode_key <= end for start, end in _KITTY_FUNCTIONAL_KEY_RANGES)


#: Legacy CSI modifier sequence mappings
#: Maps CSI final characters to curses keycodes for sequences
#: like ESC [ 1 ; mod [ABCDEFHPQRS]
CSI_FINAL_CHAR_TO_KEYCODE = {
    'A': curses.KEY_UP,
    'B': curses.KEY_DOWN,
    'C': curses.KEY_RIGHT,
    'D': curses.KEY_LEFT,
    'E': curses.KEY_B2,      # Center/Begin
    'F': curses.KEY_END,
    'H': curses.KEY_HOME,
    'P': curses.KEY_F1,
    'Q': curses.KEY_F2,
    'R': curses.KEY_F3,
    'S': curses.KEY_F4,
}

#: Maps CSI tilde numbers to curses keycodes for sequences
#: like ESC [ num ; mod ~
CSI_TILDE_NUM_TO_KEYCODE = {
    2: curses.KEY_IC,        # Insert
    3: curses.KEY_DC,        # Delete
    5: curses.KEY_PPAGE,     # Page Up
    6: curses.KEY_NPAGE,     # Page Down
    7: curses.KEY_HOME,      # Home
    8: curses.KEY_END,       # End
    11: curses.KEY_F1,       # F1
    12: curses.KEY_F2,       # F2
    13: curses.KEY_F3,       # F3
    14: curses.KEY_F4,       # F4
    15: curses.KEY_F5,       # F5
    17: curses.KEY_F6,       # F6
    18: curses.KEY_F7,       # F7
    19: curses.KEY_F8,       # F8
    20: curses.KEY_F9,       # F9
    21: curses.KEY_F10,      # F10
    23: curses.KEY_F11,      # F11
    24: curses.KEY_F12,      # F12
    25: curses.KEY_F13,      # F13
    26: curses.KEY_F14,      # F14
    28: curses.KEY_F15,      # F15
    29: KEY_MENU,            # Menu
    31: curses.KEY_F17,      # F17
    32: curses.KEY_F18,      # F18
    33: curses.KEY_F19,      # F19
    34: curses.KEY_F20,      # F20
}

#: Maps SS3 final characters to curses keycodes for sequences
#: like ESC O mod [PQRS]
SS3_FKEY_TO_KEYCODE = {
    'P': curses.KEY_F1,      # F1
    'Q': curses.KEY_F2,      # F2
    'R': curses.KEY_F3,      # F3
    'S': curses.KEY_F4,      # F4
}

#: In a perfect world, terminal emulators would always send exactly what
#: the terminfo(5) capability database plans for them, accordingly by the
#: value of the ``TERM`` name they declare.
#:
#: But this isn't a perfect world. Many vt220-derived terminals, such as
#: those declaring 'xterm', will continue to send vt220 codes instead of
#: their native-declared codes, for backwards-compatibility.
#:
#: This goes for many: rxvt, putty, iTerm.
#:
#: These "mixins" are used for *all* terminals, regardless of their type.
#:
#: Furthermore, curses does not provide sequences sent by the keypad,
#: at least, it does not provide a way to distinguish between keypad 0
#: and numeric 0.
DEFAULT_SEQUENCE_MIXIN = (
    # these common control characters (and 127, ctrl+'?') mapped to
    # an application key definition.
    (chr(10), curses.KEY_ENTER),
    (chr(13), curses.KEY_ENTER),
    (chr(8), curses.KEY_BACKSPACE),
    (chr(9), KEY_TAB),  # noqa
    (chr(27), curses.KEY_EXIT),
    (chr(127), curses.KEY_BACKSPACE),

    ("\x1b[A", curses.KEY_UP),
    ("\x1b[B", curses.KEY_DOWN),
    ("\x1b[C", curses.KEY_RIGHT),
    ("\x1b[D", curses.KEY_LEFT),
    ("\x1b[E", curses.KEY_B2),  # Center/Begin key
    ("\x1b[1;2A", curses.KEY_SR),
    ("\x1b[1;2B", curses.KEY_SF),
    ("\x1b[1;2C", curses.KEY_SRIGHT),
    ("\x1b[1;2D", curses.KEY_SLEFT),
    ("\x1b[F", curses.KEY_END),
    ("\x1b[H", curses.KEY_HOME),
    # not sure where these are from .. please report
    ("\x1b[K", curses.KEY_END),
    ("\x1b[U", curses.KEY_NPAGE),
    ("\x1b[V", curses.KEY_PPAGE),

    # keys sent after term.smkx (keypad_xmit) is emitted, source:
    # http://www.xfree86.org/current/ctlseqs.html#PC-Style%20Function%20Keys
    # http://fossies.org/linux/rxvt/doc/rxvtRef.html#KeyCodes
    #
    # keypad, numlock on
    ("\x1bOM", curses.KEY_ENTER),
    ("\x1bOj", KEY_KP_MULTIPLY),
    ("\x1bOk", KEY_KP_ADD),
    ("\x1bOl", KEY_KP_SEPARATOR),
    ("\x1bOm", KEY_KP_SUBTRACT),
    ("\x1bOn", KEY_KP_DECIMAL),
    ("\x1bOo", KEY_KP_DIVIDE),
    ("\x1bOX", KEY_KP_EQUAL),
    ("\x1bOp", KEY_KP_0),
    ("\x1bOq", KEY_KP_1),
    ("\x1bOr", KEY_KP_2),
    ("\x1bOs", KEY_KP_3),
    ("\x1bOt", KEY_KP_4),
    ("\x1bOu", KEY_KP_5),
    ("\x1bOv", KEY_KP_6),
    ("\x1bOw", KEY_KP_7),
    ("\x1bOx", KEY_KP_8),
    ("\x1bOy", KEY_KP_9),

    # keypad, numlock off
    ("\x1b[1~", curses.KEY_HOME),         # home
    ("\x1b[2~", curses.KEY_IC),           # insert (0)
    ("\x1b[3~", curses.KEY_DC),           # delete (.), "Execute"
    ("\x1b[4~", curses.KEY_END),          # end
    ("\x1b[5~", curses.KEY_PPAGE),        # pgup   (9)
    ("\x1b[6~", curses.KEY_NPAGE),        # pgdown (3)
    ("\x1b[7~", curses.KEY_HOME),         # home
    ("\x1b[8~", curses.KEY_END),          # end
    ("\x1b[OA", curses.KEY_UP),           # up     (8)
    ("\x1b[OB", curses.KEY_DOWN),         # down   (2)
    ("\x1b[OC", curses.KEY_RIGHT),        # right  (6)
    ("\x1b[OD", curses.KEY_LEFT),         # left   (4)
    ("\x1b[OF", curses.KEY_END),          # end    (1)
    ("\x1b[OH", curses.KEY_HOME),         # home   (7)

    # The vt220 placed F1-F4 above the keypad, in place of actual
    # F1-F4 were local functions (hold screen, print screen,
    # set up, data/talk, break).
    ("\x1bOP", curses.KEY_F1),
    ("\x1bOQ", curses.KEY_F2),
    ("\x1bOR", curses.KEY_F3),
    ("\x1bOS", curses.KEY_F4),

    # Kitty disambiguate mode F-keys (CSI form instead of SS3)
    ("\x1b[P", curses.KEY_F1),
    ("\x1b[Q", curses.KEY_F2),
    ("\x1b[13~", curses.KEY_F3),
    ("\x1b[S", curses.KEY_F4),
)

#: Override mixins for a few curses constants with easier
#: mnemonics: there may only be a 1:1 mapping when only a
#: keycode (int) is given, where these phrases are preferred.
CURSES_KEYCODE_OVERRIDE_MIXIN = (
    ('KEY_DELETE', curses.KEY_DC),
    ('KEY_INSERT', curses.KEY_IC),
    ('KEY_PGUP', curses.KEY_PPAGE),
    ('KEY_PGDOWN', curses.KEY_NPAGE),
    ('KEY_ESCAPE', curses.KEY_EXIT),
    ('KEY_SUP', curses.KEY_SR),
    ('KEY_SDOWN', curses.KEY_SF),
    ('KEY_UP_LEFT', curses.KEY_A1),
    ('KEY_UP_RIGHT', curses.KEY_A3),
    ('KEY_CENTER', curses.KEY_B2),
    ('KEY_BEGIN', curses.KEY_BEG),
    ('KEY_DOWN_LEFT', curses.KEY_C1),
    ('KEY_DOWN_RIGHT', curses.KEY_C3),
)

#: PUA keycode overrides to remove _PUA suffix from user-facing names.
#: The _PUA constants are internal implementation details for Kitty protocol keys;
#: users should see clean names like 'KEY_CAPS_LOCK' instead of 'KEY_CAPS_LOCK_PUA'.
#: Format: (clean_name, pua_keycode)
#:
#: Note: Keys with legacy non-PUA constants are NOT included here:
#:  - KEY_KP_0 through KEY_KP_9 (520-529) - used by DEFAULT_SEQUENCE_MIXIN (\x1bOp-\x1bOy)
#:  - KEY_KP_MULTIPLY, KEY_KP_ADD, KEY_KP_SEPARATOR, KEY_KP_SUBTRACT, KEY_KP_DECIMAL,
#:    KEY_KP_DIVIDE, KEY_KP_EQUAL (513-519) - used by DEFAULT_SEQUENCE_MIXIN (\x1bOj-\x1bOX)
#: The PUA versions of these keys are handled directly by _match_kitty_key() setting
#: the keycode, and _get_modified_keycode_name() generates the proper name.
KITTY_PUA_KEYCODE_OVERRIDE_MIXIN = (
    # Keypad navigation/editing keys (excluding digits and operators with legacy versions)
    ('KEY_KP_ENTER', KEY_KP_ENTER_PUA),
    ('KEY_KP_LEFT', KEY_KP_LEFT_PUA),
    ('KEY_KP_RIGHT', KEY_KP_RIGHT_PUA),
    ('KEY_KP_UP', KEY_KP_UP_PUA),
    ('KEY_KP_DOWN', KEY_KP_DOWN_PUA),
    ('KEY_KP_PAGE_UP', KEY_KP_PAGE_UP_PUA),
    ('KEY_KP_PAGE_DOWN', KEY_KP_PAGE_DOWN_PUA),
    ('KEY_KP_HOME', KEY_KP_HOME_PUA),
    ('KEY_KP_END', KEY_KP_END_PUA),
    ('KEY_KP_INSERT', KEY_KP_INSERT_PUA),
    ('KEY_KP_DELETE', KEY_KP_DELETE_PUA),
    ('KEY_KP_BEGIN', KEY_KP_BEGIN_PUA),
    # Lock and special function keys
    ('KEY_CAPS_LOCK', KEY_CAPS_LOCK_PUA),
    ('KEY_SCROLL_LOCK', KEY_SCROLL_LOCK_PUA),
    ('KEY_NUM_LOCK', KEY_NUM_LOCK_PUA),
    ('KEY_PRINT_SCREEN', KEY_PRINT_SCREEN_PUA),
    ('KEY_PAUSE', KEY_PAUSE_PUA),
    ('KEY_MENU', KEY_MENU_PUA),
    # Extended F-keys
    ('KEY_F13', KEY_F13_PUA),
    ('KEY_F14', KEY_F14_PUA),
    ('KEY_F15', KEY_F15_PUA),
    ('KEY_F16', KEY_F16_PUA),
    ('KEY_F17', KEY_F17_PUA),
    ('KEY_F18', KEY_F18_PUA),
    ('KEY_F19', KEY_F19_PUA),
    ('KEY_F20', KEY_F20_PUA),
    ('KEY_F21', KEY_F21_PUA),
    ('KEY_F22', KEY_F22_PUA),
    ('KEY_F23', KEY_F23_PUA),
    ('KEY_F24', KEY_F24_PUA),
    ('KEY_F25', KEY_F25_PUA),
    ('KEY_F26', KEY_F26_PUA),
    ('KEY_F27', KEY_F27_PUA),
    ('KEY_F28', KEY_F28_PUA),
    ('KEY_F29', KEY_F29_PUA),
    ('KEY_F30', KEY_F30_PUA),
    ('KEY_F31', KEY_F31_PUA),
    ('KEY_F32', KEY_F32_PUA),
    ('KEY_F33', KEY_F33_PUA),
    ('KEY_F34', KEY_F34_PUA),
    ('KEY_F35', KEY_F35_PUA),
    # Media control keys
    ('KEY_MEDIA_PLAY', KEY_MEDIA_PLAY_PUA),
    ('KEY_MEDIA_PAUSE', KEY_MEDIA_PAUSE_PUA),
    ('KEY_MEDIA_PLAY_PAUSE', KEY_MEDIA_PLAY_PAUSE_PUA),
    ('KEY_MEDIA_REVERSE', KEY_MEDIA_REVERSE_PUA),
    ('KEY_MEDIA_STOP', KEY_MEDIA_STOP_PUA),
    ('KEY_MEDIA_FAST_FORWARD', KEY_MEDIA_FAST_FORWARD_PUA),
    ('KEY_MEDIA_REWIND', KEY_MEDIA_REWIND_PUA),
    ('KEY_MEDIA_TRACK_NEXT', KEY_MEDIA_TRACK_NEXT_PUA),
    ('KEY_MEDIA_TRACK_PREVIOUS', KEY_MEDIA_TRACK_PREVIOUS_PUA),
    ('KEY_MEDIA_RECORD', KEY_MEDIA_RECORD_PUA),
    ('KEY_LOWER_VOLUME', KEY_LOWER_VOLUME_PUA),
    ('KEY_RAISE_VOLUME', KEY_RAISE_VOLUME_PUA),
    ('KEY_MUTE_VOLUME', KEY_MUTE_VOLUME_PUA),
    # ISO level shift keys
    ('KEY_ISO_LEVEL3_SHIFT', KEY_ISO_LEVEL3_SHIFT_PUA),
    ('KEY_ISO_LEVEL5_SHIFT', KEY_ISO_LEVEL5_SHIFT_PUA),
)

#: Default delay, in seconds, of Escape key detection in
#: :meth:`Terminal.inkey`.` curses has a default delay of 1000ms (1 second) for
#: escape sequences.  This is too long for modern applications, so we set it to
#: 350ms, or 0.35 seconds. It is still a bit conservative, for remote telnet or
#: ssh servers, for example.
DEFAULT_ESCDELAY = 0.35


def _reinit_escdelay() -> None:
    # pylint: disable=global-statement
    # Using the global statement: this is necessary to
    # allow test coverage without complex module reload
    global DEFAULT_ESCDELAY
    if os.environ.get('ESCDELAY'):
        try:
            DEFAULT_ESCDELAY = int(os.environ['ESCDELAY']) / 1000.0
        except ValueError:
            # invalid values of 'ESCDELAY' are ignored
            pass


_reinit_escdelay()


class DeviceAttribute():
    """
    Represents a terminal's Device Attributes (DA1) response.

    Device Attributes queries allow discovering terminal capabilities and type.
    The primary DA1 query sends CSI c and expects a response like::

        CSI ? Psc ; Ps1 ; Ps2 ; ... ; Psn c

    Where Psc is the service class (architectural class) and Ps1...Psn are
    supported extensions/capabilities.
    """

    RE_RESPONSE = re.compile(r'\x1b\[\?([0-9]+)((?:;[0-9]+)*)c')

    def __init__(self, raw: str, service_class: int,
                 extensions: typing.Optional[typing.List[int]]) -> None:
        """
        Initialize DeviceAttribute instance.

        :arg str raw: Original response string from terminal
        :arg int service_class: Service class number (first parameter)
        :arg list extensions: List of extension numbers (remaining parameters)
        """
        self.raw = raw
        self.service_class = service_class
        self.extensions = set(extensions) if extensions else set()

    @property
    def supports_sixel(self) -> bool:
        """
        Whether the terminal supports sixel graphics.

        :rtype: bool
        :returns: True if extension 4 (sixel) is present in device attributes
        """
        return 4 in self.extensions

    @classmethod
    def from_match(cls, match: Match[str]) -> 'DeviceAttribute':
        """
        Create DeviceAttribute from regex match object.

        :arg re.Match match: Regex match object with groups for service_class and extensions
        :rtype: DeviceAttribute
        :returns: DeviceAttribute instance parsed from match
        """
        service_class = int(match.group(1))
        extensions_str = match.group(2)
        extensions: typing.List[int] = []

        if extensions_str:
            # Remove leading semicolon and split by semicolon
            ext_parts = extensions_str.lstrip(';').split(';')
            for part in ext_parts:
                if part.strip() and part.isdigit():
                    extensions.append(int(part.strip()))

        return cls(match.group(0), service_class, extensions)

    def __repr__(self) -> str:
        """String representation of DeviceAttribute."""
        return (f'DeviceAttribute(service_class={self.service_class}, '
                f'extensions={self.extensions}, supports_sixel={self.supports_sixel})')


class SoftwareVersion:
    """Represents a terminal's software name and version from XTVERSION response."""

    RE_RESPONSE = re.compile(r'\x1bP>\|(.+?)\x1b\\')

    def __init__(self, raw: str, name: str, version: str) -> None:
        """
        Initialize SoftwareVersion instance.

        :arg str raw: Original response string from terminal
        :arg str name: Software name (e.g., "kitty", "XTerm")
        :arg str version: Version string (e.g., "0.24.2", "367") or empty string if no version
        """
        self.raw = raw
        self.name = name
        self.version = version

    @classmethod
    def from_match(cls, match: Match[str]) -> 'SoftwareVersion':
        """
        Create SoftwareVersion from regex match object.

        :arg re.Match match: Regex match object with group for software text
        :rtype: SoftwareVersion
        :returns: SoftwareVersion instance parsed from match
        """
        text = match.group(1)
        name, version = cls._parse_text(text)
        return cls(match.group(0), name, version)

    @staticmethod
    def _parse_text(text: str) -> typing.Tuple[str, str]:
        """
        Parse software name and version from text.

        Parsing logic (in order):
        1. Check for space-separated format: "tmux 3.2a" or "X.Org 7.7.0(370)"
        2. Check for parentheses format: "kitty(0.24.2)"
        3. Name-only format: "software" (version is empty string)

        :arg str text: Text from XTVERSION response
        :rtype: tuple
        :returns: Tuple of (name, version) where both are strings
        """
        # Check for space-separated format first
        if ' ' in text:
            name, version = text.split(' ', 1)
            return name, version

        # Check for parentheses format
        if '(' in text:
            parts = text.split('(', 1)
            name = parts[0]
            version = parts[1].rstrip(')')
            return name, version

        # Name-only format
        return text, ''

    def __repr__(self) -> str:
        """String representation of SoftwareVersion."""
        return f'SoftwareVersion(name={self.name!r}, version={self.version!r})'


class KittyKeyboardProtocol:
    """
    Represents Kitty keyboard protocol flags.

    Encapsulates the integer flag value returned by Kitty keyboard protocol queries and provides
    properties for individual flag bits and a method to convert back to enable_kitty_keyboard()
    arguments.
    """

    def __init__(self, value: int) -> None:
        """
        Initialize with raw integer flag value.

        :arg int value: Raw integer flags value from Kitty keyboard protocol query
        """
        self.value = int(value)

    @property
    def disambiguate(self) -> bool:
        """Whether disambiguated escape codes are enabled (bit 1)."""
        return bool(self.value & 0b1)

    @disambiguate.setter
    def disambiguate(self, enabled: bool) -> None:
        """Set whether disambiguated escape codes are enabled (bit 1)."""
        if enabled:
            self.value |= 0b1
        else:
            self.value &= ~0b1

    @property
    def report_events(self) -> bool:
        """Whether key repeat and release events are reported (bit 2)."""
        return bool(self.value & 0b10)

    @report_events.setter
    def report_events(self, enabled: bool) -> None:
        """Set whether key repeat and release events are reported (bit 2)."""
        if enabled:
            self.value |= 0b10
        else:
            self.value &= ~0b10

    @property
    def report_alternates(self) -> bool:
        """Whether shifted and base layout keys are reported for shortcuts (bit 4)."""
        return bool(self.value & 0b100)

    @report_alternates.setter
    def report_alternates(self, enabled: bool) -> None:
        """Set whether shifted and base layout keys are reported for shortcuts (bit 4)."""
        if enabled:
            self.value |= 0b100
        else:
            self.value &= ~0b100

    @property
    def report_all_keys(self) -> bool:
        """Whether all keys are reported as escape codes (bit 8)."""
        return bool(self.value & 0b1000)

    @report_all_keys.setter
    def report_all_keys(self, enabled: bool) -> None:
        """Set whether all keys are reported as escape codes (bit 8)."""
        if enabled:
            self.value |= 0b1000
        else:
            self.value &= ~0b1000

    @property
    def report_text(self) -> bool:
        """Whether associated text is reported with key events (bit 16)."""
        return bool(self.value & 0b10000)

    @report_text.setter
    def report_text(self, enabled: bool) -> None:
        """Set whether associated text is reported with key events (bit 16)."""
        if enabled:
            self.value |= 0b10000
        else:
            self.value &= ~0b10000

    def make_arguments(self) -> Dict[str, bool]:
        """
        Return dictionary of arguments suitable for enable_kitty_keyboard().

        :rtype: dict
        :returns: Dictionary with boolean flags suitable for passing as keyword arguments to
            enable_kitty_keyboard()
        """
        return {
            'disambiguate': self.disambiguate,
            'report_events': self.report_events,
            'report_alternates': self.report_alternates,
            'report_all_keys': self.report_all_keys,
            'report_text': self.report_text
        }

    def __repr__(self) -> str:
        """Return string representation of the protocol flags."""
        flags = []
        if self.disambiguate:
            flags.append('disambiguate')
        if self.report_events:
            flags.append('report_events')
        if self.report_alternates:
            flags.append('report_alternates')
        if self.report_all_keys:
            flags.append('report_all_keys')
        if self.report_text:
            flags.append('report_text')

        return f"KittyKeyboardProtocol(value={self.value}, flags=[{', '.join(flags)}])"

    def __eq__(self, other: typing.Any) -> bool:
        """Check equality based on flag values."""
        if isinstance(other, KittyKeyboardProtocol):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return False


__all__ = ('Keystroke', 'get_keyboard_codes', 'get_keyboard_sequences',
           'KittyKeyEvent', 'ModifyOtherKeysEvent', 'LegacyCSIKeyEvent',
           'KittyKeyboardProtocol', 'DeviceAttribute', 'SoftwareVersion',
           'BracketedPasteEvent', 'FocusEvent', 'SyncEvent',)
