"""Terminal capability builder patterns."""
# std imports
import re
import typing
from typing import Dict, Optional
from collections import OrderedDict

__all__ = (
    'CAPABILITY_DATABASE',
    'CAPABILITIES_RAW_MIXIN',
    'CAPABILITIES_ADDITIVES',
    'CAPABILITIES_HORIZONTAL_DISTANCE',
    'CAPABILITIES_CAUSE_MOVEMENT',
    'XTGETTCAP_CAPABILITIES',
    'TermcapResponse',
    'ITerm2Capabilities',
)

CAPABILITY_DATABASE: \
    typing.OrderedDict[str, typing.Tuple[str, typing.Dict[str, typing.Any]]] = OrderedDict((
        ('bell', ('bel', {})),
        ('carriage_return', ('cr', {})),
        ('change_scroll_region', ('csr', {'nparams': 2})),
        ('clear_all_tabs', ('tbc', {})),
        ('clear_screen', ('clear', {})),
        ('clr_bol', ('el1', {})),
        ('clr_eol', ('el', {})),
        ('clr_eos', ('clear_eos', {})),
        ('column_address', ('hpa', {'nparams': 1})),
        ('cursor_address', ('cup', {'nparams': 2, 'match_grouped': True})),
        ('cursor_down', ('cud1', {})),
        ('cursor_home', ('home', {})),
        ('cursor_invisible', ('civis', {})),
        ('cursor_left', ('cub1', {})),
        ('cursor_normal', ('cnorm', {})),
        ('cursor_report', ('u6', {'nparams': 2, 'match_grouped': True})),
        ('cursor_right', ('cuf1', {})),
        ('cursor_up', ('cuu1', {})),
        ('cursor_visible', ('cvvis', {})),
        ('delete_character', ('dch1', {})),
        ('delete_line', ('dl1', {})),
        ('enter_blink_mode', ('blink', {})),
        ('enter_bold_mode', ('bold', {})),
        ('enter_dim_mode', ('dim', {})),
        ('enter_fullscreen', ('smcup', {})),
        ('enter_standout_mode', ('standout', {})),
        ('enter_superscript_mode', ('superscript', {})),
        ('enter_susimpleript_mode', ('susimpleript', {})),
        ('enter_underline_mode', ('underline', {})),
        ('erase_chars', ('ech', {'nparams': 1})),
        ('exit_alt_charset_mode', ('rmacs', {})),
        ('disable_line_wrap', ('rmam', {})),
        ('enable_line_wrap', ('smam', {})),
        ('exit_attribute_mode', ('sgr0', {})),
        ('exit_ca_mode', ('rmcup', {})),
        ('exit_fullscreen', ('rmcup', {})),
        ('exit_insert_mode', ('rmir', {})),
        ('exit_standout_mode', ('rmso', {})),
        ('exit_underline_mode', ('rmul', {})),
        ('flash_hook', ('hook', {})),
        ('flash_screen', ('flash', {})),
        ('insert_line', ('il1', {})),
        ('keypad_local', ('rmkx', {})),
        ('keypad_xmit', ('smkx', {})),
        ('meta_off', ('rmm', {})),
        ('meta_on', ('smm', {})),
        ('orig_pair', ('op', {})),
        ('parm_down_cursor', ('cud', {'nparams': 1})),
        ('parm_left_cursor', ('cub', {'nparams': 1, 'match_grouped': True})),
        ('parm_dch', ('dch', {'nparams': 1})),
        ('parm_delete_line', ('dl', {'nparams': 1})),
        ('parm_ich', ('ich', {'nparams': 1})),
        ('parm_index', ('indn', {'nparams': 1})),
        ('parm_insert_line', ('il', {'nparams': 1})),
        ('parm_right_cursor', ('cuf', {'nparams': 1, 'match_grouped': True})),
        ('parm_rindex', ('rin', {'nparams': 1})),
        ('parm_up_cursor', ('cuu', {'nparams': 1})),
        ('print_screen', ('mc0', {})),
        ('prtr_off', ('mc4', {})),
        ('prtr_on', ('mc5', {})),
        ('reset_1string', ('r1', {})),
        ('reset_2string', ('r2', {})),
        ('reset_3string', ('r3', {})),
        ('restore_cursor', ('rc', {})),
        ('row_address', ('vpa', {'nparams': 1})),
        ('save_cursor', ('sc', {})),
        ('scroll_forward', ('ind', {})),
        ('scroll_reverse', ('rev', {})),
        ('set0_des_seq', ('s0ds', {})),
        ('set1_des_seq', ('s1ds', {})),
        ('set2_des_seq', ('s2ds', {})),
        ('set3_des_seq', ('s3ds', {})),
        # this 'color' is deceiving, but often matching, and a better match
        # than set_a_attributes1 or set_a_foreground.
        ('color', ('_foreground_color', {'nparams': 1, 'match_any': True, 'numeric': 1})),
        ('set_a_foreground', ('color', {'nparams': 1, 'match_any': True, 'numeric': 1})),
        ('set_a_background', ('on_color', {'nparams': 1, 'match_any': True, 'numeric': 1})),
        ('set_tab', ('hts', {})),
        ('tab', ('ht', {})),
        ('italic', ('sitm', {})),
        ('no_italic', ('sitm', {})),
    ))

_ESC = re.escape('\x1b')
_CSI = rf'{_ESC}\['
_ANY_NOTESC = rf'[^{_ESC}]*'

CAPABILITIES_RAW_MIXIN: typing.Dict[str, str] = {
    'bell': re.escape('\a'),
    'carriage_return': re.escape('\r'),
    'cursor_left': re.escape('\b'),
    'cursor_report': rf'{_CSI}(\d+)\;(\d+)R',
    'cursor_right': rf'{_CSI}C',
    'exit_attribute_mode': rf'{_CSI}m',
    'parm_left_cursor': rf'{_CSI}(\d+)D',
    'parm_right_cursor': rf'{_CSI}(\d+)C',
    'restore_cursor': rf'{_CSI}u',
    'save_cursor': rf'{_CSI}s',
    'scroll_forward': re.escape('\n'),
    'set0_des_seq': re.escape('\x1b(B'),
    'tab': re.escape('\t'),
}


CAPABILITIES_ADDITIVES: typing.Dict[
    str, typing.Union[typing.Tuple[str, str, int], typing.Tuple[str, str]]] = {
    'link': (rf'{_ESC}\]8;{_ANY_NOTESC};{_ANY_NOTESC}(?:{_ESC}\\|\x07)', 'link', 1),
    'color256': (rf'{_CSI}38;5;\d+m', 'color', 1),
    'on_color256': (rf'{_CSI}48;5;\d+m', 'on_color', 1),
    'color_rgb': (rf'{_CSI}38;2;\d+;\d+;\d+m', 'color_rgb', 3),
    'on_color_rgb': (rf'{_CSI}48;2;\d+;\d+;\d+m', 'on_color_rgb', 3),
    'shift_in': (re.escape('\x0f'), ''),
    'shift_out': (re.escape('\x0e'), ''),
    # sgr(...) outputs strangely, use the basic ANSI/EMCA-48 codes here.
    'set_a_attributes1': (rf'{_CSI}\d+m', 'sgr', 1),
    'set_a_attributes2': (rf'{_CSI}\d+\;\d+m', 'sgr', 2),
    'set_a_attributes3': (rf'{_CSI}\d+\;\d+\;\d+m', 'sgr', 3),
    'set_a_attributes4': (rf'{_CSI}\d+\;\d+\;\d+\;\d+m', 'sgr', 4),
    # this helps where xterm's sgr0 includes set0_des_seq, we'd
    # rather like to also match this immediate substring.
    'sgr0': (rf'{_CSI}m', 'sgr0'),
    'backspace': (re.escape('\b'), ''),
    'ascii_tab': (CAPABILITIES_RAW_MIXIN['tab'], ''),
    'clr_eol': (rf'{_CSI}K', ''),
    'clr_eol0': (rf'{_CSI}0K', ''),
    'clr_bol': (rf'{_CSI}1K', ''),
    'clr_eosK': (rf'{_CSI}2K', ''),
}

CAPABILITIES_HORIZONTAL_DISTANCE: typing.Dict[str, int] = {
    'ascii_tab': 8,
    'backspace': -1,
    'cursor_left': -1,
    'cursor_right': 1,
    'parm_left_cursor': -1,
    'parm_right_cursor': 1,
    'tab': 8,
}

CAPABILITIES_CAUSE_MOVEMENT: typing.Tuple[str, ...] = tuple(CAPABILITIES_HORIZONTAL_DISTANCE) + (
    'carriage_return',
    'clear_screen',
    'column_address',
    'cursor_address',
    'cursor_down',
    'cursor_home',
    'cursor_up',
    'enter_fullscreen',
    'exit_fullscreen',
    'parm_down_cursor',
    'parm_up_cursor',
    'restore_cursor',
    'row_address',
    'scroll_forward',
)

XTGETTCAP_CAPABILITIES = (
    # xterm extensions
    ("TN", "Terminal name"),
    ("Co", "Number of colors"),
    # Numeric capabilities
    ("colors", "Max colors"),
    # String capabilities -- attributes
    ("bold", "Enter bold mode"),
    ("dim", "Enter dim mode"),
    ("blink", "Enter blink mode"),
    ("rev", "Enter reverse mode"),
    ("smso", "Enter standout mode"),
    ("rmso", "Exit standout mode"),
    ("smul", "Enter underline mode"),
    ("rmul", "Exit underline mode"),
    ("sitm", "Enter italics mode"),
    ("ritm", "Exit italics mode"),
    ("sgr0", "Reset attributes"),
    # String capabilities -- colors
    ("setaf", "Set foreground color"),
    ("setab", "Set background color"),
    ("op", "Original pair"),
    # String capabilities -- cursor
    ("sc", "Save cursor"),
    ("rc", "Restore cursor"),
    ("civis", "Hide cursor"),
    ("cnorm", "Normal cursor"),
    ("cvvis", "Very visible cursor"),
    ("cup", "Cursor address"),
    ("home", "Cursor home"),
    ("hpa", "Horizontal position"),
    ("vpa", "Vertical position"),
    ("cub1", "Cursor left"),
    ("cuf1", "Cursor right"),
    ("cuu1", "Cursor up"),
    ("cud1", "Cursor down"),
    ("cub", "Cursor left n"),
    ("cuf", "Cursor right n"),
    ("cuu", "Cursor up n"),
    ("cud", "Cursor down n"),
    # String capabilities -- editing
    ("el", "Clear to end of line"),
    ("el1", "Clear to start of line"),
    ("ed", "Clear to end of screen"),
    ("clear", "Clear screen"),
    ("ech", "Erase characters"),
    ("dch1", "Delete character"),
    ("dl1", "Delete line"),
    ("il1", "Insert line"),
    ("dch", "Delete n characters"),
    ("dl", "Delete n lines"),
    ("ich", "Insert n characters"),
    ("il", "Insert n lines"),
    ("indn", "Scroll forward n"),
    ("ind", "Scroll forward"),
    ("rin", "Scroll reverse n"),
    # String capabilities -- screen
    ("smcup", "Enter alt screen"),
    ("rmcup", "Exit alt screen"),
    ("csr", "Change scroll region"),
    ("smam", "Enable line wrap"),
    ("rmam", "Disable line wrap"),
    ("flash", "Flash screen"),
    ("bel", "Bell"),
    ("cr", "Carriage return"),
    # String capabilities -- keypad
    ("smkx", "Keypad transmit mode"),
    ("rmkx", "Keypad local mode"),
    # String capabilities -- user-defined (xterm convention)
    ("u6", "CPR response format"),
    ("u7", "CPR request"),
    ("u8", "DA response format"),
    ("u9", "DA request"),
)


class TermcapResponse:
    """
    Terminal capabilities queried via XTGETTCAP (DCS +q).

    XTGETTCAP queries the terminal emulator's built-in terminfo
    capabilities, bypassing the local terminfo database.  Capabilities
    are accessible by name via dict-like interface.

    .. seealso::

        `XTGETTCAP specification
        <https://invisible-island.net/xterm/ctlseqs/ctlseqs.html>`_
    """

    def __init__(self, supported: bool,
                 capabilities: Optional[Dict[str, str]] = None) -> None:
        """Initialize TermcapResponse with support status and capabilities."""
        self.supported = supported
        self.capabilities: Dict[str, str] = capabilities or {}

    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Return capability value by name, or *default*."""
        return self.capabilities.get(name, default)

    def __contains__(self, name: object) -> bool:
        """Return True if capability *name* was reported."""
        return name in self.capabilities

    def __getitem__(self, name: str) -> str:
        """Return capability value, raising :exc:`KeyError` if absent."""
        return self.capabilities[name]

    def __len__(self) -> int:
        """Return number of capabilities."""
        return len(self.capabilities)

    @property
    def terminal_name(self) -> Optional[str]:
        """Terminal name from ``TN`` capability, or ``None``."""
        return self.capabilities.get('TN')

    @property
    def num_colors(self) -> Optional[int]:
        """Number of colors from ``colors`` capability, or ``None``."""
        val = self.capabilities.get('colors')
        if val is not None:
            try:
                return int(val)
            except ValueError:
                pass
        return None

    def __repr__(self) -> str:
        """Return string representation."""
        return (f'TermcapResponse(supported={self.supported}, '
                f'capabilities={self.capabilities})')

    @staticmethod
    def hex_encode(name: str) -> str:
        """Hex-encode a capability name for an XTGETTCAP query."""
        return name.encode('ascii').hex()

    @staticmethod
    def hex_decode(hex_str: str) -> str:
        """Decode a hex-encoded string from an XTGETTCAP response."""
        try:
            return bytes.fromhex(hex_str).decode('ascii', errors='strict')
        except ValueError:
            return ''


class ITerm2Capabilities:
    """
    ITerm2 capability features from OSC 1337;Capabilities response.

    Features are accessible as a dict via :attr:`features`.

    .. seealso::

        `iTerm2 escape codes
        <https://iterm2.com/documentation-escape-codes.html>`_
    """

    FEATURE_MAP = {
        'T': ('truecolor', 'int', 2),
        'Cw': ('clipboard_writable', 'bool', 0),
        'Lr': ('decslrm', 'bool', 0),
        'M': ('mouse', 'bool', 0),
        'Sc': ('decscusr', 'int', 3),
        'U': ('unicode_basic', 'bool', 0),
        'Aw': ('ambiguous_wide', 'bool', 0),
        'Uw': ('unicode_widths', 'int', 6),
        'Ts': ('titles', 'int', 2),
        'B': ('bracketed_paste', 'bool', 0),
        'F': ('focus_reporting', 'bool', 0),
        'Gs': ('strikethrough', 'bool', 0),
        'Go': ('overline', 'bool', 0),
        'Sy': ('sync', 'bool', 0),
        'H': ('hyperlinks', 'bool', 0),
        'No': ('notifications', 'bool', 0),
        'Sx': ('sixel', 'bool', 0),
    }

    def __init__(self, supported: bool,
                 features: Optional[Dict[str, typing.Any]] = None) -> None:
        """Initialize ITerm2Capabilities with support status and features."""
        self.supported = supported
        self.features: Dict[str, typing.Any] = features or {}

    @staticmethod
    def parse_feature_string(feature_str: str) -> Dict[str, typing.Any]:
        """Parse an iTerm2 Capabilities feature string into a dict."""
        features: Dict[str, typing.Any] = {}
        pos = 0
        while pos < len(feature_str):
            matched = False
            for code_len in (2, 1):
                code = feature_str[pos:pos + code_len]
                if code in ITerm2Capabilities.FEATURE_MAP:
                    name, ftype, bits = ITerm2Capabilities.FEATURE_MAP[code]
                    pos += code_len
                    if ftype == 'int' and bits > 0:
                        digits = ''
                        while pos < len(feature_str) and feature_str[pos].isdigit():
                            digits += feature_str[pos]
                            pos += 1
                        features[name] = int(digits) if digits else 0
                    else:
                        features[name] = True
                    matched = True
                    break
            if not matched:
                pos += 1
        return features

    def __repr__(self) -> str:
        """Return string representation."""
        return (f'ITerm2Capabilities(supported={self.supported}, '
                f'features={self.features})')


class TextSizingResult:
    """
    Result of Kitty text sizing protocol detection (OSC 66).

    :param bool width: True if width sizing is supported.
    :param bool scale: True if scale sizing is supported.
    """

    def __init__(self, width: bool = False, scale: bool = False) -> None:
        self.width = width
        self.scale = scale

    def __bool__(self) -> bool:
        return self.width or self.scale

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TextSizingResult):
            return self.width == other.width and self.scale == other.scale
        return NotImplemented

    def __repr__(self) -> str:
        return f"TextSizingResult(width={self.width}, scale={self.scale})"
