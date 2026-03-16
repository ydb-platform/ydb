"""Class definitions for DEC Private Modes and their Response values."""

# std imports
from typing import Any, Dict, Union


class DecModeResponse:
    """
    Container for DEC Private Mode query response.

    Use helper properties :attr:`~DecModeResponse.supported`,
    :attr:`~DecModeResponse.enabled`, :attr:`~DecModeResponse.permanent`,
    and :attr:`~DecModeResponse.failed` to interpret responses rather than
    checking numeric values directly.
    """
    # Response value constants. Values -1 and -2 are internal abstractions
    # for timeout or query failure, functionally equivalent to NOT_RECOGNIZED.
    NOT_QUERIED = -2
    NO_RESPONSE = -1
    NOT_RECOGNIZED = 0
    SET = 1
    RESET = 2
    PERMANENTLY_SET = 3
    PERMANENTLY_RESET = 4

    def __init__(self, mode: Union[int, "DecPrivateMode"], value: int):
        """
        Initialize response for a DEC private mode query.

        :param mode: DEC private mode number
        :type mode: int
        :param value: Response value from terminal
        :type value: int
        :raises TypeError: If mode is not an integer
        """
        if isinstance(mode, DecPrivateMode):
            self._mode_value = mode.value
        elif isinstance(mode, int):
            self._mode_value = mode
        else:
            raise TypeError(f"Invalid mode got {mode!r}, DecPrivateMode or int expected")
        self._value = value

    @property
    def mode(self) -> "DecPrivateMode":
        """
        The :class:`DecPrivateMode` instance for this response.

        :rtype: DecPrivateMode
        """
        return DecPrivateMode(self._mode_value)

    @property
    def description(self) -> str:
        """
        Description of what this mode controls.

        :rtype: str
        """
        if isinstance(self.mode, DecPrivateMode):
            return self.mode.long_description
        return "Unknown mode"

    @property
    def value(self) -> int:
        """
        Numeric response value for compatibility.

        Prefer using helper properties like :attr:`~DecModeResponse.enabled`
        instead of checking this value directly.

        :rtype: int
        """
        return self._value

    @property
    def supported(self) -> bool:
        """
        Check if the mode is supported by the terminal.

        :rtype: bool
        :returns: True if terminal recognizes and supports this mode
        """
        return self.value > 0

    @property
    def enabled(self) -> bool:
        """
        Check if the mode is currently enabled.

        :rtype: bool
        :returns: True if mode is set (temporarily or permanently)
        """
        return self.value in {1, 3}

    @property
    def disabled(self) -> bool:
        """
        Check if the mode is currently disabled.

        :rtype: bool
        :returns: True if mode is reset (temporarily or permanently)
        """
        return self.value in {2, 4}

    @property
    def changeable(self) -> bool:
        """
        Check if the mode setting can be changed.

        :rtype: bool
        :returns: True if mode can be toggled by applications
        """
        return self.value in {1, 2}

    @property
    def permanent(self) -> bool:
        """
        Check if the mode setting is permanent.

        :rtype: bool
        :returns: True if mode cannot be changed by applications
        """
        return self.value in {3, 4}

    @property
    def failed(self) -> bool:
        """
        Check if the query failed.

        :rtype: bool
        :returns: True if response indicates timeout or query failure
        """
        return self.value < 0

    def to_dict(self) -> Dict[str, Any]:
        """
        Return response as a dictionary.

        :rtype: dict
        :returns: Dictionary with keys ``value``, ``value_description``,
            ``mode_description``, ``mode_name``, ``supported``, ``enabled``,
            and ``changeable``.
        """
        return {
            'value': self.value,
            'value_description': str(self),
            'mode_description': self.description,
            'mode_name': self.mode.name,
            'supported': self.supported,
            'enabled': self.enabled,
            'changeable': self.changeable,
        }

    def __str__(self) -> str:
        """
        Return the constant name for the response value.

        :rtype: str
        """
        return {
            self.NOT_QUERIED: "NOT_QUERIED",
            self.NO_RESPONSE: "NO_RESPONSE",
            self.NOT_RECOGNIZED: "NOT_RECOGNIZED",
            self.SET: "SET",
            self.RESET: "RESET",
            self.PERMANENTLY_SET: "PERMANENTLY_SET",
            self.PERMANENTLY_RESET: "PERMANENTLY_RESET"
        }.get(self.value, "UNKNOWN")

    def __repr__(self) -> str:
        """
        Return full representation with mode and response details.

        :rtype: str
        """
        response_name = str(self)
        response_value = f"({self.value})"
        return f"{self.mode.name}({self._mode_value}) is {response_name}{response_value}"


class DecPrivateMode:
    """
    DEC Private Mode with mnemonic name and description.

    Each instance provides:

    - :attr:`~DecPrivateMode.value`: Numeric private mode identifier
    - :attr:`~DecPrivateMode.name`: Mnemonic name or "UNKNOWN" for unrecognized modes
    - :attr:`~DecPrivateMode.long_description`: Full description of mode functionality
    """
    # These are *not* DecPrivateModes, in that they are not negotiable using the
    # DEC Private Mode sequences, but are carried in the same way, and attached
    # to this class for type safety, as they carry meaning that they have a
    # "special encoding" that is evaluated on-demand on evaluation of
    # term.inkey().name as 'KEY_SHIFT_F1' or testing inkey().is_alt_shift('a').
    # these key "events" have special late-binding evaluations depending on the
    # 'mode' they were sent as.
    SpecialInternalLegacyCSIModifier = -3
    SpecialInternalModifyOtherKeys = -2
    SpecialInternalKitty = -1

    # VT/DEC standard modes (using canonical mnemonics where available) the
    # "official" constants as published should always be used, even if cryptic,
    # at least they are sure to match exactly to existing documentation
    DECCKM = 1
    DECANM = 2
    DECCOLM = 3  # https://vt100.net/docs/vt510-rm/DECCOLM.html
    DECSCLM = 4  # https://vt100.net/docs/vt510-rm/DECSCLM.html
    DECSCNM = 5  # https://vt100.net/docs/vt510-rm/DECSCNM.html
    DECOM = 6    # https://vt100.net/docs/vt510-rm/DECOM.html
    DECAWM = 7   # https://vt100.net/docs/vt510-rm/DECAWM.html
    DECARM = 8   # https://vt100.net/docs/vt510-rm/DECARM.html
    DECINLM = 9
    DECEDM = 10
    DECLTM = 11
    DECKANAM = 12
    DECSCFDM = 13
    DECTEM = 14
    DECEKEM = 16
    DECPFF = 18
    DECPEX = 19
    OV1 = 20
    BA1 = 21
    BA2 = 22
    PK1 = 23
    AH1 = 24
    DECTCEM = 25
    DECPSP = 27
    DECPSM = 29
    SHOW_SCROLLBAR_RXVT = 30
    DECRLM = 34
    DECHEBM = 35
    DECHEM = 36
    DECTEK = 38
    DECCRNLM = 40
    DECUPM = 41
    DECNRCM = 42
    DECGEPM = 43
    DECGPCM = 44
    DECGPCS = 45
    DECGPBM = 46
    DECGRPM = 47
    DECTHAIM = 49
    DECTHAICM = 50
    DECBWRM = 51
    DECOPM = 52
    DEC131TM = 53
    DECBPM = 55
    DECNAKB = 57
    DECIPEM = 58
    DECKKDM = 59
    DECHCCM = 60
    DECVCCM = 61
    DECPCCM = 64
    DECBCMM = 65
    DECNKM = 66
    DECBKM = 67
    DECKBUM = 68
    DECVSSM = 69
    DECFPM = 70
    DECXRLM = 73
    DECSDM = 80
    DECKPM = 81
    WY_52_LINE = 83
    WYENAT_OFF = 84
    REPLACEMENT_CHAR_COLOR = 85
    DECTHAISCM = 90
    DECNCSM = 95
    DECRLCM = 96
    DECCRTSM = 97
    DECARSM = 98
    DECMCM = 99
    DECAAM = 100
    DECCANSM = 101
    DECNULM = 102
    DECHDPXM = 103
    DECESKM = 104
    DECOSCNM = 106
    DECNUMLK = 108
    DECCAPSLK = 109
    DECKLHIM = 110
    DECFWM = 111
    DECRPL = 112
    DECHWUM = 113
    DECATCUM = 114
    DECATCBM = 115
    DECBBSM = 116
    DECECM = 117

    # Mouse reporting modes and xterm/rxvt extensions
    MOUSE_REPORT_CLICK = 1000
    MOUSE_HILITE_TRACKING = 1001
    MOUSE_REPORT_DRAG = 1002
    MOUSE_ALL_MOTION = 1003
    FOCUS_IN_OUT_EVENTS = 1004
    MOUSE_EXTENDED_UTF8 = 1005
    MOUSE_EXTENDED_SGR = 1006
    ALT_SCROLL_XTERM = 1007
    SCROLL_ON_TTY_OUTPUT_RXVT = 1010
    SCROLL_ON_KEYPRESS_RXVT = 1011
    FAST_SCROLL = 1014
    MOUSE_URXVT = 1015
    MOUSE_SGR_PIXELS = 1016
    BOLD_ITALIC_HIGH_INTENSITY = 1021

    # Keyboard and meta key handling modes
    META_SETS_EIGHTH_BIT = 1034
    MODIFIERS_ALT_NUMLOCK = 1035
    META_SENDS_ESC = 1036
    KP_DELETE_SENDS_DEL = 1037
    ALT_SENDS_ESC = 1039

    # Selection, clipboard, and window manager hint modes
    KEEP_SELECTION_NO_HILITE = 1040
    USE_CLIPBOARD_SELECTION = 1041
    URGENCY_ON_CTRL_G = 1042
    RAISE_ON_CTRL_G = 1043
    REUSE_CLIPBOARD_DATA = 1044
    EXTENDED_REVERSE_WRAPAROUND = 1045
    ALT_SCREEN_BUFFER_SWITCH = 1046

    # Alternate screen buffer and cursor save/restore combinations
    ALT_SCREEN_BUFFER_XTERM = 1047
    SAVE_CURSOR_DECSC = 1048
    ALT_SCREEN_AND_SAVE_CLEAR = 1049

    # Terminal info and function key emulation modes
    TERMINFO_FUNC_KEY_MODE = 1050
    SUN_FUNC_KEY_MODE = 1051
    HP_FUNC_KEY_MODE = 1052
    SCO_FUNC_KEY_MODE = 1053

    # Legacy keyboard emulation modes
    LEGACY_KBD_X11R6 = 1060
    VT220_KBD_EMULATION = 1061

    SIXEL_PRIVATE_PALETTE = 1070

    # VTE BiDi extensions
    BIDI_ARROW_KEY_SWAPPING = 1243

    # iTerm2 extensions
    ITERM2_REPORT_KEY_UP = 1337

    # XTerm readline and mouse enhancements
    READLINE_MOUSE_BUTTON_1 = 2001
    READLINE_MOUSE_BUTTON_2 = 2002
    READLINE_MOUSE_BUTTON_3 = 2003
    BRACKETED_PASTE = 2004
    READLINE_CHARACTER_QUOTING = 2005
    READLINE_NEWLINE_PASTING = 2006

    # Modern terminal extensions
    SYNCHRONIZED_OUTPUT = 2026
    GRAPHEME_CLUSTERING = 2027
    TEXT_REFLOW = 2028
    PASSIVE_MOUSE_TRACKING = 2029
    REPORT_GRID_CELL_SELECTION = 2030
    COLOR_PALETTE_UPDATES = 2031
    IN_BAND_WINDOW_RESIZE = 2048
    BRACKETED_PASTE_MIME = 5522

    # VTE bidirectional text extensions
    MIRROR_BOX_DRAWING = 2500
    BIDI_AUTODETECTION = 2501

    # mintty extensions
    AMBIGUOUS_WIDTH_REPORTING = 7700
    SCROLL_MARKERS = 7711
    REWRAP_ON_RESIZE_MINTTY = 7723
    APPLICATION_ESCAPE_KEY = 7727
    ESC_KEY_SENDS_BACKSLASH = 7728
    GRAPHICS_POSITION = 7730
    ALT_MODIFIED_MOUSEWHEEL = 7765
    SHOW_HIDE_SCROLLBAR = 7766
    FONT_CHANGE_REPORTING = 7767
    GRAPHICS_POSITION_2 = 7780
    SHORTCUT_KEY_MODE = 7783
    MOUSEWHEEL_REPORTING = 7786
    APPLICATION_MOUSEWHEEL = 7787
    BIDI_CURRENT_LINE = 7796

    # Terminal-specific extensions
    TTCTH = 8200
    SIXEL_SCROLLING_LEAVES_CURSOR = 8452
    CHARACTER_MAPPING_SERVICE = 8800
    AMBIGUOUS_WIDTH_DOUBLE_WIDTH = 8840
    WIN32_INPUT_MODE = 9001
    KITTY_HANDLE_CTRL_C_Z = 19997
    MINTTY_BIDI = 77096
    INPUT_METHOD_EDITOR = 737769

    # Comprehensive descriptions for each mode -- it would have been nice if all
    # 3 mode items (value, key, description) could be defined side-by-side but
    # we wish to have a int-derived and like-type and not do any metaclassing,
    # or otherwise "unpicklable" or difficult to reason about for compatibility
    _LONG_DESCRIPTIONS = {
        SpecialInternalLegacyCSIModifier: "Non-DEC Mode used internally by Keystroke",
        SpecialInternalModifyOtherKeys: "Non-DEC Mode used internally by Keystroke",
        SpecialInternalKitty: "Non_DEC Mode used internally by Keystroke",

        # DEC standard modes (1-117). The "classical" names are used instead of
        # more friendly mnemonics, that's because most of these are legacy and
        # unused and it makes it easier to find out about them.
        DECCKM: "Cursor Keys Mode",
        DECANM: "ANSI/VT52 Mode",
        DECCOLM: "Column Mode",
        DECSCLM: "Scrolling Mode",
        DECSCNM: "Screen Mode (light or dark screen)",
        DECOM: "Origin Mode",
        DECAWM: "Auto Wrap Mode",
        DECARM: "Auto Repeat Mode",
        DECINLM: "Interlace Mode / Mouse X10 tracking",
        DECEDM: "Editing Mode / Show toolbar (rxvt)",
        DECLTM: "Line Transmit Mode",
        DECKANAM: "Katakana Shift Mode / Blinking cursor (xterm)",
        DECSCFDM: ("Space Compression/Field Delimiter Mode / "
                   "Start blinking cursor (xterm)"),
        DECTEM: "Transmit Execution Mode / Enable XOR of blinking cursor control (xterm)",
        DECEKEM: "Edit Key Execution Mode",
        DECPFF: "Print Form Feed",
        DECPEX: "Printer Extent",
        OV1: "Overstrike",
        BA1: "Local BASIC",
        BA2: "Host BASIC",
        PK1: "Programmable Keypad",
        AH1: "Auto Hardcopy",
        DECTCEM: "Text Cursor Enable Mode",
        DECPSP: "Proportional Spacing",
        DECPSM: "Pitch Select Mode",
        SHOW_SCROLLBAR_RXVT: "Show scrollbar (rxvt)",
        DECRLM: "Cursor Right to Left Mode",
        DECHEBM: "Hebrew (Keyboard) Mode / Enable font-shifting functions (rxvt)",
        DECHEM: "Hebrew Encoding Mode",
        DECTEK: "Tektronix 4010/4014 Mode",
        DECCRNLM: "Carriage Return/New Line Mode / Allow 80⇒132 mode (xterm)",
        DECUPM: "Unidirectional Print Mode / more(1) fix (xterm)",
        DECNRCM: "National Replacement Character Set Mode",
        DECGEPM: "Graphics Expanded Print Mode",
        DECGPCM: "Graphics Print Color Mode / Turn on margin bell (xterm)",
        DECGPCS: "Graphics Print Color Syntax / Reverse-wraparound mode (xterm)",
        DECGPBM: "Graphics Print Background Mode / Start logging (xterm)",
        DECGRPM: "Graphics Rotated Print Mode / Use Alternate Screen Buffer (xterm)",
        DECTHAIM: "Thai Input Mode",
        DECTHAICM: "Thai Cursor Mode",
        DECBWRM: "Black/White Reversal Mode",
        DECOPM: "Origin Placement Mode",
        DEC131TM: "VT131 Transmit Mode",
        DECBPM: "Bold Page Mode",
        DECNAKB: "Greek/N-A Keyboard Mapping Mode",
        DECIPEM: "Enter IBM Proprinter Emulation Mode",
        DECKKDM: "Kanji/Katakana Display Mode",
        DECHCCM: "Horizontal Cursor Coupling",
        DECVCCM: "Vertical Cursor Coupling Mode",
        DECPCCM: "Page Cursor Coupling Mode",
        DECBCMM: "Business Color Matching Mode",
        DECNKM: "Numeric Keypad Mode",
        DECBKM: "Backarrow Key Mode",
        DECKBUM: "Keyboard Usage Mode",
        DECVSSM: "Vertical Split Screen Mode / DECLRMM - Left Right Margin Mode",
        DECFPM: "Force Plot Mode",
        DECXRLM: "Transmission Rate Limiting",
        DECSDM: "Sixel Display Mode",
        DECKPM: "Key Position Mode",
        WY_52_LINE: "52 line mode (WY-370)",
        WYENAT_OFF: "Erasable/nonerasable WYENAT Off attribute select (WY-370)",
        REPLACEMENT_CHAR_COLOR: "Replacement character color (WY-370)",
        DECTHAISCM: "Thai Space Compensating Mode",
        DECNCSM: "No Clearing Screen on Column Change Mode",
        DECRLCM: "Right to Left Copy Mode",
        DECCRTSM: "CRT Save Mode",
        DECARSM: "Auto Resize Mode",
        DECMCM: "Modem Control Mode",
        DECAAM: "Auto Answerback Mode",
        DECCANSM: "Conceal Answerback Message Mode",
        DECNULM: "Ignore Null Mode",
        DECHDPXM: "Half Duplex Mode",
        DECESKM: "Secondary Keyboard Language Mode",
        DECOSCNM: "Overscan Mode",
        DECNUMLK: "NumLock Mode",
        DECCAPSLK: "Caps Lock Mode",
        DECKLHIM: "Keyboard LEDs Host Indicator Mode",
        DECFWM: "Framed Windows Mode",
        DECRPL: "Review Previous Lines Mode",
        DECHWUM: "Host Wake-Up Mode",
        DECATCUM: "Alternate Text Color Underline Mode",
        DECATCBM: "Alternate Text Color Blink Mode",
        DECBBSM: "Bold and Blink Style Mode",
        DECECM: "Erase Color Mode",

        # Mouse and xterm extensions (1000+)
        MOUSE_REPORT_CLICK: "Send Mouse X & Y on button press",
        MOUSE_HILITE_TRACKING: "Use Hilite Mouse Tracking",
        MOUSE_REPORT_DRAG: "Use Cell Motion Mouse Tracking",
        MOUSE_ALL_MOTION: "Use All Motion Mouse Tracking",
        FOCUS_IN_OUT_EVENTS: "Send FocusIn/FocusOut events",
        MOUSE_EXTENDED_UTF8: "Enable UTF-8 Mouse Mode",
        MOUSE_EXTENDED_SGR: "Enable SGR Mouse Mode",
        ALT_SCROLL_XTERM: "Enable Alternate Scroll Mode",
        SCROLL_ON_TTY_OUTPUT_RXVT: "Scroll to bottom on tty output",
        SCROLL_ON_KEYPRESS_RXVT: "Scroll to bottom on key press",
        FAST_SCROLL: "Enable fastScroll resource",
        MOUSE_URXVT: "Enable urxvt Mouse Mode",
        MOUSE_SGR_PIXELS: "Enable SGR Mouse PixelMode",
        BOLD_ITALIC_HIGH_INTENSITY: "Bold/italic implies high intensity",

        # Keyboard and meta key handling modes
        META_SETS_EIGHTH_BIT: 'Interpret "meta" key',
        MODIFIERS_ALT_NUMLOCK: "Enable special modifiers for Alt and NumLock keys",
        META_SENDS_ESC: "Send ESC when Meta modifies a key",
        KP_DELETE_SENDS_DEL: "Send DEL from the editing-keypad Delete key",
        ALT_SENDS_ESC: "Send ESC when Alt modifies a key",

        # Selection, clipboard, and window manager hint modes
        KEEP_SELECTION_NO_HILITE: "Keep selection even if not highlighted",
        USE_CLIPBOARD_SELECTION: "Use the CLIPBOARD selection",
        URGENCY_ON_CTRL_G: "Enable Urgency window manager hint when Control-G is received",
        RAISE_ON_CTRL_G: "Enable raising of the window when Control-G is received",
        REUSE_CLIPBOARD_DATA: "Reuse the most recent data copied to CLIPBOARD",
        EXTENDED_REVERSE_WRAPAROUND: "Extended Reverse-wraparound mode (XTREVWRAP2)",
        ALT_SCREEN_BUFFER_SWITCH: "Enable switching to/from Alternate Screen Buffer",

        # Alternate screen buffer and cursor save/restore combinations
        ALT_SCREEN_BUFFER_XTERM: "Use Alternate Screen Buffer",
        SAVE_CURSOR_DECSC: "Save cursor as in DECSC",
        ALT_SCREEN_AND_SAVE_CLEAR: "Save cursor as in DECSC and use alternate screen buffer",

        # Terminal info and function key emulation modes
        TERMINFO_FUNC_KEY_MODE: "Set terminfo/termcap function-key mode",
        SUN_FUNC_KEY_MODE: "Set Sun function-key mode",
        HP_FUNC_KEY_MODE: "Set HP function-key mode",
        SCO_FUNC_KEY_MODE: "Set SCO function-key mode",

        # Legacy keyboard emulation modes
        LEGACY_KBD_X11R6: "Set legacy keyboard emulation, i.e, X11R6",
        VT220_KBD_EMULATION: "Set VT220 keyboard emulation",

        SIXEL_PRIVATE_PALETTE: "Use private color registers for each graphic",

        # VTE BiDi extensions
        BIDI_ARROW_KEY_SWAPPING: "Arrow keys swapping (BiDi)",

        # iTerm2 extensions
        ITERM2_REPORT_KEY_UP: "Report Key Up",

        # XTerm readline and mouse enhancements
        READLINE_MOUSE_BUTTON_1: "Enable readline mouse button-1",
        READLINE_MOUSE_BUTTON_2: "Enable readline mouse button-2",
        READLINE_MOUSE_BUTTON_3: "Enable readline mouse button-3",
        BRACKETED_PASTE: "Set bracketed paste mode",
        READLINE_CHARACTER_QUOTING: "Enable readline character-quoting",
        READLINE_NEWLINE_PASTING: "Enable readline newline pasting",

        # Modern terminal extensions
        SYNCHRONIZED_OUTPUT: "Synchronized Output",
        GRAPHEME_CLUSTERING: "Grapheme Clustering",
        TEXT_REFLOW: "Text reflow",
        PASSIVE_MOUSE_TRACKING: "Passive Mouse Tracking",
        REPORT_GRID_CELL_SELECTION: "Report grid cell selection",
        COLOR_PALETTE_UPDATES: "Color palette updates",
        IN_BAND_WINDOW_RESIZE: "In-Band Window Resize Notifications",
        BRACKETED_PASTE_MIME: "Bracketed Paste MIME",

        # VTE bidirectional text extensions
        MIRROR_BOX_DRAWING: "Mirror box drawing characters",
        BIDI_AUTODETECTION: "BiDi autodetection",

        # mintty extensions
        AMBIGUOUS_WIDTH_REPORTING: "Ambiguous width reporting",
        SCROLL_MARKERS: "Scroll markers (prompt start)",
        REWRAP_ON_RESIZE_MINTTY: "Rewrap on resize",
        APPLICATION_ESCAPE_KEY: "Application escape key mode",
        ESC_KEY_SENDS_BACKSLASH: "Send ^\\ instead of the standard ^[ for the ESC key",
        GRAPHICS_POSITION: "Graphics position",
        ALT_MODIFIED_MOUSEWHEEL: "Alt-modified mousewheel mode",
        SHOW_HIDE_SCROLLBAR: "Show/hide scrollbar",
        FONT_CHANGE_REPORTING: "Font change reporting",
        GRAPHICS_POSITION_2: "Graphics position",
        SHORTCUT_KEY_MODE: "Shortcut key mode",
        MOUSEWHEEL_REPORTING: "Mousewheel reporting",
        APPLICATION_MOUSEWHEEL: "Application mousewheel mode",
        BIDI_CURRENT_LINE: "BiDi on current line",

        # Terminal-specific extensions
        TTCTH: "Terminal-to-Computer Talk-back Handler",
        SIXEL_SCROLLING_LEAVES_CURSOR: "Sixel scrolling leaves cursor to right of graphic",
        CHARACTER_MAPPING_SERVICE: "enable/disable character mapping service",
        AMBIGUOUS_WIDTH_DOUBLE_WIDTH: "Treat ambiguous width characters as double-width",
        WIN32_INPUT_MODE: "win32-input-mode",
        KITTY_HANDLE_CTRL_C_Z: "Handle Ctrl-C/Ctrl-Z mode",
        MINTTY_BIDI: "BiDi",
        INPUT_METHOD_EDITOR: "Input Method Editor (IME) mode",
    }

    # Reverse lookup mapping from numeric value to mnemonic name
    _VALUE_TO_NAME = {v: k for k, v in locals().items()
                      if k.isupper() and isinstance(v, int)
                      and not k.startswith('_')}

    def __init__(self, value: int):
        """
        Initialize DEC Private Mode with numeric identifier.

        :param value: Numeric DEC private mode identifier
        :type value: int
        """
        self.value = int(value)
        self.name = self._VALUE_TO_NAME.get(value, "UNKNOWN")

    def __repr__(self) -> str:
        """
        Return representation in format, ``'NAME(value)'``.

        :rtype: str
        """
        return f"{self.name}({self.value})"

    def __int__(self) -> int:
        """
        Return the integer value of this mode.

        :rtype: int
        """
        return self.value

    def __index__(self) -> int:
        """
        Return integer value for use in contexts requiring an integer index.

        :rtype: int
        """
        return self.value

    def __eq__(self, other: Any) -> bool:
        """
        Compare with another :class:`DecPrivateMode` or int.

        :param other: Object to compare with
        :type other: DecPrivateMode or int
        :rtype: bool
        """
        if isinstance(other, DecPrivateMode):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return False

    def __hash__(self) -> int:
        """
        Return hash based on numeric value.

        :rtype: int
        """
        return hash(self.value)

    @property
    def long_description(self) -> str:
        """
        Full description of this DEC private mode's functionality.

        :rtype: str
        :returns: Detailed description or "Unknown mode" for unrecognized modes
        """
        return self._LONG_DESCRIPTIONS.get(self.value, "Unknown mode")
