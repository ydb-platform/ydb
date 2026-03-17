# ruff: noqa: N801
"""
Windows specific code for the terminal.

Note that the naming convention here is non-pythonic because we are
matching the Windows API naming.
"""

from __future__ import annotations

import ctypes
import enum
from ctypes.wintypes import (
    BOOL as _BOOL,
    CHAR as _CHAR,
    DWORD as _DWORD,
    HANDLE as _HANDLE,
    SHORT as _SHORT,
    UINT as _UINT,
    WCHAR as _WCHAR,
    WORD as _WORD,
)

_kernel32 = ctypes.windll.Kernel32  # type: ignore

_STD_INPUT_HANDLE = _DWORD(-10)
_STD_OUTPUT_HANDLE = _DWORD(-11)


class WindowsConsoleModeFlags(enum.IntFlag):
    ENABLE_ECHO_INPUT = 0x0004
    ENABLE_EXTENDED_FLAGS = 0x0080
    ENABLE_INSERT_MODE = 0x0020
    ENABLE_LINE_INPUT = 0x0002
    ENABLE_MOUSE_INPUT = 0x0010
    ENABLE_PROCESSED_INPUT = 0x0001
    ENABLE_QUICK_EDIT_MODE = 0x0040
    ENABLE_WINDOW_INPUT = 0x0008
    ENABLE_VIRTUAL_TERMINAL_INPUT = 0x0200

    ENABLE_PROCESSED_OUTPUT = 0x0001
    ENABLE_WRAP_AT_EOL_OUTPUT = 0x0002
    ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
    DISABLE_NEWLINE_AUTO_RETURN = 0x0008
    ENABLE_LVB_GRID_WORLDWIDE = 0x0010

    def __str__(self) -> str:
        return f'{self.name} (0x{self.value:04X})'


_GetConsoleMode = _kernel32.GetConsoleMode
_GetConsoleMode.restype = _BOOL

_SetConsoleMode = _kernel32.SetConsoleMode
_SetConsoleMode.restype = _BOOL

_GetStdHandle = _kernel32.GetStdHandle
_GetStdHandle.restype = _HANDLE

_ReadConsoleInput = _kernel32.ReadConsoleInputA
_ReadConsoleInput.restype = _BOOL

_h_console_input = _GetStdHandle(_STD_INPUT_HANDLE)
_input_mode = _DWORD()
_GetConsoleMode(_HANDLE(_h_console_input), ctypes.byref(_input_mode))

_h_console_output = _GetStdHandle(_STD_OUTPUT_HANDLE)
_output_mode = _DWORD()
_GetConsoleMode(_HANDLE(_h_console_output), ctypes.byref(_output_mode))


class _COORD(ctypes.Structure):
    _fields_ = (('X', _SHORT), ('Y', _SHORT))


class _FOCUS_EVENT_RECORD(ctypes.Structure):
    _fields_ = (('bSetFocus', _BOOL),)


class _KEY_EVENT_RECORD(ctypes.Structure):
    class _uchar(ctypes.Union):
        _fields_ = (('UnicodeChar', _WCHAR), ('AsciiChar', _CHAR))

    _fields_ = (
        ('bKeyDown', _BOOL),
        ('wRepeatCount', _WORD),
        ('wVirtualKeyCode', _WORD),
        ('wVirtualScanCode', _WORD),
        ('uChar', _uchar),
        ('dwControlKeyState', _DWORD),
    )


class _MENU_EVENT_RECORD(ctypes.Structure):
    _fields_ = (('dwCommandId', _UINT),)


class _MOUSE_EVENT_RECORD(ctypes.Structure):
    _fields_ = (
        ('dwMousePosition', _COORD),
        ('dwButtonState', _DWORD),
        ('dwControlKeyState', _DWORD),
        ('dwEventFlags', _DWORD),
    )


class _WINDOW_BUFFER_SIZE_RECORD(ctypes.Structure):
    _fields_ = (('dwSize', _COORD),)


class _INPUT_RECORD(ctypes.Structure):
    class _Event(ctypes.Union):
        _fields_ = (
            ('KeyEvent', _KEY_EVENT_RECORD),
            ('MouseEvent', _MOUSE_EVENT_RECORD),
            ('WindowBufferSizeEvent', _WINDOW_BUFFER_SIZE_RECORD),
            ('MenuEvent', _MENU_EVENT_RECORD),
            ('FocusEvent', _FOCUS_EVENT_RECORD),
        )

    _fields_ = (('EventType', _WORD), ('Event', _Event))


def reset_console_mode() -> None:
    _SetConsoleMode(_HANDLE(_h_console_input), _DWORD(_input_mode.value))
    _SetConsoleMode(_HANDLE(_h_console_output), _DWORD(_output_mode.value))


def set_console_mode() -> bool:
    mode = (
        _input_mode.value
        | WindowsConsoleModeFlags.ENABLE_VIRTUAL_TERMINAL_INPUT
    )
    _SetConsoleMode(_HANDLE(_h_console_input), _DWORD(mode))

    mode = (
        _output_mode.value
        | WindowsConsoleModeFlags.ENABLE_PROCESSED_OUTPUT
        | WindowsConsoleModeFlags.ENABLE_VIRTUAL_TERMINAL_PROCESSING
    )
    return bool(_SetConsoleMode(_HANDLE(_h_console_output), _DWORD(mode)))


def get_console_mode() -> int:
    return _input_mode.value


def set_text_color(color) -> None:
    _kernel32.SetConsoleTextAttribute(_h_console_output, color)


def print_color(text, color) -> None:
    set_text_color(color)
    print(text)  # noqa: T201
    set_text_color(7)  # Reset to default color, grey


def getch():
    lp_buffer = (_INPUT_RECORD * 2)()
    n_length = _DWORD(2)
    lp_number_of_events_read = _DWORD()

    _ReadConsoleInput(
        _HANDLE(_h_console_input),
        lp_buffer,
        n_length,
        ctypes.byref(lp_number_of_events_read),
    )

    char = lp_buffer[1].Event.KeyEvent.uChar.AsciiChar.decode('ascii')
    if char == '\x00':
        return None

    return char
