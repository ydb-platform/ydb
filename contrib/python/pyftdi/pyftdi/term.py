"""Terminal management helpers"""

# Copyright (c) 2020-2024, Emmanuel Blot <emmanuel.blot@free.fr>
# Copyright (c) 2020, Michael Pratt <mpratt51@gmail.com>
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

from os import environ, read as os_read
from sys import platform, stderr, stdin, stdout

# pylint: disable=import-error
if platform == 'win32':
    import msvcrt
    from subprocess import call  # ugly workaround for an ugly OS
else:
    from termios import (ECHO, ICANON, TCSAFLUSH, TCSANOW, VINTR, VMIN, VSUSP,
                         VTIME, tcgetattr, tcsetattr)

    # pylint workaround (disable=used-before-assignment)
    def call():
        # pylint: disable=missing-function-docstring
        pass


class Terminal:
    """Terminal management function
    """

    FNKEYS = {
        # Ctrl + Alt + Backspace
        14:     b'\x1b^H',
        # Ctrl + Alt + Enter
        28:     b'\x1b\r',
        # Pause/Break
        29:     b'\x1c',
        # Arrows
        72:     b'\x1b[A',
        80:     b'\x1b[B',
        77:     b'\x1b[C',
        75:     b'\x1b[D',
        # Arrows (Alt)
        152:    b'\x1b[1;3A',
        160:    b'\x1b[1;3B',
        157:    b'\x1b[1;3C',
        155:    b'\x1b[1;3D',
        # Arrows (Ctrl)
        141:    b'\x1b[1;5A',
        145:    b'\x1b[1;5B',
        116:    b'\x1b[1;5C',
        115:    b'\x1b[1;5D',
        # Ctrl + Tab
        148:    b'\x1b[2J',
        # Cursor (Home, Ins, Del...)
        71:     b'\x1b[1~',
        82:     b'\x1b[2~',
        83:     b'\x1b[3~',
        79:     b'\x1b[4~',
        73:     b'\x1b[5~',
        81:     b'\x1b[6~',
        # Cursor + Alt
        151:    b'\x1b[1;3~',
        162:    b'\x1b[2;3~',
        163:    b'\x1b[3;3~',
        159:    b'\x1b[4;3~',
        153:    b'\x1b[5;3~',
        161:    b'\x1b[6;3~',
        # Cursor + Ctrl (xterm)
        119:    b'\x1b[1;5H',
        146:    b'\x1b[2;5~',
        147:    b'\x1b[3;5~',
        117:    b'\x1b[1;5F',
        114:    b'\x1b[5;5~',
        118:    b'\x1b[6;5~',
        # Function Keys (F1 - F12)
        59:     b'\x1b[11~',
        60:     b'\x1b[12~',
        61:     b'\x1b[13~',
        62:     b'\x1b[14~',
        63:     b'\x1b[15~',
        64:     b'\x1b[17~',
        65:     b'\x1b[18~',
        66:     b'\x1b[19~',
        67:     b'\x1b[20~',
        68:     b'\x1b[21~',
        133:    b'\x1b[23~',
        134:    b'\x1b[24~',
        # Function Keys + Shift (F11 - F22)
        84:     b'\x1b[23;2~',
        85:     b'\x1b[24;2~',
        86:     b'\x1b[25~',
        87:     b'\x1b[26~',
        88:     b'\x1b[28~',
        89:     b'\x1b[29~',
        90:     b'\x1b[31~',
        91:     b'\x1b[32~',
        92:     b'\x1b[33~',
        93:     b'\x1b[34~',
        135:    b'\x1b[20;2~',
        136:    b'\x1b[21;2~',
        # Function Keys + Ctrl (xterm)
        94:     b'\x1bOP',
        95:     b'\x1bOQ',
        96:     b'\x1bOR',
        97:     b'\x1bOS',
        98:     b'\x1b[15;2~',
        99:     b'\x1b[17;2~',
        100:    b'\x1b[18;2~',
        101:    b'\x1b[19;2~',
        102:    b'\x1b[20;3~',
        103:    b'\x1b[21;3~',
        137:    b'\x1b[23;3~',
        138:    b'\x1b[24;3~',
        # Function Keys + Alt (xterm)
        104:    b'\x1b[11;5~',
        105:    b'\x1b[12;5~',
        106:    b'\x1b[13;5~',
        107:    b'\x1b[14;5~',
        108:    b'\x1b[15;5~',
        109:    b'\x1b[17;5~',
        110:    b'\x1b[18;5~',
        111:    b'\x1b[19;5~',
        112:    b'\x1b[20;5~',
        113:    b'\x1b[21;5~',
        139:    b'\x1b[23;5~',
        140:    b'\x1b[24;5~',
    }
    """
    Pause/Break, Ctrl+Alt+Del, Ctrl+Alt+arrows not mapable
    key: ordinal of char from msvcrt.getch()
    value: bytes string of ANSI escape sequence for linux/xterm
            numerical used over linux specifics for Home and End
            VT or CSI escape sequences used when linux has no sequence
            something unique for keys without an escape function
            0x1b == Escape key
    """

    IS_MSWIN = platform == 'win32'
    """Whether we run on crap OS."""

    def __init__(self):
        self._termstates = []

    def init(self, fullterm: bool) -> None:
        """Internal terminal initialization function"""
        if not self.IS_MSWIN:
            self._termstates = [(t.fileno(),
                                tcgetattr(t.fileno()) if t.isatty() else None)
                                for t in (stdin, stdout, stderr)]
            tfd, istty = self._termstates[0]
            if istty:
                new = tcgetattr(tfd)
                new[3] = new[3] & ~ICANON & ~ECHO
                new[6][VMIN] = 1
                new[6][VTIME] = 0
                if fullterm:
                    new[6][VINTR] = 0
                    new[6][VSUSP] = 0
                tcsetattr(tfd, TCSANOW, new)
        else:
            # Windows black magic
            # https://stackoverflow.com/questions/12492810
            call('', shell=True)

    def reset(self) -> None:
        """Reset the terminal to its original state."""
        for tfd, att in self._termstates:
            # terminal modes have to be restored on exit...
            if att is not None:
                tcsetattr(tfd, TCSANOW, att)
                tcsetattr(tfd, TCSAFLUSH, att)

    @staticmethod
    def is_term() -> bool:
        """Tells whether the current stdout/stderr stream are connected to a
        terminal (vs. a regular file or pipe)"""
        return stdout.isatty()

    @staticmethod
    def is_colorterm() -> bool:
        """Tells whether the current terminal (if any) support colors escape
        sequences"""
        terms = ['xterm-color', 'ansi']
        return stdout.isatty() and environ.get('TERM') in terms

    @classmethod
    def getkey(cls) -> bytes:
        """Return a key from the current console, in a platform independent
           way.
        """
        # there's probably a better way to initialize the module without
        # relying onto a singleton pattern. To be fixed
        if cls.IS_MSWIN:
            # w/ py2exe, it seems the importation fails to define the global
            # symbol 'msvcrt', to be fixed
            while True:
                char = msvcrt.getch()
                if char == b'\r':
                    return b'\n'
                return char
        else:
            char = os_read(stdin.fileno(), 1)
            return char

    @classmethod
    def getch_to_escape(cls, char: bytes) -> bytes:
        """Get Windows escape sequence."""
        if cls.IS_MSWIN:
            return cls.FNKEYS.get(ord(char), char)
        return char
