#   Copyright 2000-2008 Michael Hudson-Doyle <micahel@gmail.com>
#                       Armin Rigo
#
#                        All Rights Reserved
#
#
# Permission to use, copy, modify, and distribute this software and
# its documentation for any purpose is hereby granted without fee,
# provided that the above copyright notice appear in all copies and
# that both that copyright notice and this permission notice appear in
# supporting documentation.
#
# THE AUTHOR MICHAEL HUDSON DISCLAIMS ALL WARRANTIES WITH REGARD TO
# THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
# AND FITNESS, IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL,
# INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
# RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
# CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
# CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

# Bah, this would be easier to test if curses/terminfo didn't have so
# much non-introspectable global state.

import os
from collections import deque
from termios import VERASE, tcgetattr
from typing import Deque, Dict, Optional, Union

from pyrepl import curses, keymap
from pyrepl.console import Event

from .trace import trace

_keynames = {
    "delete": "kdch1",
    "down": "kcud1",
    "end": "kend",
    "enter": "kent",
    "home": "khome",
    "insert": "kich1",
    "left": "kcub1",
    "page down": "knp",
    "page up": "kpp",
    "right": "kcuf1",
    "up": "kcuu1",
}


# function keys x in 1-20 -> fX: kfX
_keynames.update((f"f{i}", f"kf{i}") for i in range(1, 21))

# this is a bit of a hack: CTRL-left and CTRL-right are not standardized
# termios sequences: each terminal emulator implements its own slightly
# different incarnation, and as far as I know, there is no way to know
# programmatically which sequences correspond to CTRL-left and
# CTRL-right. In bash, these keys usually work because there are bindings
# in ~/.inputrc, but pyrepl does not support it. The workaround is to
# hard-code here a bunch of known sequences, which will be seen as "ctrl
# left" and "ctrl right" keys, which can be finally be mapped to commands
# by the reader's keymaps.
#
CTRL_ARROW_KEYCODE = {
    # for xterm, gnome-terminal, xfce terminal, etc.
    b"\033[1;5D": "ctrl left",
    b"\033[1;5C": "ctrl right",
    # for rxvt
    b"\033Od": "ctrl left",
    b"\033Oc": "ctrl right",
}


def general_keycodes() -> Dict[bytes, str]:
    keycodes: Dict[bytes, str] = {}
    for key, tiname in _keynames.items():
        keycode = curses.tigetstr(tiname)

        trace("key {key} tiname {tiname} keycode {keycode!r}", **locals())
        if keycode:
            keycodes[keycode] = key
    keycodes.update(CTRL_ARROW_KEYCODE)
    return keycodes


def EventQueue(fd: int, encoding: str) -> "EncodedQueue":
    keycodes = general_keycodes()
    if os.isatty(fd):
        backspace = tcgetattr(fd)[6][VERASE]
        keycodes[backspace] = "backspace"
    k = keymap.compile_keymap(keycodes)
    trace("keymap {k!r}", k=k)
    return EncodedQueue(k, encoding)


class EncodedQueue:
    def __init__(self, keymap: Dict[str, str], encoding: str):
        self.k = self.ck = keymap
        self.events: Deque[Event] = deque()
        self.buf = bytearray()
        self.encoding = encoding

    def get(self) -> Optional[Event]:
        if not self.events:
            return None

        return self.events.popleft()

    def empty(self) -> bool:
        return not self.events

    def flush_buf(self) -> bytearray:
        old = self.buf
        self.buf = bytearray()
        return old

    def insert(self, event: Event):
        trace("added event {event}", event=event)
        self.events.append(event)

    def push(self, char: Union[bytes, str, int]):
        ord_char: Union[int, str] = char if isinstance(char, int) else ord(char)
        char_bytes = bytes(bytearray((ord_char,)))
        self.buf.append(ord_char)
        if char_bytes in self.k:
            if self.k is self.ck:
                # sanity check, buffer is empty when a special key comes
                assert len(self.buf) == 1
            k = self.k[char_bytes]
            trace("found map {k!r}", k=k)
            if isinstance(k, dict):
                self.k = k
            else:
                self.insert(Event("key", k, self.flush_buf()))
                self.k = self.ck

        elif self.buf and self.buf[0] == 27:  # escape
            # escape sequence not recognized by our keymap: propagate it
            # outside so that i can be recognized as an M-... key (see also
            # the docstring in keymap.py, in particular the line \\E.
            trace("unrecognized escape sequence, propagating...")
            self.k = self.ck
            self.insert(Event("key", "\033", bytearray(b"\033")))
            for c in self.flush_buf()[1:]:
                self.push(chr(c))

        else:
            try:
                decoded = bytes(self.buf).decode(self.encoding)
            except UnicodeError:
                return

            self.insert(Event("key", decoded, self.flush_buf()))
            self.k = self.ck
