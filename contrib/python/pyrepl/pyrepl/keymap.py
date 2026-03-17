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

"""
functions for parsing keyspecs

Support for turning keyspecs into appropriate sequences.

pyrepl uses it's own bastardized keyspec format, which is meant to be
a strict superset of readline's \"KEYSEQ\" format (which is to say
that if you can come up with a spec readline accepts that this
doesn't, you've found a bug and should tell me about it).

Note that this is the `\\C-o' style of readline keyspec, not the
`Control-o' sort.

A keyspec is a string representing a sequence of keypresses that can
be bound to a command.

All characters other than the backslash represent themselves.  In the
traditional manner, a backslash introduces a escape sequence.

The extension to readline is that the sequence \\<KEY> denotes the
sequence of charaters produced by hitting KEY.

Examples:

`a'     - what you get when you hit the `a' key
`\\EOA'  - Escape - O - A (up, on my terminal)
`\\<UP>' - the up arrow key
`\\<up>' - ditto (keynames are case insensitive)
`\\C-o', `\\c-o'  - control-o
`\\M-.'  - meta-period
`\\E.'   - ditto (that's how meta works for pyrepl)
`\\<tab>', `\\<TAB>', `\\t', `\\011', '\\x09', '\\X09', '\\C-i', '\\C-I'
   - all of these are the tab character.  Can you think of any more?
"""

from typing import Dict, List, Optional, Tuple, Union

KeyMap = Tuple[Tuple[str, str], ...]

_escapes = {
    "\\": "\\",
    "'": "'",
    '"': '"',
    "a": "\a",
    "b": r"\h",
    "e": "\033",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "v": "\v",
}

_keynames = {
    "backspace": "backspace",
    "delete": "delete",
    "down": "down",
    "end": "end",
    "enter": "\r",
    "escape": "\033",
    "f1": "f1",
    "f2": "f2",
    "f3": "f3",
    "f4": "f4",
    "f5": "f5",
    "f6": "f6",
    "f7": "f7",
    "f8": "f8",
    "f9": "f9",
    "f10": "f10",
    "f11": "f11",
    "f12": "f12",
    "f13": "f13",
    "f14": "f14",
    "f15": "f15",
    "f16": "f16",
    "f17": "f17",
    "f18": "f18",
    "f19": "f19",
    "f20": "f20",
    "home": "home",
    "insert": "insert",
    "left": "left",
    "page down": "page down",
    "page up": "page up",
    "return": "\r",
    "right": "right",
    "space": " ",
    "tab": "\t",
    "up": "up",
    "ctrl left": "ctrl left",
    "ctrl right": "ctrl right",
}


class KeySpecError(Exception):
    pass


def _parse_key1(key: str, s: int) -> Tuple[List[str], int]:
    ctrl = 0
    meta = 0
    ret: Optional[str] = None
    while not ret and s < len(key):
        if key[s] != "\\":
            ret = key[s]
            s += 1
            continue

        c = key[s + 1].lower()
        if c in _escapes:
            ret = _escapes[c]
            s += 2
        elif c == "c":
            if key[s + 2] != "-":
                raise KeySpecError(
                    f"\\C must be followed by `-' (char {s+2} of {repr(key)})"
                )
            if ctrl:
                raise KeySpecError(f"doubled \\C- (char {s+1} of {repr(key)})")
            ctrl = 1
            s += 3
        elif c == "m":
            if key[s + 2] != "-":
                raise KeySpecError(
                    f"\\M must be followed by `-' (char {s+2} of {repr(key)})"
                )
            if meta:
                raise KeySpecError(f"doubled \\M- (char {s+1} of {repr(key)})")
            meta = 1
            s += 3
        elif c.isdigit():
            n = key[s + 1 : s + 4]
            ret = chr(int(n, 8))
            s += 4
        elif c == "x":
            n = key[s + 2 : s + 4]
            ret = chr(int(n, 16))
            s += 4
        elif c == "<":
            t = key.find(">", s)
            if t == -1:
                raise KeySpecError(
                    f"unterminated \\< starting at char {s+1} of {repr(key)}"
                )
            ret = key[s + 2 : t].lower()
            if ret not in _keynames:
                raise KeySpecError(
                    f"unrecognised keyname `{ret}' at char {s+2} of {repr(key)}"
                )
            ret = _keynames[ret]
            s = t + 1
        else:
            raise KeySpecError(
                f"unknown backslash escape {repr(c)} at char {s+2} of {repr(key)}"
            )
    if ctrl:
        assert ret
        if len(ret) > 1:
            raise KeySpecError("\\C- must be followed by a character")
        ret = chr(ord(ret) & 0x1F)  # curses.ascii.ctrl()

    assert ret
    return (["\x1b", ret] if meta else [ret], s)


def parse_keys(key: str) -> List[str]:
    s = 0
    r: List[str] = []
    while s < len(key):
        k, s = _parse_key1(key, s)
        r.extend(k)
    return r


def compile_keymap(keymap, empty: bytes = b""):
    res = {}

    for key, value in keymap.items():
        first = key[:1] if isinstance(key, bytes) else key[0]

        res.setdefault(first, {})[key[1:]] = value

    for key, value in res.items():
        if empty in value:
            if len(value) != 1:
                raise KeySpecError(f"key definitions for {list(value.values())} clash")

            res[key] = value[empty]
        else:
            res[key] = compile_keymap(value, empty)

    return res
