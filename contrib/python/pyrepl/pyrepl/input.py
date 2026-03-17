#   Copyright 2000-2004 Michael Hudson-Doyle <micahel@gmail.com>
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

# (naming modules after builtin functions is not such a hot idea...)

# an KeyTrans instance translates Event objects into Command objects

# hmm, at what level do we want [C-i] and [tab] to be equivalent?
# [meta-a] and [esc a]?  obviously, these are going to be equivalent
# for the UnixConsole, but should they be for PygameConsole?

# it would in any situation seem to be a bad idea to bind, say, [tab]
# and [C-i] to *different* things... but should binding one bind the
# other?

# executive, temporary decision: [tab] and [C-i] are distinct, but
# [meta-key] is identified with [esc key].  We demand that any console
# class does quite a lot towards emulating a unix terminal.

import abc
import pprint
import unicodedata
from collections import deque
from typing import TYPE_CHECKING, Deque, List, Optional, Tuple

from .trace import trace

if TYPE_CHECKING:
    from .console import Event
    from .reader import KeyMap


class InputTranslator(abc.ABC):
    @abc.abstractmethod
    def push(self, event: "Event"):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self):
        raise NotImplementedError

    @abc.abstractmethod
    def empty(self):
        raise NotImplementedError


class KeymapTranslator(InputTranslator):
    def __init__(
        self,
        keymap: "KeyMap",
        verbose: bool = False,
        invalid_cls: Optional[str] = None,  # FIXME: add type
        character_cls: Optional[str] = None,  # FIXME: add type
    ):
        self.verbose = verbose
        from pyrepl.keymap import compile_keymap, parse_keys

        self.keymap = keymap
        self.invalid_cls = invalid_cls
        self.character_cls = character_cls
        d = {}
        for keyspec, command in keymap:
            keyseq = tuple(parse_keys(keyspec))
            d[keyseq] = command
        if verbose:
            trace("[input] keymap: {}", pprint.pformat(d))
        self.k = self.ck = compile_keymap(d, ())
        self.results: Deque[Tuple[str, List[str]]] = deque()
        self.stack: List[str] = []

    def push(self, event: "Event"):
        trace("[input] pushed {!r}", event.data)
        key = event.data
        d = self.k.get(key)
        if isinstance(d, dict):
            trace("[input] transition")
            self.stack.append(key)
            self.k = d
            return

        if d is None:
            trace("[input] invalid")
            if self.stack or len(key) > 1 or unicodedata.category(key) == "C":
                assert self.invalid_cls
                self.results.append((self.invalid_cls, self.stack + [key]))
            else:
                assert self.character_cls
                # small optimization:
                self.k[key] = self.character_cls
                self.results.append((self.character_cls, [key]))
        else:
            trace("[input] matched {}", d)
            self.results.append((d, self.stack + [key]))
        self.stack = []
        self.k = self.ck

    def get(self):
        if not self.results:
            return None

        return self.results.popleft()

    def empty(self) -> bool:
        return not self.results
