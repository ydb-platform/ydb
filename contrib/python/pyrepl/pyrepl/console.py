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

# noqa: B027

import abc
import sys
from typing import Optional, Tuple


class Event:
    __slots__ = "type", "data", "raw"

    def __init__(self, type_: str, data: Optional[str], raw: str = ""):
        self.type = type_
        self.data = data
        self.raw = raw

    def __repr__(self):
        return f"Event({self.name}, {self.data})"

    def __eq__(self, other):
        if not isinstance(other, Event):
            raise NotImplementedError(
                f"cannot compare {self.__class__} with {other.__class__}"
            )
        return (
            self.type == other.type
            and self.data == other.data
            and self.raw == other.raw
        )


class Console(abc.ABC):
    def __init__(
        self,
        height: Optional[int] = None,
        width: Optional[int] = None,
        encoding: Optional[str] = None,
    ):
        self.height = height
        self.width = width
        self.encoding = encoding or sys.getdefaultencoding()

        self.screen = []  # FIXME: add type

    # TODO: add type hints
    @abc.abstractmethod
    def refresh(self, screen, xy: Tuple[int, int]):
        pass

    def prepare(self):
        pass

    def restore(self):
        pass

    def move_cursor(self, x: int, y: int):
        pass

    def set_cursor_vis(self, vis):
        pass

    def getheightwidth(self):
        """Return (height, width) where height and width are the height
        and width of the terminal window in characters."""

    @abc.abstractmethod
    def get_event(self, block: bool = True):
        """Return an Event instance.  Returns None if |block| is false
        and there is no event pending, otherwise waits for the
        completion of an event."""

    def beep(self):
        pass

    def clear(self):
        """Wipe the screen"""

    def finish(self):
        """Move the cursor to the end of the display and otherwise get
        ready for end.  XXX could be merged with restore?  Hmm."""

    def flushoutput(self):
        """Flush all output to the screen (assuming there's some
        buffering going on somewhere)."""

    def forgetinput(self):
        """Forget all pending, but not yet processed input."""

    def getpending(self):
        """Return the characters that have been typed but not yet
        processed."""

    def wait(self):
        """Wait for an event."""
