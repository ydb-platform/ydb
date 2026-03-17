#   Copyright 2000-2010 Michael Hudson-Doyle <micahel@gmail.com>
#                       Alex Gaynor
#                       Antonio Cuni
#                       Armin Rigo
#                       Holger Krekel
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

"""A compatibility wrapper reimplementing the 'readline' standard module
on top of pyrepl.  Not all functionalities are supported.  Contains
extensions for multiline input.
"""

import contextlib
import os
import sys
from typing import Callable, Union

from pyrepl import commands
from pyrepl.completing_reader import CompletingReader
from pyrepl.historical_reader import HistoricalReader
from pyrepl.unix_console import UnixConsole, _error

ENCODING = sys.getfilesystemencoding() or "latin1"  # XXX review


# Note: stubs are dynamically generated for
# read_init_file, redisplay and set_pre_input_hook
# so we F822 is a false positive
__all__ = [  # noqa: F822
    "add_history",
    "clear_history",
    "get_begidx",
    "get_completer",
    "get_completer_delims",
    "get_current_history_length",
    "get_endidx",
    "get_history_item",
    "get_history_length",
    "get_line_buffer",
    "insert_text",
    "parse_and_bind",
    "read_history_file",
    "read_init_file",
    "redisplay",
    "remove_history_item",
    "replace_history_item",
    "set_completer",
    "set_completer_delims",
    "set_history_length",
    "set_pre_input_hook",
    "set_startup_hook",
    "write_history_file",
    # ---- multiline extensions ----
    "multiline_input",
]

# ____________________________________________________________


class ReadlineConfig:
    readline_completer = None
    completer_delims = dict.fromkeys(" \t\n`~!@#$%^&*()-=+[{]}\\|;:'\",<>/?")


class ReadlineAlikeReader(HistoricalReader, CompletingReader):
    assume_immutable_completions = False
    use_brackets = False
    sort_in_column = True

    def error(self, msg="none"):
        pass  # don't show error messages by default

    def get_stem(self):
        b = self.buffer
        p = self.pos - 1
        completer_delims = self.config.completer_delims
        while p >= 0 and b[p] not in completer_delims:
            p -= 1
        return "".join(b[p + 1 : self.pos])

    def get_completions(self, stem):
        result = []
        function = self.config.readline_completer
        if function is not None:
            with contextlib.suppress(UnicodeEncodeError):
                stem = str(stem)  # rlcompleter.py seems to not like unicode
            state = 0
            while True:
                try:
                    next = function(stem, state)
                except:  # noqa: E722
                    break
                if not isinstance(next, str):
                    break
                result.append(next)
                state += 1
            # emulate the behavior of the standard readline that sorts
            # the completions before displaying them.
            result.sort()
        return result

    def get_trimmed_history(self, maxlength):
        if maxlength >= 0:
            cut = len(self.history) - maxlength
            if cut < 0:
                cut = 0
        else:
            cut = 0
        return self.history[cut:]

    # --- simplified support for reading multiline Python statements ---

    # This duplicates small parts of pyrepl.python_reader.  I'm not
    # reusing the PythonicReader class directly for two reasons.  One is
    # to try to keep as close as possible to CPython's prompt.  The
    # other is that it is the readline module that we are ultimately
    # implementing here, and I don't want the built-in raw_input() to
    # start trying to read multiline inputs just because what the user
    # typed look like valid but incomplete Python code.  So we get the
    # multiline feature only when using the multiline_input() function
    # directly (see _pypy_interact.py).

    more_lines = None

    def collect_keymap(self):
        return super().collect_keymap() + ((r"\n", "maybe-accept"),)

    def __init__(self, console):
        super().__init__(console)
        self.commands["maybe_accept"] = maybe_accept
        self.commands["maybe-accept"] = maybe_accept

    def after_command(self, cmd):
        super().after_command(cmd)
        if self.more_lines is None:
            # Force single-line input if we are in raw_input() mode.
            # Although there is no direct way to add a \n in this mode,
            # multiline buffers can still show up using various
            # commands, e.g. navigating the history.
            try:
                index = self.buffer.index("\n")
            except ValueError:
                pass
            else:
                self.buffer = self.buffer[:index]
                if self.pos > len(self.buffer):
                    self.pos = len(self.buffer)


class maybe_accept(commands.Command):
    def do(self):
        r = self.reader
        r.dirty = True  # this is needed to hide the completion menu, if visible
        #
        # if there are already several lines and the cursor
        # is not on the last one, always insert a new \n.
        text = r.get_str()
        if "\n" in r.buffer[r.pos :] or r.more_lines is not None and r.more_lines(text):
            r.insert("\n")
        else:
            self.finish = 1


class _ReadlineWrapper:
    reader = None
    saved_history_length = -1
    startup_hook = None
    config = ReadlineConfig()
    stdin = None
    stdout = None
    stderr = None

    def __init__(self, f_in=None, f_out=None):
        self.f_in = f_in if f_in is not None else os.dup(0)
        self.f_out = f_out if f_out is not None else os.dup(1)

    def setup_std_streams(self, stdin, stdout, stderr):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr

    def get_reader(self):
        if self.reader is None:
            console = UnixConsole(self.f_in, self.f_out, encoding=ENCODING)
            self.reader = ReadlineAlikeReader(console)
            self.reader.config = self.config
        return self.reader

    def input(self, prompt: str = "") -> str:
        try:
            reader = self.get_reader()
        except _error:
            if _old_input is not None:
                return _old_input(prompt)

            return input(prompt)

        reader.ps1 = prompt

        if self.stdout and hasattr(self.stdout, "flush"):
            self.stdout.flush()
        if self.stderr and hasattr(self.stderr, "flush"):
            self.stderr.flush()

        ret = reader.readline(startup_hook=self.startup_hook)

        return str(ret, ENCODING)

    def multiline_input(
        self,
        more_lines: Callable[[str], bool],
        ps1: str,
        ps2: str,
        get_bytes: bool = True,
    ) -> Union[bytes, str]:
        """Read an input on possibly multiple lines, asking for more
        lines as long as 'more_lines(str)' is truish."""
        reader = self.get_reader()
        saved = reader.more_lines
        try:
            reader.more_lines = more_lines
            reader.ps1 = reader.ps2 = ps1
            reader.ps3 = reader.ps4 = ps2
            return reader.readline(get_bytes=get_bytes)
        finally:
            reader.more_lines = saved

    def parse_and_bind(self, string):
        pass  # XXX we don't support parsing GNU-readline-style init files

    def set_completer(self, function=None):
        self.config.readline_completer = function

    def get_completer(self):
        return self.config.readline_completer

    def set_completer_delims(self, string):
        self.config.completer_delims = dict.fromkeys(string)

    def get_completer_delims(self):
        chars = list(self.config.completer_delims.keys())
        chars.sort()
        return "".join(chars)

    def _histline(self, line: str):
        return line.rstrip("\n")

    def get_history_length(self):
        return self.saved_history_length

    def set_history_length(self, length):
        self.saved_history_length = length

    def get_current_history_length(self):
        return len(self.get_reader().history)

    def read_history_file(self, filename="~/.history"):
        # multiline extension (really a hack) for the end of lines that
        # are actually continuations inside a single multiline_input()
        # history item: we use \r\n instead of just \n.  If the history
        # file is passed to GNU readline, the extra \r are just ignored.
        history = self.get_reader().history
        with open(os.path.expanduser(filename)) as f:
            buffer = []
            for line in f:
                if line.endswith("\r\n"):
                    buffer.append(line)
                else:
                    line = self._histline(line)
                    if buffer:
                        line = "".join(buffer).replace("\r", "") + line
                        del buffer[:]
                    if line:
                        history.append(line)

    def write_history_file(self, filename="~/.history"):
        maxlength = self.saved_history_length
        history = self.get_reader().get_trimmed_history(maxlength)
        entries = ""
        for entry in history:
            entry = entry.replace("\n", "\r\n")  # multiline history support
            entries += entry + "\n"

        fname = os.path.expanduser(filename)
        with open(fname, "w", encoding="utf-8") as f:
            f.write(entries)

    def clear_history(self):
        del self.get_reader().history[:]

    def get_history_item(self, index):
        history = self.get_reader().history
        if 1 <= index <= len(history):
            return history[index - 1]

        return None  # blame readline.c for not raising

    def remove_history_item(self, index):
        history = self.get_reader().history
        if 0 <= index < len(history):
            del history[index]
        else:
            raise ValueError(f"No history item at position {index}")
            # blame readline.c for raising ValueError

    def replace_history_item(self, index, line):
        history = self.get_reader().history
        if 0 <= index < len(history):
            history[index] = self._histline(line)
        else:
            raise ValueError(f"No history item at position {index}")
            # blame readline.c for raising ValueError

    def add_history(self, line):
        self.get_reader().history.append(self._histline(line))

    def set_startup_hook(self, function=None):
        self.startup_hook = function

    def get_line_buffer(self):
        return self.get_reader().get_str()

    def _get_idxs(self):
        start = cursor = self.get_reader().pos
        buf = self.get_line_buffer()
        for i in range(cursor - 1, -1, -1):
            if buf[i] in self.get_completer_delims():
                break
            start = i
        return start, cursor

    def get_begidx(self):
        return self._get_idxs()[0]

    def get_endidx(self):
        return self._get_idxs()[1]

    def insert_text(self, text):
        return self.get_reader().insert(text)


_wrapper = _ReadlineWrapper()

# ____________________________________________________________
# Public API

parse_and_bind = _wrapper.parse_and_bind
set_completer = _wrapper.set_completer
get_completer = _wrapper.get_completer
set_completer_delims = _wrapper.set_completer_delims
get_completer_delims = _wrapper.get_completer_delims
get_history_length = _wrapper.get_history_length
set_history_length = _wrapper.set_history_length
get_current_history_length = _wrapper.get_current_history_length
read_history_file = _wrapper.read_history_file
write_history_file = _wrapper.write_history_file
clear_history = _wrapper.clear_history
get_history_item = _wrapper.get_history_item
remove_history_item = _wrapper.remove_history_item
replace_history_item = _wrapper.replace_history_item
add_history = _wrapper.add_history
set_startup_hook = _wrapper.set_startup_hook
get_line_buffer = _wrapper.get_line_buffer
get_begidx = _wrapper.get_begidx
get_endidx = _wrapper.get_endidx
insert_text = _wrapper.insert_text

# Extension
multiline_input = _wrapper.multiline_input

# Internal hook
_get_reader = _wrapper.get_reader

# ____________________________________________________________
# Stubs


def _make_stub(_name, _ret):
    def stub(*args, **kwds):
        import warnings

        warnings.warn(f"readline.{_name}() not implemented", stacklevel=2)

    stub.__name__ = _name
    globals()[_name] = stub


for _name, _ret in [
    ("read_init_file", None),
    ("redisplay", None),
    ("set_pre_input_hook", None),
]:
    assert _name not in globals(), _name
    _make_stub(_name, _ret)


def _setup():
    global _old_input
    if _old_input is not None:
        return
    # don't run _setup twice

    try:
        f_in = sys.stdin.fileno()
        f_out = sys.stdout.fileno()
    except (AttributeError, ValueError):
        return
    if not os.isatty(f_in) or not os.isatty(f_out):
        return

    _wrapper.f_in = f_in
    _wrapper.f_out = f_out
    _wrapper.setup_std_streams(sys.stdin, sys.stdout, sys.stderr)

    import builtins

    _old_input = builtins.input
    builtins.input = _wrapper.input


_old_input = None
_setup()
