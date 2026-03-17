#   Copyright 2000-2010 Michael Hudson-Doyle <micahel@gmail.com>
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

"""This is an alternative to python_reader which tries to emulate
the CPython prompt as closely as possible, with the exception of
allowing multiline input and multiline history entries.
"""

import sys

from pyrepl.readline import _error, _get_reader, multiline_input


def check():  # returns False if there is a problem initializing the state
    try:
        _get_reader()
    except _error:
        return False
    return True


def run_multiline_interactive_console(mainmodule=None, future_flags=0):
    import code

    import __main__

    mainmodule = mainmodule or __main__
    console = code.InteractiveConsole(mainmodule.__dict__, filename="<stdin>")
    if future_flags:
        console.compile.compiler.flags |= future_flags

    def more_lines(src: str) -> bool:
        try:
            code = console.compile(src, "<stdin>", "single")
        except (OverflowError, SyntaxError, ValueError):
            return False

        return code is None

    while True:
        try:
            ps1 = getattr(sys, "ps1", ">>> ")
            ps2 = getattr(sys, "ps2", "... ")
            try:
                statement = multiline_input(
                    more_lines,
                    ps1,
                    ps2,
                    get_bytes=False,
                )
            except EOFError:
                break
            more = console.push(statement)
            assert not more
        except KeyboardInterrupt:
            console.write("\nKeyboardInterrupt\n")
            console.resetbuffer()
