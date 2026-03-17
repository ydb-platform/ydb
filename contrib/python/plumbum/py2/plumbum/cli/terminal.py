# -*- coding: utf-8 -*-
"""
Terminal-related utilities
--------------------------
"""

from __future__ import absolute_import, division, print_function

import os
import sys

from plumbum import local

from .progress import Progress
from .termsize import get_terminal_size

__all__ = (
    "readline",
    "ask",
    "choose",
    "prompt",
    "get_terminal_size",
    "Progress",
    "get_terminal_size",
)


def __dir__():
    return __all__


def readline(message=""):
    """Gets a line of input from the user (stdin)"""
    sys.stdout.write(message)
    sys.stdout.flush()
    return sys.stdin.readline()


def ask(question, default=None):
    """
    Presents the user with a yes/no question.

    :param question: The question to ask
    :param default: If ``None``, the user must answer. If ``True`` or ``False``, lack of response is
                    interpreted as the default option

    :returns: the user's choice
    """
    question = question.rstrip().rstrip("?").rstrip() + "?"
    if default is None:
        question += " (y/n) "
    elif default:
        question += " [Y/n] "
    else:
        question += " [y/N] "

    while True:
        try:
            answer = readline(question).strip().lower()
        except EOFError:
            answer = None
        if answer in ("y", "yes"):
            return True
        elif answer in ("n", "no"):
            return False
        elif not answer and default is not None:
            return default
        else:
            sys.stdout.write("Invalid response, please try again\n")


def choose(question, options, default=None):
    """Prompts the user with a question and a set of options, from which the user needs to choose.

    :param question: The question to ask
    :param options: A set of options. It can be a list (of strings or two-tuples, mapping text
                    to returned-object) or a dict (mapping text to returned-object).``
    :param default: If ``None``, the user must answer. Otherwise, lack of response is interpreted
                    as this answer

    :returns: The user's choice

    Example::

        ans = choose("What is your favorite color?", ["blue", "yellow", "green"], default = "yellow")
        # `ans` will be one of "blue", "yellow" or "green"

        ans = choose("What is your favorite color?",
                {"blue" : 0x0000ff, "yellow" : 0xffff00 , "green" : 0x00ff00}, default = 0x00ff00)
        # this will display "blue", "yellow" and "green" but return a numerical value
    """
    if hasattr(options, "items"):
        options = options.items()
    sys.stdout.write(question.rstrip() + "\n")
    choices = {}
    defindex = None
    for i, item in enumerate(options):
        i = i + 1  # python2.5
        if isinstance(item, (tuple, list)) and len(item) == 2:
            text = item[0]
            val = item[1]
        else:
            text = item
            val = item
        choices[i] = val
        if default is not None and default == val:
            defindex = i
        sys.stdout.write("(%d) %s\n" % (i, text))
    if default is not None:
        if defindex is None:
            msg = "Choice [{}]: ".format(default)
        else:
            msg = "Choice [%d]: " % (defindex,)
    else:
        msg = "Choice: "
    while True:
        try:
            choice = readline(msg).strip()
        except EOFError:
            choice = ""
        if not choice and default:
            return default
        try:
            choice = int(choice)
            if choice not in choices:
                raise ValueError()
        except ValueError:
            sys.stdout.write("Invalid choice, please try again\n")
            continue
        return choices[choice]


def prompt(question, type=str, default=NotImplemented, validator=lambda val: True):
    """
    Presents the user with a validated question, keeps asking if validation does not pass.

    :param question: The question to ask
    :param type: The type of the answer, defaults to str
    :param default: The default choice
    :param validator: An extra validator called after type conversion, can raise ValueError or return False to trigger a retry.

    :returns: the user's choice
    """
    question = question.rstrip(" \t:")
    if default is not NotImplemented:
        question += " [{}]".format(default)
    question += ": "
    while True:
        try:
            ans = readline(question).strip()
        except EOFError:
            ans = ""
        if not ans:
            if default is not NotImplemented:
                # sys.stdout.write("\b%s\n" % (default,))
                return default
            else:
                continue
        try:
            ans = type(ans)
        except (TypeError, ValueError) as ex:
            sys.stdout.write("Invalid value ({}), please try again\n".format(ex))
            continue
        try:
            valid = validator(ans)
        except ValueError as ex:
            sys.stdout.write("{}, please try again\n".format(ex))
            continue
        if not valid:
            sys.stdout.write("Value not in specified range, please try again\n")
            continue
        return ans


def hexdump(data_or_stream, bytes_per_line=16, aggregate=True):
    """Convert the given bytes (or a stream with a buffering ``read()`` method) to hexdump-formatted lines,
    with possible aggregation of identical lines. Returns a generator of formatted lines.
    """
    if hasattr(data_or_stream, "read"):

        def read_chunk():
            while True:
                buf = data_or_stream.read(bytes_per_line)
                if not buf:
                    break
                yield buf

    else:

        def read_chunk():
            for i in range(0, len(data_or_stream), bytes_per_line):
                yield data_or_stream[i : i + bytes_per_line]

    prev = None
    skipped = False
    for i, chunk in enumerate(read_chunk()):
        hexd = " ".join("{:02x}".format(ord(ch)) for ch in chunk)
        text = "".join(ch if 32 <= ord(ch) < 127 else "." for ch in chunk)
        if aggregate and prev == chunk:
            skipped = True
            continue
        prev = chunk
        if skipped:
            yield "*"
        yield "{:06x} | {}| {}".format(
            i * bytes_per_line,
            hexd.ljust(bytes_per_line * 3, " "),
            text,
        )
        skipped = False


def pager(rows, pagercmd=None):  # pragma: no cover
    """Opens a pager (e.g., ``less``) to display the given text. Requires a terminal.

    :param rows: a ``bytes`` or a list/iterator of "rows" (``bytes``)
    :param pagercmd: the pager program to run. Defaults to ``less -RSin``
    """
    if not pagercmd:
        pagercmd = local["less"]["-RSin"]
    if hasattr(rows, "splitlines"):
        rows = rows.splitlines()

    pg = pagercmd.popen(stdout=None, stderr=None)
    try:
        for row in rows:
            line = "{}\n".format(row)
            try:
                pg.stdin.write(line)
                pg.stdin.flush()
            except IOError:
                break
        pg.stdin.close()
        pg.wait()
    finally:
        try:
            rows.close()
        except Exception:
            pass
        if pg and pg.poll() is None:
            try:
                pg.terminate()
            except Exception:
                pass
            os.system("reset")
