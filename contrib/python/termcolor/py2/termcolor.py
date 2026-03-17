# coding: utf-8
# Copyright (c) 2008-2011 Volvox Development Team
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Author: Konstantin Lepa <konstantin.lepa@gmail.com>

"""ANSI color formatting for output in terminal."""

from __future__ import print_function
import os
import sys


__ALL__ = [ "ATTRIBUTES", "COLORS", "HIGHLIGHTS", "RESET", "colored", "cprint" ]

ATTRIBUTES = {
    "bold": 1,
    "dark": 2,
    "underline": 4,
    "blink": 5,
    "reverse": 7,
    "concealed": 8,
}


HIGHLIGHTS = {
    "on_black": 40,
    "on_grey": 40,  # Actually black but kept for backwards compatibility
    "on_red": 41,
    "on_green": 42,
    "on_yellow": 43,
    "on_blue": 44,
    "on_magenta": 45,
    "on_cyan": 46,
    "on_light_grey": 47,
    "on_dark_grey": 100,
    "on_light_red": 101,
    "on_light_green": 102,
    "on_light_yellow": 103,
    "on_light_blue": 104,
    "on_light_magenta": 105,
    "on_light_cyan": 106,
    "on_white": 107,
}

COLORS = {
    "black": 30,
    "grey": 30,  # Actually black but kept for backwards compatibility
    "red": 31,
    "green": 32,
    "yellow": 33,
    "blue": 34,
    "magenta": 35,
    "cyan": 36,
    "light_grey": 37,
    "dark_grey": 90,
    "light_red": 91,
    "light_green": 92,
    "light_yellow": 93,
    "light_blue": 94,
    "light_magenta": 95,
    "light_cyan": 96,
    "white": 97,
}


RESET = "\033[0m"


def _can_do_colour(no_color, force_color=None):
    """Check env vars and for tty/dumb terminal"""
    # First check overrides:
    # "User-level configuration files and per-instance command-line arguments should
    # override $NO_COLOR. A user should be able to export $NO_COLOR in their shell
    # configuration file as a default, but configure a specific program in its
    # configuration file to specifically enable color."
    # https://no-color.org
    if no_color is not None and no_color:
        return False
    if force_color is not None and force_color:
        return True

    # Then check env vars:
    if "ANSI_COLORS_DISABLED" in os.environ:
        return False
    if "NO_COLOR" in os.environ:
        return False
    if "FORCE_COLOR" in os.environ:
        return True
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty() and os.environ.get("TERM") != "dumb"


def colored(text, color=None, on_color=None, attrs=None, no_color=None, force_color=None):
    """Colorize text.

    Available text colors:
        black, red, green, yellow, blue, magenta, cyan, white,
        light_grey, dark_grey, light_red, light_green, light_yellow, light_blue,
        light_magenta, light_cyan.

    Available text highlights:
        on_black, on_red, on_green, on_yellow, on_blue, on_magenta, on_cyan, on_white,
        on_light_grey, on_dark_grey, on_light_red, on_light_green, on_light_yellow,
        on_light_blue, on_light_magenta, on_light_cyan.

    Available attributes:
        bold, dark, underline, blink, reverse, concealed.

    Example:
        colored('Hello, World!', 'red', 'on_black', ['bold', 'blink'])
        colored('Hello, World!', 'green')
    """
    if not _can_do_colour(no_color=no_color, force_color=force_color):
        return text

    fmt_str = "\033[%dm%s"
    if color is not None:
        text = fmt_str % (COLORS[color], text)

    if on_color is not None:
        text = fmt_str % (HIGHLIGHTS[on_color], text)

    if attrs is not None:
        for attr in attrs:
            text = fmt_str % (ATTRIBUTES[attr], text)

    return text + RESET


def cprint(text, color=None, on_color=None, attrs=None, no_color=None, force_color=None, **kwargs):
    """Print colorized text.

    It accepts arguments of print function.
    """

    print((colored(text, color, on_color, attrs, no_color=no_color, force_color=force_color)), **kwargs)


if __name__ == "__main__":
    print("Current terminal type: %s" % os.getenv("TERM"))
    print("Test basic colors:")
    cprint("Black color", "black")
    cprint("Red color", "red")
    cprint("Green color", "green")
    cprint("Yellow color", "yellow")
    cprint("Blue color", "blue")
    cprint("Magenta color", "magenta")
    cprint("Cyan color", "cyan")
    cprint("White color", "white")
    cprint("Light grey color", "light_grey")
    cprint("Dark grey color", "dark_grey")
    cprint("Light red color", "light_red")
    cprint("Light green color", "light_green")
    cprint("Light yellow color", "light_yellow")
    cprint("Light blue color", "light_blue")
    cprint("Light magenta color", "light_magenta")
    cprint("Light cyan color", "light_cyan")
    print("-" * 78)

    print("Test highlights:")
    cprint("On black color", on_color="on_black")
    cprint("On red color", on_color="on_red")
    cprint("On green color", on_color="on_green")
    cprint("On yellow color", on_color="on_yellow")
    cprint("On blue color", on_color="on_blue")
    cprint("On magenta color", on_color="on_magenta")
    cprint("On cyan color", on_color="on_cyan")
    cprint("On white color", color="black", on_color="on_white")
    cprint("On light grey color", on_color="on_light_grey")
    cprint("On dark grey color", on_color="on_dark_grey")
    cprint("On light red color", on_color="on_light_red")
    cprint("On light green color", on_color="on_light_green")
    cprint("On light yellow color", on_color="on_light_yellow")
    cprint("On light blue color", on_color="on_light_blue")
    cprint("On light magenta color", on_color="on_light_magenta")
    cprint("On light cyan color", on_color="on_light_cyan")
    print("-" * 78)

    print("Test attributes:")
    cprint("Bold black color", "black", attrs=["bold"])
    cprint("Dark red color", "red", attrs=["dark"])
    cprint("Underline green color", "green", attrs=["underline"])
    cprint("Blink yellow color", "yellow", attrs=["blink"])
    cprint("Reversed blue color", "blue", attrs=["reverse"])
    cprint("Concealed Magenta color", "magenta", attrs=["concealed"])
    cprint(
        "Bold underline reverse cyan color",
        "cyan",
        attrs=["bold", "underline", "reverse"],
    )
    cprint(
        "Dark blink concealed white color",
        "white",
        attrs=["dark", "blink", "concealed"],
    )
    print("-" * 78)

    print("Test mixing:")
    cprint("Underline red on black color", "red", "on_black", ["underline"])
    cprint("Reversed green on red color", "green", "on_red", ["reverse"])
