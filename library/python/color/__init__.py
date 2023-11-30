from __future__ import print_function

import copy
import os

from termcolor import ATTRIBUTES, COLORS, HIGHLIGHTS, RESET

__all__ = [
    "ATTRIBUTES",
    "COLORS",
    "HIGHLIGHTS",
    "RESET",
    "colored",
    "cprint",
    "tcolor",
    "get_color_by_spec"
]

ATTRIBUTES = copy.deepcopy(ATTRIBUTES)
ATTRIBUTES["light"] = ATTRIBUTES['bold']

COLORS = copy.deepcopy(COLORS)
COLORS['gray'] = COLORS['grey']
COLORS['purple'] = COLORS['magenta']
COLORS["reset"] = 0


def get_code(code):
    if os.getenv("ANSI_COLORS_DISABLED") is None:
        return "\033[{}m".format(code)
    return ""


def get_color_by_spec(color_spec):
    color, on_color, attrs = get_spec(color_spec)
    return get_color(color, on_color, attrs)


def get_color(color, on_color, attrs):
    res = ""

    if color is not None:
        res += get_code(COLORS[color])

    if on_color is not None:
        res += get_code(HIGHLIGHTS[on_color])

    if attrs is not None:
        for attr in attrs:
            res += get_code(ATTRIBUTES[attr])

    return res


def get_spec(color_spec):
    """Parses string text color formatting specification.

    Arguments:
    color_spec -- string spec for text color formatting, csv string with
    `color` / `bg_color` / `attr` spec items having "-" as a delimiter.

    Returns a tuple: (color, bg-color, attributes list)

    Example:
        get_spec("green-bold-on_red") -> (32, 41, [1])
    """
    parts = color_spec.split("-")
    color = None
    on_color = None
    attrs = []
    for part in parts:
        part = part.lower()
        if part in COLORS:
            color = part
        if part in HIGHLIGHTS:
            on_color = part
        if part in ATTRIBUTES:
            attrs.append(part)
    return color, on_color, attrs


def tcolor(text, color_spec):
    color, on_color, attrs = get_spec(color_spec)
    return colored(text, color=color, on_color=on_color, attrs=attrs)


def colored(text, color=None, on_color=None, attrs=None):
    return get_color(color, on_color, attrs) + text + get_code(COLORS["reset"])


def cprint(text, color=None, on_color=None, attrs=None, **kwargs):
    print((colored(text, color, on_color, attrs)), **kwargs)
