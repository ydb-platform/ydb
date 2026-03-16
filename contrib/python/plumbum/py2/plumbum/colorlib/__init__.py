# -*- coding: utf-8 -*-
"""\
The ``ansicolor`` object provides ``bg`` and ``fg`` to access colors,
and attributes like bold and
underlined text. It also provides ``reset`` to recover the normal font.
"""

from __future__ import absolute_import, print_function

from .factories import StyleFactory
from .styles import ANSIStyle, ColorNotFound, HTMLStyle, Style

ansicolors = StyleFactory(ANSIStyle)
htmlcolors = StyleFactory(HTMLStyle)


def load_ipython_extension(ipython):  # pragma: no cover
    try:
        from ._ipython_ext import OutputMagics
    except ImportError:
        print("IPython required for the IPython extension to be loaded.")
        raise

    ipython.push({"colors": htmlcolors})
    ipython.register_magics(OutputMagics)


def main():  # pragma: no cover
    """Color changing script entry. Call using
    python -m plumbum.colors, will reset if no arguments given."""
    import sys

    color = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""
    ansicolors.use_color = True
    ansicolors.get_colors_from_string(color).now()
