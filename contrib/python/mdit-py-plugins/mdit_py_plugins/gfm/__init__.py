"""Composite GFM (GitHub Flavored Markdown) plugin.

Enables a set of plugins that together approximate GitHub's Markdown rendering:

- Tables (built-in)
- Strikethrough with single and double tildes (built-in)
- Autolinks (gfm_autolink plugin)
- Task lists (built-in, markdown-it-py >= 4.1.0)
- Alerts (built-in, markdown-it-py >= 4.1.0)
- Footnotes (``[^label]`` references and definitions)

Optional extras:

- Dollar math (``$...$`` / ``$$...$$``)
- Front matter (YAML)

.. note::
   Tag filtering (disallowed raw HTML tags) is not yet implemented.

.. seealso::
   - `GitHub Flavored Markdown Spec <https://github.github.com/gfm/>`__
   - `GitHub basic formatting syntax
     <https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax>`__

.. versionadded:: 0.5.0

Requires markdown-it-py >= 4.1.0.
"""

from __future__ import annotations

from functools import lru_cache

from markdown_it import MarkdownIt
from markdown_it import __version__ as _mdit_version

from mdit_py_plugins.dollarmath import dollarmath_plugin
from mdit_py_plugins.footnote import footnote_plugin
from mdit_py_plugins.front_matter import front_matter_plugin
from mdit_py_plugins.gfm_autolink import gfm_autolink_plugin

__all__ = ("gfm_plugin",)

_MIN_VERSION = (4, 1, 0)


@lru_cache(maxsize=8)
def _parse_version(v: str) -> tuple[int, ...]:
    """Parse a version string like '4.1.0' into a tuple of ints."""
    return tuple(int(x) for x in v.split(".")[:3])


def gfm_plugin(
    md: MarkdownIt,
    *,
    dollarmath: bool = False,
    front_matter: bool = False,
    tasklists_editable: bool = False,
) -> None:
    """Enable GFM-like rendering.

    Starts from the current parser configuration and enables the GFM
    components on top.

    :param dollarmath: Enable dollar-delimited math (``$...$``, ``$$...$$``).
    :param front_matter: Enable YAML front matter (``---``).
    :param tasklists_editable: If True, rendered task list checkboxes are not
        disabled (i.e. they are interactive).
    """
    if _parse_version(_mdit_version) < _MIN_VERSION:
        raise RuntimeError(
            f"gfm_plugin requires markdown-it-py >= {'.'.join(str(x) for x in _MIN_VERSION)} "
            f"(installed: {_mdit_version})"
        )

    # Enable table and strikethrough rules (built into markdown-it-py)
    md.enable("table")
    md.enable("strikethrough")

    # GFM options available in markdown-it-py >= 4.1.0
    md.options["tasklists"] = True
    md.options["tasklists_editable"] = tasklists_editable
    md.options["alerts"] = True
    md.options["strikethrough_single_tilde"] = True
    # GFM autolinks
    md.use(gfm_autolink_plugin)

    # Footnotes (inline footnotes ^[...] are not part of GFM)
    md.use(footnote_plugin, inline=False)

    # Dollar math (inline $...$ and block $$...$$)
    if dollarmath:
        md.use(dollarmath_plugin, allow_blank_lines=False)

    # TODO: Tag filter — replace leading `<` with `&lt;` for disallowed raw
    # HTML tags: <title>, <textarea>, <style>, <xmp>, <iframe>, <noembed>,
    # <noframes>, <script>, <plaintext>.
    # See https://github.github.com/gfm/#disallowed-raw-html-extension-

    # Optional plugins
    if front_matter:
        md.use(front_matter_plugin)
