"""
Markdown-it-py plugin to introduce <sub> markup using ~subscript~.

Ported from
https://github.com/markdown-it/markdown-it-sub/blob/master/index.mjs

Originally ported during implementation of https://github.com/hasgeek/funnel/blob/main/funnel/utils/markdown/mdit_plugins/sub_tag.py
"""

from __future__ import annotations

from collections.abc import Sequence
import re

from markdown_it import MarkdownIt
from markdown_it.renderer import RendererHTML
from markdown_it.rules_inline import StateInline
from markdown_it.token import Token
from markdown_it.utils import EnvType, OptionsDict

__all__ = ["sub_plugin"]

TILDE_CHAR = "~"

WHITESPACE_RE = re.compile(r"(^|[^\\])(\\\\)*\s")
UNESCAPE_RE = re.compile(r'\\([ \\!"#$%&\'()*+,.\/:;<=>?@[\]^_`{|}~-])')


def tokenize(state: StateInline, silent: bool) -> bool:
    """Parse a ~subscript~ token."""
    start = state.pos
    ch = state.src[start]
    maximum = state.posMax
    found = False

    # Don't run any pairs in validation mode
    if silent:
        return False

    if ch != TILDE_CHAR:
        return False

    if start + 2 >= maximum:
        return False

    state.pos = start + 1

    while state.pos < maximum:
        if state.src[state.pos] == TILDE_CHAR:
            found = True
            break
        state.md.inline.skipToken(state)

    if not found or start + 1 == state.pos:
        state.pos = start
        return False

    content = state.src[start + 1 : state.pos]

    # Don't allow unescaped spaces/newlines inside
    if WHITESPACE_RE.search(content) is not None:
        state.pos = start
        return False

    # Found a valid pair, so update posMax and pos
    state.posMax = state.pos
    state.pos = start + 1

    # Earlier we checked "not silent", but this implementation does not need it
    token = state.push("sub_open", "sub", 1)
    token.markup = TILDE_CHAR

    token = state.push("text", "", 0)
    token.content = UNESCAPE_RE.sub(r"\1", content)

    token = state.push("sub_close", "sub", -1)
    token.markup = TILDE_CHAR

    state.pos = state.posMax + 1
    state.posMax = maximum
    return True


def sub_open(
    renderer: RendererHTML,
    tokens: Sequence[Token],
    idx: int,
    options: OptionsDict,
    env: EnvType,
) -> str:
    """Render the opening tag for a ~subscript~ token."""
    return "<sub>"


def sub_close(
    renderer: RendererHTML,
    tokens: Sequence[Token],
    idx: int,
    options: OptionsDict,
    env: EnvType,
) -> str:
    """Render the closing tag for a ~subscript~ token."""
    return "</sub>"


def sub_plugin(md: MarkdownIt) -> None:
    """
    Markdown-it-py plugin to introduce <sub> markup using ~subscript~.

    Ported from
    https://github.com/markdown-it/markdown-it-sub/blob/master/index.mjs

    Originally ported during implementation of https://github.com/hasgeek/funnel/blob/main/funnel/utils/markdown/mdit_plugins/sub_tag.py
    """
    md.inline.ruler.after("emphasis", "sub", tokenize)
    md.add_render_rule("sub_open", sub_open)
    md.add_render_rule("sub_close", sub_close)
