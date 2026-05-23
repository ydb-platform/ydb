"""Superscript tag plugin.

Ported by Elijah Greenstein from https://github.com/markdown-it/markdown-it-sup
cf. Subscript tag plugin, https://mdit-py-plugins.readthedocs.io/en/latest/#subscripts

MIT License
Copyright (c) 2014-2015 Vitaly Puzrin, Alex Kocharin.

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
"""

from markdown_it import MarkdownIt
from markdown_it.rules_inline import StateInline

from mdit_py_plugins.utils import UNESCAPE_RE, WHITESPACE_RE


def superscript_plugin(md: MarkdownIt) -> None:
    """Superscript (``<sup>``) tag plugin for Markdown-It-Py.

    This plugin is ported from `markdown-it-sup
    <https://github.com/markdown-it/markdown-it-sup>`_. Markup is based on the
    `Pandoc superscript extension
    <https://pandoc.org/MANUAL.html#superscripts-and-subscripts>`_.

    Place superscripted text within caret ``^`` characters. You must escape any
    spaces in the superscripted text. Note that you cannot use newline or tab
    characters, and that nested markup is not supported.

    Example usage:

    >>> from markdown_it import MarkdownIt
    >>> from mdit_py_plugins.superscript import superscript_plugin
    >>> md = MarkdownIt().use(superscript_plugin)
    >>> md.render("1^st^")
    '<p>1<sup>st</sup></p>\\n'
    >>> md.render("this^text\\\\ has\\\\ spaces^")
    '<p>this<sup>text has spaces</sup></p>\\n'
    """

    def superscript(state: StateInline, silent: bool) -> bool:
        """Parse inline text for superscripted text between caret ``^`` characters."""
        maximum = state.posMax
        start = state.pos

        if ord(state.src[start]) != 0x5E:  # Check if char is `^`
            return False
        if silent:  # Do not run any pairs in validation mode
            return False
        if start + 2 >= maximum:
            return False

        state.pos = start + 1
        found = False

        while state.pos < maximum:
            if ord(state.src[state.pos]) == 0x5E:  # Check if char is `^`
                found = True
                break
            state.md.inline.skipToken(state)

        if (not found) or (start + 1 == state.pos):
            state.pos = start
            return False

        content = state.src[start + 1 : state.pos]

        # Do not allow unescaped spaces/newlines inside
        if WHITESPACE_RE.search(content) is not None:
            state.pos = start
            return False

        # Found!
        state.posMax = state.pos
        state.pos = start + 1

        # Earlier we checked !silent, but this implementation does not need it
        token_so = state.push("sup_open", "sup", 1)
        token_so.markup = "^"

        token_t = state.push("text", "", 0)
        token_t.content = UNESCAPE_RE.sub(r"\1", content)

        token_sc = state.push("sup_close", "sup", -1)
        token_sc.markup = "^"

        state.pos = state.posMax + 1
        state.posMax = maximum
        return True

    md.inline.ruler.after("emphasis", "sup", superscript)
