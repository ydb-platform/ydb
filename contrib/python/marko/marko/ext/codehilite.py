r"""
Code highlight extension
~~~~~~~~~~~~~~~~~~~~~~~~

Enable code highlight using ``pygments``. This requires to install `codehilite` extras::

    pip install marko[codehilite]

Arguments:
    All arguments are passed to ``pygments.formatters.html.HtmlFormatter``.

Usage::

    from marko import Markdown

    markdown = Markdown(extensions=['codehilite'])
    markdown.convert('```python filename="my_script.py"\nprint('hello world')\n```')
"""

import json

from pygments import highlight
from pygments.formatters import html
from pygments.lexers import get_lexer_by_name, guess_lexer
from pygments.util import ClassNotFound

from marko import HTMLRenderer
from marko.helpers import MarkoExtension, render_dispatch


def _parse_extras(line):
    if not line:
        return {}
    extras = {}
    for token in line.split(","):
        k, has_eq, v = token.partition("=")
        if has_eq:
            try:
                parsed_v = json.loads(v)
                extras[k] = parsed_v
            except json.JSONDecodeError:
                continue
    return extras


class CodeHiliteRendererMixin:
    options = {}  # type: dict

    @render_dispatch(HTMLRenderer)
    def render_fenced_code(self, element):
        code = element.children[0].children
        options = {**self.options, **_parse_extras(getattr(element, "extra", None))}
        if element.lang:
            try:
                lexer = get_lexer_by_name(element.lang, stripall=True)
            except ClassNotFound:
                lexer = guess_lexer(code)
        else:
            lexer = guess_lexer(code)
        formatter = html.HtmlFormatter(**options)
        return highlight(code, lexer, formatter)


def make_extension(**options):
    mixin_cls = type(
        "CodeHiliteRendererMixin", (CodeHiliteRendererMixin,), {"options": options}
    )
    return MarkoExtension(renderer_mixins=[mixin_cls])
