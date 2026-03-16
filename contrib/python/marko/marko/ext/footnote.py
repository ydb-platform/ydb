# mypy: disable-error-code="no-redef"
"""
Footnotes extension
~~~~~~~~~~~~~~~~~~~

Enable footnotes parsing and renderering in Marko.

Usage::

    from marko import Markdown

    text = 'Foo[^1]\\n\\n[^1]: This is a footnote.\\n'
    markdown = Markdown(extensions=['footnote'])
    print(markdown(text))

"""
from __future__ import annotations

import re

from marko import HTMLRenderer, block, helpers, inline
from marko.md_renderer import MarkdownRenderer


class Document(block.Document):
    def __init__(self):
        super().__init__()
        self.footnotes = {}


class FootnoteDef(block.BlockElement):
    pattern = re.compile(r" {,3}\[\^([^\]]+)\]:[^\n\S]*(?=\S| {4})")
    priority = 6

    def __init__(self, match):
        self.label = helpers.normalize_label(match.group(1))
        self._prefix = re.escape(match.group())
        self._second_prefix = r" {1,4}"

    @classmethod
    def match(cls, source):
        return source.expect_re(cls.pattern)

    @classmethod
    def parse(cls, source):
        state = cls(source.match)
        with source.under_state(state):
            state.children = source.parser.parse_source(source)
        source.root.footnotes[state.label] = state
        return state


class FootnoteRef(inline.InlineElement):
    pattern = re.compile(r"\[\^([^\]]+)\]")
    priority = 6

    def __init__(self, match):
        self.label = helpers.normalize_label(match.group(1))

    @classmethod
    def find(cls, text, *, source):
        for match in super().find(text, source=source):
            label = helpers.normalize_label(match.group(1))
            if label in source.root.footnotes:
                yield match


class FootnoteRendererMixin:
    def __init__(self):
        super().__init__()
        self.footnotes = []

    @helpers.render_dispatch(HTMLRenderer)
    def render_footnote_ref(self, element):
        if element.label not in self.footnotes:
            self.footnotes.append(element.label)
        idx = self.footnotes.index(element.label) + 1
        return (
            '<sup class="footnote-ref" id="fnref-{lab}">'
            '<a href="#fn-{lab}">{id}</a></sup>'.format(
                lab=self.escape_url(element.label), id=idx
            )
        )

    @render_footnote_ref.dispatch(MarkdownRenderer)
    def render_footnote_ref(self, element):
        return f"[^{element.label}]"

    @helpers.render_dispatch(HTMLRenderer)
    def render_footnote_def(self, element):
        return ""

    @render_footnote_def.dispatch(MarkdownRenderer)
    def render_footnote_def(self, element):
        return f"[^{element.label}]: {self.render_children(element)}"

    def _render_footnote_def(self, element):
        children = self.render_children(element).rstrip()
        back = f'<a href="#fnref-{element.label}" class="footnote">&#8617;</a>'
        if children.endswith("</p>"):
            children = re.sub(r"</p>$", f"{back}</p>", children)
        else:
            children = f"{children}<p>{back}</p>\n"
        return '<li id="fn-{}">\n{}</li>\n'.format(
            self.escape_url(element.label), children
        )

    @helpers.render_dispatch((HTMLRenderer, MarkdownRenderer))
    def render_document(self, element):
        text = self.render_children(element)
        items = [self.root_node.footnotes[label] for label in self.footnotes]
        if not items or not isinstance(self, HTMLRenderer):
            return text
        children = "".join(self._render_footnote_def(item) for item in items)
        footnotes = f'<div class="footnotes">\n<ol>\n{children}</ol>\n</div>\n'
        self.footnotes = []
        return text + footnotes


def make_extension():
    return helpers.MarkoExtension(
        elements=[Document, FootnoteDef, FootnoteRef],
        renderer_mixins=[FootnoteRendererMixin],
    )
