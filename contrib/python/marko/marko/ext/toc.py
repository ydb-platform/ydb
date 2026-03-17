"""
TOC extension
~~~~~~~~~~~~~

Renders the TOC(Table Of Content) for a markdown document.
This requires to install `toc` extras::

    pip install marko[toc]

Arguments:
    * opening: the opening tag, defaults to <ul>
    * closing: the closing tag, defaults to </ul>
    * item_format: the toc item format, defaults to '<li><a href="#{slug}">{text}</a></li>'

Usage::

    from marko import Markdown

    markdown = Markdown(extensions=['toc'])

    print(markdown(text))
    print(markdown.renderer.render_toc())

"""

import re

from marko.helpers import MarkoExtension, render_dispatch  # type: ignore
from marko.html_renderer import HTMLRenderer

try:
    from slugify import slugify
except ImportError:

    def slugify(source: str) -> str:  # type: ignore[misc]
        return re.sub(r"[^\w\- ]+", "", source).strip().lower().replace(" ", "-")


class TocRendererMixin:
    opening = "<ul>"
    closing = "</ul>"
    item_format = '<li><a href="#{slug}">{text}</a></li>'

    def __enter__(self):
        self.headings = []
        return super().__enter__()

    def render_toc(self, maxdepth=3):
        if not self.headings:
            return ""
        first_level = None
        last_level = None
        rv = []
        for level, slug, text in self.headings:
            if first_level is not None and level >= first_level + maxdepth:
                continue

            if first_level is None:
                first_level = level
                last_level = level
                rv.append(self.opening + "\n")

            if last_level == level - 1:
                rv.append(self.opening + "\n")
                last_level = level
            while last_level > level:
                rv.append(self.closing + "\n")
                last_level -= 1
            # last_level == level
            rv.append(self.item_format.format(slug=slug, text=text) + "\n")
        for _ in range(first_level, last_level + 1):
            rv.append(self.closing + "\n")

        return "".join(rv)

    @render_dispatch(HTMLRenderer)
    def render_heading(self, element):
        children = self.render_children(element)
        raw = re.sub(r"<.+?>", "", children)
        slug = slugify(raw)
        self.headings.append((int(element.level), slug, children))
        return '<h{0} id="{1}">{2}</h{0}>\n'.format(element.level, slug, children)


def make_extension(opening=None, closing=None, item_format=None):
    options = {}
    if opening:
        options["opening"] = opening
    if closing:
        options["closing"] = closing
    if item_format:
        options["item_format"] = item_format
    renderer_mixins = [type("TocRendererMixin", (TocRendererMixin,), options)]
    return MarkoExtension(renderer_mixins=renderer_mixins)
