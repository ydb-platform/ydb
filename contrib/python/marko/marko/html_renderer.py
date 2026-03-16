"""
HTML renderer
"""

from __future__ import annotations

import html
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote

from .renderer import Renderer

if TYPE_CHECKING:
    from . import block, inline


class HTMLRenderer(Renderer):
    """The most common renderer for markdown parser"""

    def render_paragraph(self, element: block.Paragraph) -> str:
        children = self.render_children(element)
        if element._tight:  # type: ignore
            return children
        else:
            return f"<p>{children}</p>\n"

    def render_list(self, element: block.List) -> str:
        if element.ordered:
            tag = "ol"
            extra = f' start="{element.start}"' if element.start != 1 else ""
        else:
            tag = "ul"
            extra = ""
        return "<{tag}{extra}>\n{children}</{tag}>\n".format(
            tag=tag, extra=extra, children=self.render_children(element)
        )

    def render_list_item(self, element: block.ListItem) -> str:
        if len(element.children) == 1 and getattr(element.children[0], "_tight", False):  # type: ignore
            sep = ""
        else:
            sep = "\n"
        return f"<li>{sep}{self.render_children(element)}</li>\n"

    def render_quote(self, element: block.Quote) -> str:
        return f"<blockquote>\n{self.render_children(element)}</blockquote>\n"

    def render_fenced_code(self, element: block.FencedCode) -> str:
        lang = (
            f' class="language-{self.escape_html(element.lang)}"'
            if element.lang
            else ""
        )
        return "<pre><code{}>{}</code></pre>\n".format(
            lang, html.escape(element.children[0].children)  # type: ignore
        )

    def render_code_block(self, element: block.CodeBlock) -> str:
        return self.render_fenced_code(cast("block.FencedCode", element))

    def render_html_block(self, element: block.HTMLBlock) -> str:
        return element.body

    def render_thematic_break(self, element: block.ThematicBreak) -> str:
        return "<hr />\n"

    def render_heading(self, element: block.Heading) -> str:
        return "<h{level}>{children}</h{level}>\n".format(
            level=element.level, children=self.render_children(element)
        )

    def render_setext_heading(self, element: block.SetextHeading) -> str:
        return self.render_heading(cast("block.Heading", element))

    def render_blank_line(self, element: block.BlankLine) -> str:
        return ""

    def render_link_ref_def(self, element: block.LinkRefDef) -> str:
        return ""

    def render_emphasis(self, element: inline.Emphasis) -> str:
        return f"<em>{self.render_children(element)}</em>"

    def render_strong_emphasis(self, element: inline.StrongEmphasis) -> str:
        return f"<strong>{self.render_children(element)}</strong>"

    def render_inline_html(self, element: inline.InlineHTML) -> str:
        return cast(str, element.children)

    def render_plain_text(self, element: Any) -> str:
        if isinstance(element.children, str):
            return self.escape_html(element.children)
        return self.render_children(element)

    def render_link(self, element: inline.Link) -> str:
        template = '<a href="{}"{}>{}</a>'
        title = f' title="{self.escape_html(element.title)}"' if element.title else ""
        url = self.escape_url(element.dest)
        body = self.render_children(element)
        return template.format(url, title, body)

    def render_auto_link(self, element: inline.AutoLink) -> str:
        return self.render_link(cast("inline.Link", element))

    def render_image(self, element: inline.Image) -> str:
        template = '<img src="{}" alt="{}"{} />'
        title = f' title="{self.escape_html(element.title)}"' if element.title else ""
        url = self.escape_url(element.dest)
        render_func = self.render
        self.render = self.render_plain_text  # type: ignore
        body = self.render_children(element)
        self.render = render_func  # type: ignore
        return template.format(url, body, title)

    def render_literal(self, element: inline.Literal) -> str:
        return self.render_raw_text(cast("inline.RawText", element))

    def render_raw_text(self, element: inline.RawText) -> str:
        return self.escape_html(element.children)

    def render_line_break(self, element: inline.LineBreak) -> str:
        if element.soft:
            return "\n"
        return "<br />\n"

    def render_code_span(self, element: inline.CodeSpan) -> str:
        return f"<code>{html.escape(cast(str, element.children))}</code>"

    @staticmethod
    def escape_html(raw: str) -> str:
        return html.escape(html.unescape(raw)).replace("&#x27;", "'")

    @staticmethod
    def escape_url(raw: str) -> str:
        """
        Escape urls to prevent code injection craziness. (Hopefully.)
        """
        return html.escape(quote(html.unescape(raw), safe="/#:()*?=%@+,&"))
