# mypy: disable-error-code="no-redef"
from __future__ import annotations

import re

from marko.helpers import render_dispatch
from marko.html_renderer import HTMLRenderer
from marko.md_renderer import MarkdownRenderer


class GFMRendererMixin:
    tagfilter = re.compile(
        r"<(title|textarea|style|xmp|iframe|noembed|noframes|script|plaintext)",
        flags=re.I,
    )
    tagfilter_no_open = re.compile(
        r"(?<!^)( *)<(title|textarea|style|xmp|iframe|noembed|noframes|script|plaintext)",
        flags=re.I,
    )

    @render_dispatch(HTMLRenderer)
    def render_paragraph(self, element):
        children = self.render_children(element)
        template = '<input{} disabled="" type="checkbox">{}'
        if hasattr(element, "checked"):
            children = template.format(
                ' checked=""' if element.checked else "", children
            )
        if element._tight:
            return children
        else:
            return f"<p>{children}</p>\n"

    @render_paragraph.dispatch(MarkdownRenderer)
    def render_paragraph(self, element):
        para = self.render_children(element)
        if hasattr(element, "checked"):
            para = f"[{'x' if element.checked else ' '}]{para}"
        line = self._prefix + para + "\n"
        self._prefix = self._second_prefix
        return line

    @render_dispatch(HTMLRenderer)
    def render_strikethrough(self, element):
        return f"<del>{self.render_children(element)}</del>"

    @render_strikethrough.dispatch(MarkdownRenderer)
    def render_strikethrough(self, element):
        return f"~~{self.render_children(element)}~~"

    @render_dispatch(HTMLRenderer)
    def render_inline_html(self, element):
        return self.tagfilter.sub(r"&lt;\1", element.children)

    @render_dispatch(HTMLRenderer)
    def render_html_block(self, element):
        return self.tagfilter_no_open.sub(r"\1&lt;\2", element.body)

    @render_dispatch(HTMLRenderer)
    def render_table(self, element):
        head, *body = element.children
        theader = f"<thead>\n{self.render(head)}</thead>"
        tbody = ""
        if body:
            tbody = "\n<tbody>\n{}</tbody>".format(
                "".join(self.render(row) for row in body)
            )
        return f"<table>\n{theader}{tbody}</table>"

    @render_table.dispatch(MarkdownRenderer)
    def render_table(self, element):
        lines = []
        head, *body = element.children
        lines.append(self.render(head))
        lines.append(f"| {' | '.join(element.delimiters)} |\n")
        for row in body:
            lines.append(self.render(row))
        return "".join(lines)

    @render_dispatch(HTMLRenderer)
    def render_table_row(self, element):
        return f"<tr>\n{self.render_children(element)}</tr>\n"

    @render_table_row.dispatch(MarkdownRenderer)
    def render_table_row(self, element):
        return f"| {' | '.join(self.render(cell) for cell in element.children)} |\n"

    @render_dispatch(HTMLRenderer)
    def render_table_cell(self, element):
        tag = "th" if element.header else "td"
        align = ""
        if element.align:
            align = f' align="{element.align}"'
        return "<{tag}{align}>{children}</{tag}>\n".format(
            tag=tag, children=self.render_children(element), align=align
        )

    @render_table_cell.dispatch(MarkdownRenderer)
    def render_table_cell(self, element):
        return self.render_children(element).replace("|", "\\|")

    @render_dispatch(HTMLRenderer)
    def render_url(self, element):
        return self.render_link(element)

    @render_url.dispatch(MarkdownRenderer)
    def render_url(self, element):
        return element.dest

    @render_dispatch(HTMLRenderer)
    def render_alert(self, element):
        header = self.escape_html(element.alert_type)
        children = self.render_children(element)
        return (
            f'<blockquote class="alert alert-{element.alert_type.lower()}">\n'
            f"<p>{header.title()}</p>\n{children}</blockquote>\n"
        )

    @render_alert.dispatch(MarkdownRenderer)
    def render_alert(self, element):
        lines: list[str] = []
        lines.append(self._prefix + f"> [!{element.alert_type}]\n")
        with self.container("> ", "> "):
            for child in element.children:
                lines.append(self.render(child))
        self._prefix = self._second_prefix
        return "".join(lines)
