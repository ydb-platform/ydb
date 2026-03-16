"""
Markdown renderer
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator, cast

from .renderer import Renderer

if TYPE_CHECKING:
    from . import block, inline


class MarkdownRenderer(Renderer):
    """Render the AST back to markdown document.

    It is useful for, e.g. merging sections and formatting documents.
    For convenience, markdown renderer provides all render functions for basic elements
    and those from common extensions.
    """

    def __init__(self) -> None:
        super().__init__()
        self._prefix = ""
        self._second_prefix = ""

    def __enter__(self) -> MarkdownRenderer:
        self._prefix = ""
        self._second_prefix = ""
        return super().__enter__()

    @contextmanager
    def container(
        self, prefix: str, second_prefix: str = ""
    ) -> Generator[None, None, None]:
        old_prefix = self._prefix
        old_second_prefix = self._second_prefix
        self._prefix += prefix
        self._second_prefix += second_prefix
        yield
        self._prefix = old_prefix
        self._second_prefix = old_second_prefix

    def render_paragraph(self, element: block.Paragraph) -> str:
        children = self.render_children(element)
        line = self._prefix + children + "\n"
        self._prefix = self._second_prefix
        return line

    def render_list(self, element: block.List) -> str:
        result = []
        if element.ordered:
            for num, child in enumerate(element.children, element.start):
                with self.container(f"{num}. ", " " * (len(str(num)) + 2)):
                    result.append(self.render(child))
        else:
            for child in element.children:
                with self.container(f"{element.bullet} ", "  "):
                    result.append(self.render(child))
        self._prefix = self._second_prefix
        return "".join(result)

    def render_list_item(self, element: block.ListItem) -> str:
        return self.render_children(element)

    def render_quote(self, element: block.Quote) -> str:
        with self.container("> ", "> "):
            result = self.render_children(element).rstrip("\n")
        self._prefix = self._second_prefix
        return result + "\n"

    def render_fenced_code(self, element: block.FencedCode) -> str:
        extra = f" {element.extra}" if element.extra else ""
        lines = [self._prefix + f"```{element.lang}{extra}"]
        lines.extend(
            self._second_prefix + line
            for line in self.render_children(element).splitlines()
        )
        lines.append(self._second_prefix + "```")
        self._prefix = self._second_prefix
        return "\n".join(lines) + "\n"

    def render_code_block(self, element: block.CodeBlock) -> str:
        indent = " " * 4
        lines = self.render_children(element).splitlines()
        lines = [self._prefix + indent + lines[0]] + [
            self._second_prefix + indent + line for line in lines[1:]
        ]
        self._prefix = self._second_prefix
        return "\n".join(lines) + "\n"

    def render_html_block(self, element: block.HTMLBlock) -> str:
        result = self._prefix + element.body + "\n"  # type: ignore[attr-defined]
        self._prefix = self._second_prefix
        return result

    def render_thematic_break(self, element: block.ThematicBreak) -> str:
        result = self._prefix + "* * *\n"
        self._prefix = self._second_prefix
        return result

    def render_heading(self, element: block.Heading) -> str:
        result = (
            self._prefix
            + "#" * element.level
            + " "
            + self.render_children(element)
            + "\n"
        )
        self._prefix = self._second_prefix
        return result

    def render_setext_heading(self, element: block.SetextHeading) -> str:
        return self.render_heading(cast("block.Heading", element))

    def render_blank_line(self, element: block.BlankLine) -> str:
        result = self._prefix + "\n"
        self._prefix = self._second_prefix
        return result

    def render_link_ref_def(self, element: block.LinkRefDef) -> str:
        link_text = element.dest
        if element.title:
            link_text += f" {element.title}"
        return f"[{element.label}]: {link_text}\n"

    def render_emphasis(self, element: inline.Emphasis) -> str:
        return f"*{self.render_children(element)}*"

    def render_strong_emphasis(self, element: inline.StrongEmphasis) -> str:
        return f"**{self.render_children(element)}**"

    def render_inline_html(self, element: inline.InlineHTML) -> str:
        return cast(str, element.children)

    def render_link(self, element: inline.Link) -> str:
        link_text = self.render_children(element)
        link_title = (
            '"{}"'.format(element.title.replace('"', '\\"')) if element.title else None
        )
        assert self.root_node
        label = next(
            (
                k
                for k, v in self.root_node.link_ref_defs.items()
                if v == (element.dest, link_title)
            ),
            None,
        )
        if label is not None:
            if label == link_text:
                return f"[{label}]"
            return f"[{link_text}][{label}]"
        title = f" {link_title}" if link_title is not None else ""
        return f"[{link_text}]({element.dest}{title})"

    def render_auto_link(self, element: inline.AutoLink) -> str:
        return f"<{element.dest}>"

    def render_image(self, element: inline.Image) -> str:
        template = "![{}]({}{})"
        title = (
            ' "{}"'.format(element.title.replace('"', '\\"')) if element.title else ""
        )
        return template.format(self.render_children(element), element.dest, title)

    def render_literal(self, element: inline.Literal) -> str:
        return f"\\{element.children}"

    def render_raw_text(self, element: inline.RawText) -> str:
        from .ext.pangu import PANGU_RE

        return re.sub(PANGU_RE, " ", element.children)

    def render_line_break(self, element: inline.LineBreak) -> str:
        return "\n" if element.soft else "\\\n"

    def render_code_span(self, element: inline.CodeSpan) -> str:
        text = element.children
        if text and text[0] == "`" or text[-1] == "`":
            return f"`` {text} ``"
        return f"`{element.children}`"
