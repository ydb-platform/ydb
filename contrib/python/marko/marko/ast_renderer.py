"""
AST renderers for inspecting the markdown parsing result.
"""

from __future__ import annotations

import html
import json
from typing import TYPE_CHECKING, Any, overload

from marko.html_renderer import HTMLRenderer

from .helpers import camel_to_snake_case
from .renderer import Renderer, force_delegate

if TYPE_CHECKING:
    from marko import inline
    from marko.element import Element


class ASTRenderer(Renderer):
    """Render as AST structure.

    Example::

        >>> print(markdown('# heading', ASTRenderer))
        {'footnotes': [],
         'link_ref_defs': {},
         'children': [{'level': 1, 'children': ['heading'], 'element': 'heading'}],
         'element': 'document'}
    """

    delegate = False

    @force_delegate
    def render_raw_text(self, element: inline.RawText) -> dict[str, Any]:
        return {
            "element": "raw_text",
            "children": (
                html.unescape(element.children) if element.escape else element.children
            ),
            "escape": element.escape,
        }

    @overload
    def render_children(self, element: list[Element]) -> list[dict[str, Any]]: ...

    @overload
    def render_children(self, element: Element) -> dict[str, Any]: ...

    @overload
    def render_children(self, element: str) -> str: ...

    def render_children(self, element):
        if isinstance(element, list):
            return [self.render(e) for e in element]
        if isinstance(element, str):
            return element
        rv = {k: v for k, v in element.__dict__.items() if not k.startswith("_")}
        if "children" in rv:
            rv["children"] = self.render(rv["children"])
        rv["element"] = camel_to_snake_case(element.__class__.__name__)
        return rv


class XMLRenderer(Renderer):
    """Render as XML format AST.

    It will render the parsed result and XML string and you can print it or
    write it to a file.

    Example::

        >>> print(markdown('# heading', XMLRenderer))
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE document SYSTEM "CommonMark.dtd">
        <document footnotes="[]" link_ref_defs="{}">
        <heading level="1">
            heading
        </heading>
        </document>
    """

    delegate = False

    def __enter__(self) -> XMLRenderer:
        self.indent = 0
        return super().__enter__()

    def __exit__(self, *args: Any) -> None:
        self.indent = 0
        return super().__exit__(*args)

    def render_children(self, element: Element) -> str:
        lines = []
        if element is self.root_node:
            lines.append(" " * self.indent + '<?xml version="1.0" encoding="UTF-8"?>')
            lines.append(
                " " * self.indent + '<!DOCTYPE document SYSTEM "CommonMark.dtd">'
            )
        attrs = {
            k: v
            for k, v in element.__dict__.items()
            if not k.startswith("_") and k not in ("body", "children")
        }
        attr_str = "".join(f' {k}="{v}"' for k, v in attrs.items())
        element_name = camel_to_snake_case(element.__class__.__name__)
        lines.append(" " * self.indent + f"<{element_name}{attr_str}>")
        children = getattr(element, "body", None) or getattr(element, "children", None)
        if children:
            self.indent += 2
            if isinstance(children, str):  # type: ignore
                lines.append(
                    " " * self.indent
                    + HTMLRenderer.escape_html(json.dumps(children)[1:-1])  # type: ignore
                )
            else:
                lines.extend(self.render(child) for child in children)  # type: ignore
            self.indent -= 2
            lines.append(" " * self.indent + f"</{element_name}>")
        else:
            lines[-1] = lines[-1][:-1] + " />"
        return "\n".join(lines)
