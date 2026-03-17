"""
Base renderer class
"""

from __future__ import annotations

import html
import re
from typing import TYPE_CHECKING, Any, Callable, TypeVar

if TYPE_CHECKING:
    from .block import Document
    from .element import Element

_T = TypeVar("_T", bound="Renderer")
_charref_bak = html._charref  # type: ignore[attr-defined]


class Renderer:
    """The base class of renderers.

    A custom renderer should subclass this class and include your own render functions.

    A render function should:

    * be named as ``render_<element_name>``, where the ``element_name`` is the snake
      case form of the element class name, the renderer will search the corresponding
      function in this way.
    * accept the element instance and return any output you want.

    If no corresponding render function is found, renderer will fallback to call
    :meth:`Renderer.render_children`.
    """

    #: Whether to delegate rendering to specific render functions.
    # It is useful when the renderer is to be mixed with other renderers. When set to False,
    # the render functions from the base renderer will be ignored unless decorated by
    # ``@force_delegate``.
    delegate: bool = True

    _charref = re.compile(
        r"&(#[0-9]{1,7};" r"|#[xX][0-9a-fA-F]{1,6};" r"|[^\t\n\f <&#;]{1,32};)"
    )

    def __init__(self) -> None:
        self.root_node: Document | None = None

    def __enter__(self: _T) -> _T:
        """Provide a context so that root_node can be reset after render."""
        html._charref = self._charref  # type: ignore[attr-defined]
        return self

    def __exit__(self, *args: Any) -> None:
        html._charref = _charref_bak  # type: ignore[attr-defined]
        self.root_node = None

    def render(self, element: Element) -> Any:
        """Renders the given element to string.

        :param element: a element to be rendered.
        :returns: the output string or any values.
        """
        from .block import Document

        # Store the root node since it may be required by the render functions
        if not self.root_node:  # pragma: no cover
            if isinstance(element, Document):
                self.root_node = element
            else:
                # Make a dummy root node from it
                self.root_node = Document()
                self.root_node.children = [element]
        if hasattr(element, "get_type"):
            func_name = "render_" + element.get_type(snake_case=True)
            render_func = getattr(self, func_name, None)
            if render_func is not None and (
                getattr(render_func, "_force_delegate", False) or self.delegate
            ):
                return render_func(element)
        return self.render_children(element)

    def render_children(self, element: Any) -> Any:
        """
        Recursively renders child elements. Joins the rendered
        strings with no space in between.

        If newlines / spaces are needed between elements, add them
        in their respective templates, or override this function
        in the renderer subclass, so that whitespace won't seem to
        appear magically for anyone reading your program.

        :param element: a branch node who has children attribute.
        """
        rendered = [self.render(child) for child in element.children]  # type: ignore
        return "".join(rendered)


_F = TypeVar("_F", bound=Callable)


def force_delegate(func: _F) -> _F:
    """
    A decorator to allow delegation for the specified method even if cls.delegate = False
    """
    func._force_delegate = True  # type: ignore[attr-defined]
    return func
