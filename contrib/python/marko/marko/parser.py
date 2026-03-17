"""
Base parser
"""

from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Type, cast

from .source import Source


class Parser:
    r"""
    All elements defined in CommonMark's spec are included in the parser
    by default.

    Attributes:
        block_elements(dict): a dict of name: block_element pairs
        inline_elements(dict): a dict of name: inline_element pairs

    :param \*extras: extra elements to be included in parsing process.
    """

    def __init__(self) -> None:
        self.block_elements: dict[str, BlockElementType] = {}
        self.inline_elements: dict[str, InlineElementType] = {}

        for el in itertools.chain(
            (getattr(block, name) for name in block.__all__),
            (getattr(inline, name) for name in inline.__all__),
        ):
            self.add_element(el)

    def add_element(self, element: ElementType) -> None:
        """Add an element to the parser.

        :param element: the element class.

        .. note:: If one needs to call it inside ``__init__()``, please call it after
             ``super().__init__()`` is called.
        """
        dest: dict[str, ElementType] = {}
        if issubclass(element, inline.InlineElement):
            dest = self.inline_elements  # type: ignore
        elif issubclass(element, block.BlockElement):
            dest = self.block_elements  # type: ignore
        else:
            raise TypeError(
                "The element should be a subclass of either `BlockElement` or "
                "`InlineElement`."
            )
        dest[element.get_type()] = element

    def parse(self, text: str) -> block.Document:
        """Do the actual parsing and returns an AST or parsed element.

        :param text: the text to parse.
        :returns: the parsed root element
        """
        source = Source(text)
        source.parser = self
        doc = cast(block.Document, self.block_elements["Document"]())
        with source.under_state(doc):
            doc.children = self.parse_source(source)
            self.parse_inline(doc, source)
        return doc

    def parse_source(self, source: Source) -> list[block.BlockElement]:
        """Parse the source into a list of block elements."""
        element_list = self._build_block_element_list()
        ast: list[block.BlockElement] = []
        while not source.exhausted:
            for ele_type in element_list:
                if ele_type.match(source):
                    result = ele_type.parse(source)
                    if not hasattr(result, "priority"):
                        # In some cases ``parse()`` won't return the element, but
                        # instead some information to create one, which will be passed
                        # to ``__init__()``.
                        result = ele_type(result)  # type: ignore
                    ast.append(result)
                    break
            else:
                # Quit the current parsing and go back to the last level.
                break
        return ast

    def parse_inline(self, element: block.BlockElement, source: Source) -> None:
        """Inline parsing is postponed so that all link references
        are seen before that.
        """
        if element.inline_body:
            element.children = self._parse_inline(element.inline_body, source)
            # clear the inline body to avoid parsing it again.
            element.inline_body = ""
        else:
            for child in element.children:
                if isinstance(child, block.BlockElement):
                    self.parse_inline(child, source)

    def _parse_inline(self, text: str, source: Source) -> list[inline.InlineElement]:
        """Parses text into inline elements.
        RawText is not considered in parsing but created as a wrapper of holes
        that don't match any other elements.

        :param text: the text to be parsed.
        :returns: a list of inline elements.
        """
        element_list = self._build_inline_element_list()
        return inline_parser.parse(
            text, element_list, fallback=self.inline_elements["RawText"], source=source
        )

    def _build_block_element_list(self) -> list[BlockElementType]:
        """Return a list of block elements, ordered from highest priority to lowest."""
        return sorted(
            (e for e in self.block_elements.values() if not e.virtual),
            key=lambda e: e.priority,
            reverse=True,
        )

    def _build_inline_element_list(self) -> list[InlineElementType]:
        """Return a list of elements, each item is a list of elements
        with the same priority.
        """
        return [e for e in self.inline_elements.values() if not e.virtual]


from . import block, element, inline, inline_parser  # noqa

if TYPE_CHECKING:
    BlockElementType = Type[block.BlockElement]
    InlineElementType = Type[inline.InlineElement]
    ElementType = Type[element.Element]
