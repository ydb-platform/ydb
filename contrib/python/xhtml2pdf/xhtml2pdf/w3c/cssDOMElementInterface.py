# Copyright (C) 2002-2004  TechGame Networks, LLC.
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the BSD style License as found in the
# LICENSE file included with this distribution.
#
# Modified by Dirk Holtwick <holtwick@web.de>, 2007-2008

# ruff: noqa: N802, N803, N815, N816, N999
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, ClassVar

from xhtml2pdf.w3c import css

if TYPE_CHECKING:
    from typing_extensions import Self

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ Definitions
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSDOMElementInterface(css.CSSElementInterfaceAbstract):
    """An implementation of css.CSSElementInterfaceAbstract for xml.dom Element Nodes."""

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Constants / Variables / Etc.
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    style = None

    _pseudoStateHandlerLookup: ClassVar[dict[str, Callable[[Self], bool]]] = {
        "first-child": lambda self: not bool(self.getPreviousSibling()),
        "not-first-child": lambda self: bool(self.getPreviousSibling()),
        "last-child": lambda self: not bool(self.getNextSibling()),
        "not-last-child": lambda self: bool(self.getNextSibling()),
        "middle-child": lambda self: not bool(self.getPreviousSibling())
        and not bool(self.getNextSibling()),
        "not-middle-child": lambda self: bool(self.getPreviousSibling())
        or bool(self.getNextSibling()),
        # XXX 'first-line':
    }

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Definitions
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __init__(self, domElement, cssParser=None) -> None:
        self.domElement = domElement
        # print self.domElement.attributes
        if cssParser is not None:
            self.onCSSParserVisit(cssParser)

    def onCSSParserVisit(self, cssParser):
        styleSrc = self.getStyleAttr()
        if styleSrc:
            style = cssParser.parseInline(styleSrc)
            self.setInlineStyle(style)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def matchesNode(self, namespace_tagName):
        namespace, tagName = namespace_tagName
        if tagName not in {"*", self.domElement.tagName}:
            return False
        if namespace in {None, "", "*"}:
            # matches any namespace
            return True
        # full compare
        return namespace == self.domElement.namespaceURI

    def getAttr(self, name, default=NotImplemented):
        attr_value = self.domElement.attributes.get(name)
        if attr_value is not None:
            return attr_value.value
        return default

    def getIdAttr(self):
        return self.getAttr("id", "")

    def getClassAttr(self):
        return self.getAttr("class", "")

    def getStyleAttr(self):
        return self.getAttr("style", None)

    def inPseudoState(self, name, params=()):
        handler = self._pseudoStateHandlerLookup.get(name, lambda _: False)
        return handler(self)

    def iterXMLParents(self, *, includeSelf=False):
        klass = type(self)
        current = self.domElement
        if not includeSelf:
            current = current.parentNode
        while (current is not None) and (current.nodeType == current.ELEMENT_NODE):
            yield klass(current)
            current = current.parentNode

    def getPreviousSibling(self):
        sibling = self.domElement.previousSibling
        while sibling:
            if sibling.nodeType == sibling.ELEMENT_NODE:
                return sibling
            sibling = sibling.previousSibling
        return None

    def getNextSibling(self):
        sibling = self.domElement.nextSibling
        while sibling:
            if sibling.nodeType == sibling.ELEMENT_NODE:
                return sibling
            sibling = sibling.nextSibling
        return None

    def getInlineStyle(self):
        return self.style

    def setInlineStyle(self, style):
        self.style = style
