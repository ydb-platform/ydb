#
# Copyright (c), 2023-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
Optional module for handling XPath 3 parsing on XSD 1.1 assertions.
"""
from typing import Optional

from elementpath import XPathToken, XPathContext
from elementpath.xpath3 import XPath3Parser

__all__ = ['XPath3Parser', 'XsdAssertionXPath3Parser']


class XsdAssertionXPath3Parser(XPath3Parser):
    """Parser for XSD 1.1 assertion facets with XPath 3."""


XsdAssertionXPath3Parser.unregister('last')
XsdAssertionXPath3Parser.unregister('position')


# noinspection PyUnusedLocal
@XsdAssertionXPath3Parser.method(
    XsdAssertionXPath3Parser.function('last', nargs=0)
)
def evaluate_last(self: XPathToken, context: Optional[XPathContext] = None) -> None:
    raise self.missing_context("context item size is undefined")  # pragma: no cover


# noinspection PyUnusedLocal
@XsdAssertionXPath3Parser.method(
    XsdAssertionXPath3Parser.function('position', nargs=0)
)
def evaluate_position(self: XPathToken, context: Optional[XPathContext] = None) -> None:
    raise self.missing_context("context item position is undefined")  # pragma: no cover
