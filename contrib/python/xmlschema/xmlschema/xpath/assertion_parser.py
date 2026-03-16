#
# Copyright (c), 2023-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from elementpath import XPath2Parser, XPathToken, XPathContext


class XsdAssertionXPathParser(XPath2Parser):
    """Parser for XSD 1.1 assertion facets."""


XsdAssertionXPathParser.unregister('last')
XsdAssertionXPathParser.unregister('position')


# noinspection PyUnusedLocal
@XsdAssertionXPathParser.method(
    XsdAssertionXPathParser.function('last', nargs=0)
)
def evaluate_last(self: XPathToken, context: XPathContext | None = None) -> None:
    raise self.missing_context("context item size is undefined")


# noinspection PyUnusedLocal
@XsdAssertionXPathParser.method(
    XsdAssertionXPathParser.function('position', nargs=0)
)
def evaluate_position(self: XPathToken, context: XPathContext | None = None) -> None:
    raise self.missing_context("context item position is undefined")
