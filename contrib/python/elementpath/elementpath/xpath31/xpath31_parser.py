#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
XPath 3.1 implementation
"""
from typing import ClassVar

import elementpath.namespaces as ns
from elementpath.datatypes import QName
from elementpath.xpath30 import XPath30Parser


class XPath31Parser(XPath30Parser):
    """
    XPath 3.1 expression parser class.
    """
    version = '3.1'

    DEFAULT_NAMESPACES: ClassVar[dict[str, str]] = {
        'map': ns.XPATH_MAP_FUNCTIONS_NAMESPACE,
        'array': ns.XPATH_ARRAY_FUNCTIONS_NAMESPACE,
        **XPath30Parser.DEFAULT_NAMESPACES
    }

    # https://www.w3.org/TR/xpath-31/#id-reserved-fn-names
    RESERVED_FUNCTION_NAMES = {
        'array', 'attribute', 'comment', 'document-node', 'element', 'empty-sequence',
        'function', 'if', 'item', 'map', 'namespace-node', 'node', 'processing-instruction',
        'schema-attribute', 'schema-element', 'switch', 'text', 'typeswitch',
    }

    function_signatures: dict[tuple[QName, int], str] = XPath30Parser.function_signatures.copy()
