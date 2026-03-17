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
XPath 3.0 implementation - part 1 (parser class)

Refs:
  - https://www.w3.org/TR/2014/REC-xpath-30-20140408/
  - https://www.w3.org/TR/xpath-functions-30/
"""
from copy import deepcopy
from typing import Any, ClassVar, Optional

from elementpath.namespaces import XPATH_MATH_FUNCTIONS_NAMESPACE
from elementpath.datatypes import QName
from elementpath.xpath2 import XPath2Parser


DecimalFormatsType = dict[Optional[str], dict[str, str]]


class XPath30Parser(XPath2Parser):
    """
    XPath 3.0 expression parser class. Accepts all XPath 2.0 options as keyword
    arguments, but the *strict* option is ignored because XPath 3.0+ has braced
    URI literals and the expanded name syntax is not compatible.

    :param args: the same positional arguments of class :class:`elementpath.XPath2Parser`.
    :param decimal_formats: a mapping with statically known decimal formats.
    :param defuse_xml: if `True` defuse XML data before parsing, that is the default.
    :param kwargs: the same keyword arguments of class :class:`elementpath.XPath2Parser`.
    """
    version = '3.0'

    DEFAULT_NAMESPACES: ClassVar[dict[str, str]] = {
        'math': XPATH_MATH_FUNCTIONS_NAMESPACE, **XPath2Parser.DEFAULT_NAMESPACES
    }
    PATH_STEP_SYMBOLS = {
        '(integer)', '(string)', '(float)', '(decimal)', '(name)',
        '*', '@', '..', '.', '(', '{', 'Q{', '$',
    }

    # https://www.w3.org/TR/xpath-30/#id-reserved-fn-names
    RESERVED_FUNCTION_NAMES = {
        'attribute', 'comment', 'document-node', 'element', 'empty-sequence',
        'function', 'if', 'item', 'namespace-node', 'node', 'processing-instruction',
        'schema-attribute', 'schema-element', 'switch', 'text', 'typeswitch',
    }

    function_signatures: dict[tuple[QName, int], str] = XPath2Parser.function_signatures.copy()

    decimal_formats: DecimalFormatsType = {
        None: {
            'decimal-separator': '.',
            'grouping-separator': ',',
            'exponent-separator': 'e',
            'infinity': 'Infinity',
            'minus-sign': '-',
            'NaN': 'NaN',
            'percent': '%',
            'per-mille': '‰',
            'zero-digit': '0',
            'digit': '#',
            'pattern-separator': ';',
        }
    }

    def __init__(self, *args: Any, decimal_formats: Optional[DecimalFormatsType] = None,
                 defuse_xml: bool = True, **kwargs: Any) -> None:
        kwargs.pop('strict', None)
        super(XPath30Parser, self).__init__(*args, **kwargs)

        if decimal_formats is not None:
            self.decimal_formats = deepcopy(self.decimal_formats)

            for k, v in decimal_formats.items():
                if k is not None:
                    self.decimal_formats[k] = self.decimal_formats[None].copy()
                    self.decimal_formats[k].update(v)

            if None in decimal_formats:
                self.decimal_formats[None].update(decimal_formats[None])
        if not defuse_xml:
            self.defuse_xml = defuse_xml

    def __str__(self) -> str:
        args = []
        if self.decimal_formats != self.__class__.decimal_formats:
            args.append(f'decimal_formats={self.decimal_formats!r}')
        if not self.defuse_xml:
            args.append('defuse_xml=False')
        if not args:
            return super().__str__()

        repr_string = super().__str__()[:-1]
        if repr_string.endswith('('):
            return f"{repr_string}{', '.join(args)})"
        return f"{repr_string}, {', '.join(args)})"
