#
# Copyright (c), 2023-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from elementpath import XPath2Parser

XSD_IDENTITY_XPATH_SYMBOLS = frozenset((
    'processing-instruction', 'following-sibling', 'preceding-sibling',
    'ancestor-or-self', 'attribute', 'following', 'namespace', 'preceding',
    'ancestor', 'position', 'comment', 'parent', 'child', 'false', 'text', 'node',
    'true', 'last', 'not', 'and', 'mod', 'div', 'or', '..', '//', '!=', '<=', '>=',
    '(', ')', '[', ']', '.', '@', ',', '/', '|', '*', '-', '=', '+', '<', '>', ':',
    '(end)', '(unknown)', '(invalid)', '(name)', '(string)', '(float)', '(decimal)',
    '(integer)', '::', '{', '}',
))


class IdentityXPathParser(XPath2Parser):
    # noinspection PyTypeChecker
    symbol_table = {
        k: v for k, v in XPath2Parser.symbol_table.items()
        if k in XSD_IDENTITY_XPATH_SYMBOLS
    }
