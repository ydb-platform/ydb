#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
This package defines a proxy class and a mixin class for enabling XPath on schemas,
and custom parser for identities and assertions.
"""
from .proxy import XMLSchemaProxy
from .mixin import ElementPathMixin, XPathElement
from .assertion_parser import XsdAssertionXPathParser
from .identity_parser import IdentityXPathParser
from .selectors import split_path, ElementSelector, ElementPathSelector

__all__ = ['XMLSchemaProxy', 'ElementPathMixin', 'XPathElement',
           'XsdAssertionXPathParser', 'IdentityXPathParser',
           'split_path', 'ElementSelector', 'ElementPathSelector']
