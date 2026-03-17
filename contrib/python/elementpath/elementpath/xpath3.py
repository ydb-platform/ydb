#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from .xpath30 import XPath30Parser
from .xpath31 import XPath31Parser

XPath3Parser = XPath31Parser

__all__ = ['XPath30Parser', 'XPath31Parser', 'XPath3Parser']
