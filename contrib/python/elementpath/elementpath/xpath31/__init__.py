#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .xpath31_parser import XPath31Parser
else:
    from ._xpath31_functions import XPath31Parser

__all__ = ['XPath31Parser']
