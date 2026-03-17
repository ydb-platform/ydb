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
    from .xpath2_parser import XPath2Parser
else:
    from ._xpath2_constructors import XPath2Parser

__all__ = ['XPath2Parser']
