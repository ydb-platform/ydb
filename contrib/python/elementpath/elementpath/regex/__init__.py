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
Subpackage for processing XML regular expressions and for converting them to
Python-compatible regexps.

XPath/XQuery/XML-Schema regexp flavors are supported through translate_pattern()
API options. Default options process XPath/XQuery patterns.
"""
from .codepoints import RegexError, iter_code_points
from .unicode_subsets import UnicodeSubset, UnicodeData, install_unicode_data, \
    unicode_version, unicode_subset, lazy_subset, unicode_category, unicode_block
from .character_classes import CharacterClass
from .patterns import translate_pattern

__all__ = ['translate_pattern', 'RegexError', 'UnicodeSubset', 'UnicodeData',
           'install_unicode_data', 'unicode_version', 'unicode_subset', 'lazy_subset',
           'unicode_category', 'unicode_block', 'CharacterClass', 'iter_code_points']
