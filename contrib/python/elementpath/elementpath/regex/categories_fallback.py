#
# Copyright (c), 2025-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""Fallback module for generating Unicode categories if version doesn't match."""
from sys import maxunicode
from typing import Union
from unicodedata import category

from .unicode_subsets import UnicodeSubset

UNICODE_CATEGORIES = (
    'C', 'Cc', 'Cf', 'Cs', 'Co', 'Cn',
    'L', 'Lu', 'Ll', 'Lt', 'Lm', 'Lo',
    'M', 'Mn', 'Mc', 'Me',
    'N', 'Nd', 'Nl', 'No',
    'P', 'Pc', 'Pd', 'Ps', 'Pe', 'Pi', 'Pf', 'Po',
    'S', 'Sm', 'Sc', 'Sk', 'So',
    'Z', 'Zs', 'Zl', 'Zp'
)


def get_unicodedata_categories() -> dict[str, UnicodeSubset]:
    """
    Extracts Unicode categories information from unicodedata library or from normative
    raw data. Each category is represented with an ordered list containing code points
    and code point ranges.

    :return: a dictionary with category names as keys and lists as values.
    """
    categories: dict[str, list[Union[int, tuple[int, int]]]]
    categories = {k: [] for k in UNICODE_CATEGORIES}

    major_category = 'C'
    major_start_cp, major_next_cp = 0, 1

    minor_category = 'Cc'
    minor_start_cp, minor_next_cp = 0, 1

    for cp in range(maxunicode + 1):
        cat = category(chr(cp))
        if cat[0] != major_category:
            if cp > major_next_cp:
                categories[major_category].append((major_start_cp, cp))
            else:
                categories[major_category].append(major_start_cp)

            major_category = cat[0]
            major_start_cp, major_next_cp = cp, cp + 1

        if cat != minor_category:
            if cp > minor_next_cp:
                categories[minor_category].append((minor_start_cp, cp))
            else:
                categories[minor_category].append(minor_start_cp)

            minor_category = cat
            minor_start_cp, minor_next_cp = cp, cp + 1

    else:
        if major_next_cp == maxunicode + 1:
            categories[major_category].append(major_start_cp)
        else:
            categories[major_category].append((major_start_cp, maxunicode + 1))

        if minor_next_cp == maxunicode + 1:
            categories[minor_category].append(minor_start_cp)
        else:
            categories[minor_category].append((minor_start_cp, maxunicode + 1))

    return {k: UnicodeSubset(v) for k, v in categories.items()}
