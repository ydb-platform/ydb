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
This module defines a class for handling Unicode subsets with less usage of memory.
"""
import sys
import warnings
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, MutableSet
from functools import wraps
from sys import maxunicode
from types import ModuleType
from typing import cast, Optional, Union
from unicodedata import unidata_version

from .codepoints import RegexError, CodePoint, code_point_order, \
    code_point_repr, iter_code_points, iterparse_character_subset
from . import unicode_blocks
from . import unicode_categories

__all__ = ['UnicodeSubset', 'UnicodeData', 'install_unicode_data', 'unicode_version',
           'lazy_subset', 'unicode_subset', 'unicode_category', 'unicode_block']

UNICODE_VERSIONS = (
    '16.0.0', '15.1.0', '15.0.0', '14.0.0', '13.0.0', '12.1.0', '12.0.0', '11.0.0',
    '10.0.0', '9.0.0', '8.0.0', '7.0.0', '6.3.0', '6.2.0', '6.1.0', '6.0.0',
    '5.2.0', '5.1.0', '5.0.0', '4.1.0', '4.0.1', '4.0.0', '3.2.0', '3.1.1',
    '3.1.0', '3.0.1', '3.0.0', '2.1.9', '2.1.8', '2.1.5', '2.1.2', '2.0.0'
)


CodePointsArgType = Union[None, str, 'UnicodeSubset', list[CodePoint], Iterable[CodePoint]]


class UnicodeSubset(MutableSet[CodePoint]):
    """
    Represents a subset of Unicode code points, implemented with an ordered list of
    integer values and ranges. Codepoints can be added or discarded using sequences
    of integer values and ranges or with strings equivalent to regex character set.

    :param codepoints: a sequence of integer values and ranges, another UnicodeSubset \
    instance ora a string equivalent of a regex character set.
    """
    __slots__ = '_codepoints',
    _codepoints: list[CodePoint]

    def __init__(self, codepoints: CodePointsArgType = None) -> None:
        if not codepoints:
            self._codepoints = list()
        elif isinstance(codepoints, list):
            self._codepoints = sorted(codepoints, key=code_point_order)
        elif isinstance(codepoints, UnicodeSubset):
            self._codepoints = codepoints._codepoints.copy()
        else:
            self._codepoints = list()
            self.update(codepoints)

    @property
    def codepoints(self) -> list[CodePoint]:
        return self._codepoints

    @codepoints.setter
    def codepoints(self, codepoints: Iterable[CodePoint]) -> None:
        self._codepoints = sorted(codepoints, key=code_point_order)

    def __repr__(self) -> str:
        return '%s(%r)' % (self.__class__.__name__, str(self))

    def __str__(self) -> str:
        return ''.join(code_point_repr(cp) for cp in self._codepoints)

    def copy(self) -> 'UnicodeSubset':
        return self.__copy__()

    def __copy__(self) -> 'UnicodeSubset':
        subset = self.__class__()
        subset._codepoints = self._codepoints.copy()
        return subset

    def __reversed__(self) -> Iterator[int]:
        for item in reversed(self._codepoints):
            if isinstance(item, int):
                yield item
            else:
                yield from reversed(range(item[0], item[1]))

    def complement(self) -> Iterator[CodePoint]:
        last_cp = 0
        for cp in self._codepoints:
            if isinstance(cp, int):
                cp0 = cp
                cp1 = cp + 1
            else:
                cp0, cp1 = cp

            diff = cp0 - last_cp
            if diff > 2:
                yield last_cp, cp0
            elif diff == 2:
                yield last_cp
                yield last_cp + 1
            elif diff == 1:
                yield last_cp
            elif diff:
                raise ValueError("unordered code points found in {!r}".format(self))
            last_cp = cp1

        if last_cp < maxunicode:
            yield last_cp, maxunicode + 1
        elif last_cp == maxunicode:
            yield maxunicode

    def iter_characters(self) -> Iterator[str]:
        return map(chr, self.__iter__())

    #
    # MutableSet's abstract methods implementation
    def __contains__(self, value: object) -> bool:
        if not isinstance(value, int):
            try:
                value = ord(value)  # type: ignore[arg-type]
            except TypeError:
                return False

        for cp in self._codepoints:
            if not isinstance(cp, int):
                if cp[0] > value:
                    return False
                elif cp[1] <= value:
                    continue
                else:
                    return True
            elif cp > value:
                return False
            elif cp == value:
                return True
        return False

    def __iter__(self) -> Iterator[int]:
        for cp in self._codepoints:
            if isinstance(cp, int):
                yield cp
            else:
                yield from range(*cp)

    def __len__(self) -> int:
        k = 0
        for _ in self:
            k += 1
        return k

    def update(self, *others: Union[str, Iterable[CodePoint]]) -> None:
        for value in others:
            if isinstance(value, str):
                for cp in iter_code_points(iterparse_character_subset(value), reverse=True):
                    self.add(cp)
            else:
                for cp in iter_code_points(value, reverse=True):
                    self.add(cp)

    def add(self, value: CodePoint) -> None:
        if isinstance(value, int):
            if 0 <= value <= maxunicode:
                start_cp = value
                end_cp = value + 1
            else:
                raise ValueError(f"{value!r} is not a Unicode code point value")

        elif 0 <= value[0] < value[1] <= maxunicode + 1:
            start_cp, end_cp = value
        else:
            raise ValueError(f"{value!r} is not a Unicode code point range")

        code_points = self._codepoints
        last_index = len(code_points) - 1
        for k, cp in enumerate(code_points):
            if isinstance(cp, int):
                cp0 = cp
                cp1 = cp + 1
            else:
                cp0, cp1 = cp

            if end_cp < cp0:
                code_points.insert(k, value)
            elif start_cp > cp1:
                continue
            elif end_cp > cp1:
                if k == last_index:
                    code_points[k] = min(cp0, start_cp), end_cp
                else:
                    next_cp = code_points[k + 1]
                    higher_bound = next_cp if isinstance(next_cp, int) else next_cp[0]
                    if end_cp <= higher_bound:
                        code_points[k] = min(cp0, start_cp), end_cp
                    else:
                        code_points[k] = min(cp0, start_cp), higher_bound
                        start_cp = higher_bound
                        continue
            elif start_cp < cp0:
                code_points[k] = start_cp, cp1
            break
        else:
            self._codepoints.append(value)

    def difference(self, other: 'UnicodeSubset') -> 'UnicodeSubset':
        subset = self.__copy__()
        subset.difference_update(other)
        return subset

    def difference_update(self, *others: Union[str, Iterable[CodePoint]]) -> None:
        for value in others:
            if isinstance(value, str):
                for cp in iter_code_points(iterparse_character_subset(value), reverse=True):
                    self.discard(cp)
            else:
                for cp in iter_code_points(value, reverse=True):
                    self.discard(cp)

    def discard(self, value: CodePoint) -> None:
        if isinstance(value, int):
            if 0 <= value <= maxunicode:
                start_cp = value
                end_cp = value + 1
            else:
                raise ValueError(f"{value!r} is not a Unicode code point value")

        elif 0 <= value[0] < value[1] <= maxunicode + 1:
            start_cp, end_cp = value
        else:
            raise ValueError(f"{value!r} is not a Unicode code point range")

        codepoints = self._codepoints
        for k in reversed(range(len(codepoints))):
            cp = codepoints[k]
            if isinstance(cp, int):
                cp0 = cp
                cp1 = cp + 1
            else:
                cp0, cp1 = cp

            if start_cp >= cp1:
                break
            elif end_cp >= cp1:
                if start_cp <= cp0:
                    del codepoints[k]
                elif start_cp - cp0 > 1:
                    codepoints[k] = cp0, start_cp
                else:
                    codepoints[k] = cp0
            elif end_cp > cp0:
                if start_cp <= cp0:
                    if cp1 - end_cp > 1:
                        codepoints[k] = end_cp, cp1
                    else:
                        codepoints[k] = cp1 - 1
                else:
                    if cp1 - end_cp > 1:
                        codepoints.insert(k + 1, (end_cp, cp1))
                    else:
                        codepoints.insert(k + 1, cp1 - 1)
                    if start_cp - cp0 > 1:
                        codepoints[k] = cp0, start_cp
                    else:
                        codepoints[k] = cp0

    #
    # MutableSet's mixin methods override
    def clear(self) -> None:
        del self._codepoints[:]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Iterable):
            return NotImplemented
        elif isinstance(other, UnicodeSubset):
            return self._codepoints == other._codepoints
        else:
            return self._codepoints == other

    def __ior__(self, other: object) -> 'UnicodeSubset':
        if not isinstance(other, Iterable):
            return NotImplemented
        elif isinstance(other, UnicodeSubset):
            other = reversed(other._codepoints)
        elif isinstance(other, str):
            other = reversed(UnicodeSubset(other)._codepoints)
        else:
            other = iter_code_points(other, reverse=True)

        for cp in other:
            self.add(cp)
        return self

    def __or__(self, other: object) -> 'UnicodeSubset':
        obj = self.__copy__()
        return obj.__ior__(other)

    def __isub__(self, other: object) -> 'UnicodeSubset':
        if not isinstance(other, Iterable):
            return NotImplemented
        elif isinstance(other, UnicodeSubset):
            other = reversed(other._codepoints)
        elif isinstance(other, str):
            other = reversed(UnicodeSubset(other)._codepoints)
        else:
            other = iter_code_points(other, reverse=True)

        for cp in other:
            self.discard(cp)
        return self

    def __sub__(self, other: object) -> 'UnicodeSubset':
        obj = self.__copy__()
        return obj.__isub__(other)

    __rsub__ = __sub__

    def __iand__(self, other: object) -> 'UnicodeSubset':
        if not isinstance(other, Iterable):
            return NotImplemented

        for value in (self - other):
            self.discard(value)
        return self

    def __and__(self, other: object) -> 'UnicodeSubset':
        obj = self.__copy__()
        return obj.__iand__(other)

    def __ixor__(self, other: object) -> 'UnicodeSubset':
        if other is self:
            self.clear()
            return self
        elif not isinstance(other, Iterable):
            return NotImplemented
        elif not isinstance(other, UnicodeSubset):
            other = UnicodeSubset(cast(Union[str, Iterable[CodePoint]], other))

        for value in other:
            if value in self:
                self.discard(value)
            else:
                self.add(value)
        return self

    def __xor__(self, other: object) -> 'UnicodeSubset':
        obj = self.__copy__()
        return obj.__ixor__(other)


def iterparse_unicode_data(url: str) -> Iterator[tuple[int, str]]:
    """Iterate UnicodeData.txt source giving back codepoints and categories."""
    from urllib.request import urlopen

    with urlopen(url) as res:
        prev_cp = -1

        for line in res.readlines():
            fields = line.split(b';')
            cp = int(fields[0], 16)
            cat = fields[2].decode('utf-8')

            if cp - prev_cp > 1:
                if fields[1].endswith(b', Last>'):
                    # Ranges of codepoints expressed with First and then Last
                    for x in range(prev_cp + 1, cp):
                        yield x, cat
                else:
                    # For default is 'Cn' that means 'Other, not assigned'
                    for x in range(prev_cp + 1, cp):
                        yield x, 'Cn'

            prev_cp = cp
            yield cp, cat

    while cp < maxunicode:
        cp += 1
        yield cp, 'Cn'


def get_categories_from_url(url: str) -> dict[str, UnicodeSubset]:
    categories: dict[str, list[CodePoint]] = defaultdict(list)

    major_category = 'C'
    major_start_cp, major_next_cp = 0, 1

    minor_category = 'Cc'
    minor_start_cp, minor_next_cp = 0, 1

    for cp, cat in iterparse_unicode_data(url):

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


def get_categories(version_info: tuple[int, ...], module: ModuleType) -> dict[str, UnicodeSubset]:
    categories = {k: v.copy() for k, v in module.UNICODE_CATEGORIES.items()}
    for name in module.__dict__:
        if not name.startswith('DIFF_CATEGORIES_VER_'):
            continue

        diff_version = name[20:].replace('_', '.')
        if version_info < tuple(int(x) for x in diff_version.split('.')):
            break

        for k, (exclude_cps, insert_cps) in getattr(unicode_categories, name).items():
            values = []
            additional = iter(insert_cps)
            cpa = next(additional, None)
            cpa_int = cpa[0] if isinstance(cpa, tuple) else cpa

            for cp in categories[k]:
                if cp in exclude_cps:
                    continue

                cp_int = cp[0] if isinstance(cp, tuple) else cp
                while cpa_int is not None and cpa_int <= cp_int:
                    values.append(cpa)
                    cpa = next(additional, None)
                    cpa_int = cpa[0] if isinstance(cpa, tuple) else cpa
                else:
                    values.append(cp)
            else:
                if cpa is not None:
                    values.append(cpa)
                    values.extend(additional)

            categories[k] = values

    return {k: UnicodeSubset(v) for k, v in categories.items()}


class UnicodeData:

    _blocks: dict[str, Union[str, UnicodeSubset]]

    @staticmethod
    def _unicode_block_key(name: str) -> str:
        return name.upper().replace(' ', '').replace('_', '').replace('-', '')

    def __init__(self, version: Optional[str] = None,
                 categories: Optional[dict[str, UnicodeSubset]] = None) -> None:
        if version is None:
            version = unidata_version
        elif version not in UNICODE_VERSIONS:
            raise ValueError("argument is not a valid Unicode version")

        self.version = version
        version_info = tuple(int(x) for x in version.split('.'))
        if categories is not None:
            self._categories = categories
        elif self.version in unicode_categories.UNICODE_VERSIONS:
            self._categories = get_categories(version_info, unicode_categories)
        else:
            from . import categories_fallback

            py_version = '.'.join(str(n) for n in sys.version_info[:2])
            msg = (f"unexpected Unicode version {version} for Python {py_version}, "
                   f"use unicodedata module for building categories map.")
            warnings.warn(UnicodeWarning(msg))

            self._categories = categories_fallback.get_unicodedata_categories()

        # Build blocks dict for version
        superseded_blocks = []
        blocks = unicode_blocks.UNICODE_BLOCKS_VER_2_0_0.copy()

        for name in unicode_blocks.__dict__:  # noqa
            if name.startswith('UPDATE_BLOCKS_VER_'):
                diff_version = name[18:].replace('_', '.')
                if version_info < tuple(int(x) for x in diff_version.split('.')):
                    break
                blocks.update(getattr(unicode_blocks, name))

            elif name.startswith('REMOVED_BLOCKS_VER_'):
                diff_version = name[19:].replace('_', '.')
                if version_info < tuple(int(x) for x in diff_version.split('.')):
                    break
                superseded_blocks.extend(getattr(unicode_blocks, name))

        # Following naming rules: https://www.w3.org/TR/xmlschema11-2/#cces-blockesc
        self._blocks = {k.replace(' ', '').replace('_', ''): v for k, v in blocks.items()}

        # Additional map for lookup using normalization for Unicode naming rules,
        # doesn't include superseded blocks.
        self._unicode_blocks = {
            k.upper().replace(' ', '').replace('_', '').replace('-', ''): k
            for k in blocks if k not in superseded_blocks
        }

    def category(self, name: str) -> UnicodeSubset:
        return self._categories[name]

    def block(self, name: str, normalize: bool = False) -> UnicodeSubset:
        if normalize:
            key = name.upper().replace(' ', '').replace('_', '').replace('-', '')
            try:
                name = self._unicode_blocks[key]
            except KeyError:
                if key != 'NOBLOCK':
                    raise
                name = key

        try:
            subset = self._blocks[name]
        except KeyError:
            if name != 'NoBlock':
                raise

            # Define the special block "No_Block", that contains all the other codepoints not
            # belonging to a defined block (https://www.unicode.org/Public/UNIDATA/Blocks.txt)
            no_block = UnicodeSubset([(0, maxunicode + 1)])
            for v in self._blocks.values():
                no_block -= v
            self._blocks['NoBlock'] = no_block
            self._unicode_blocks['NOBLOCK'] = 'NoBlock'
            return no_block

        else:
            if not isinstance(subset, UnicodeSubset):
                subset = self._blocks[name] = UnicodeSubset(subset)
            return subset


###
# Installed Unicode Data instance and accessors
__unicode_data = UnicodeData()

# Simple cache for Unicode subsets defined using callables with no-arguments, that
# can include subsets defined on versioned Unicode data or fixed codepoints. This
# cache is cleared if Unicode data is reinstalled.
__subsets_cache: dict[Callable[[], UnicodeSubset], UnicodeSubset] = {}


def install_unicode_data(version: Optional[str] = None,
                         name_or_url: Optional[str] = None) -> None:
    """
    Install a different version of UnicodeData. For default the package installs the version
    that matches `unicodedata.unidata_version`. Call without parameters to restore the
    default version.

    :param version: Unicode version to install. It's required if a *name_or_url* is provided.
    :param name_or_url: Import name of an additional module or a URL to raw UnicodeData.txt.
    """
    global __unicode_data

    if name_or_url is None:
        __unicode_data = UnicodeData(version)
    elif version is None:
        raise TypeError("you must specify a version to install")
    elif name_or_url.endswith('unicode_categories'):
        import importlib
        module = importlib.import_module(name_or_url)
        version_info = tuple(int(x) for x in version.split('.'))
        categories = get_categories(version_info, module)
        __unicode_data = UnicodeData(version, categories)
    else:
        categories = get_categories_from_url(name_or_url)
        __unicode_data = UnicodeData(version, categories)

    __subsets_cache.clear()


def unicode_version() -> str:
    """Returns the installed UnicodeData version."""
    return __unicode_data.version


def lazy_subset(func: Callable[[], UnicodeSubset]) -> Callable[[], UnicodeSubset]:
    """
    Defines a lazy UnicodeSubset wrapping its definition in a callable with no arguments.
    """
    @wraps(func)
    def wrapper() -> UnicodeSubset:
        try:
            return __subsets_cache[func]
        except KeyError:
            __subsets_cache[func] = func()
            return __subsets_cache[func]

    return wrapper


def unicode_subset(name: str) -> UnicodeSubset:
    """Retrieve a Unicode subset by name, raising a RegexError if it cannot be retrieved."""
    if name[:2] == 'Is':
        try:
            return __unicode_data.block(name[2:])
        except KeyError:
            raise RegexError(f"{name!r} doesn't match any Unicode block")
    else:
        try:
            return __unicode_data.category(name)
        except KeyError:
            raise RegexError(f"{name!r} doesn't match any Unicode category")


def unicode_category(name: str) -> UnicodeSubset:
    """
    Returns the Unicode Character Category subset addressed by the provided name, raising a
    KeyError if it's not found.
    """
    return __unicode_data.category(name)


def unicode_block(name: str, normalize: bool = False) -> UnicodeSubset:
    """
    Returns the Unicode block subset addressed by the provided name, raising a KeyError
    if it's not found. For default the lookup is done following the XSD naming rules for
    blocks and keeping superseded blocks (e.g. Greek), otherwise the name is normalized
    following the Unicode standard rules, without considering the casing, spaces, hyphens
    and underscores and the lookup is restricted to blocks defined on installed version.
    """
    return __unicode_data.block(name, normalize)
