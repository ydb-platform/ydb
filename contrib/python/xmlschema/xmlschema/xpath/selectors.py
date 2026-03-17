#
# Copyright (c), 2025-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from functools import cached_property
from typing import cast, Optional, TYPE_CHECKING, Union
from xml.etree.ElementTree import Element

from elementpath import XPath2Parser, XPathToken, ElementNode, XPathContext

from xmlschema.aliases import ElementType, NsmapType
from xmlschema.exceptions import XMLSchemaTypeError

if TYPE_CHECKING:
    from xmlschema.resources import XMLResource

CacheKeyType = Union[
    tuple[str, type['ElementSelector']],
    tuple[Union[str, type['ElementSelector'], Iterable[tuple[str, str]], tuple[str, str]], ...]
]

_selectors_cache: dict[CacheKeyType, 'ElementSelector'] = {}
_dummy_element = Element('dummy')


def is_ncname(s: str) -> bool:
    return s.isalpha() and ':' not in s and all(is_ncname_continuation(c) for c in s[1:])


def is_ncname_continuation(c: str) -> bool:
    return (c.isalnum() or c in '-.\u00B7\u0387\u06DD\u06DE\u203F\u2040'
            or 0x300 <= ord(c) <= 0x36F)


def split_path(path: str, namespaces: Optional[NsmapType] = None,
               extended_names: bool = False) -> deque[str]:
    """
    Splits a path expression to a sequence of chunks that put in evidence path steps,
    predicates and other parts that can be useful for checking some properties of the
    provided path, like the path depth of if the path is composed only by path steps
    and wildcards.

    :param path: the path expression to split.
    :param namespaces: an optional namespace mapping to use on prefixed names.
    :param extended_names: if `True` maps prefixed names to extended form. For \
    default only the default namespace is used, if defined and not empty.
    """
    start = end = 0

    def flush() -> None:
        nonlocal start
        if start < end:
            chunks.append(path[start:end])
            start = end

    def advance(condition: Callable[[str], bool]) -> None:
        nonlocal end
        end += 1
        while condition(path[end]):
            end += 1

    path = path.replace(' ', '').replace('\t', '').replace('./', '')  # path normalization
    chunks: deque[str] = deque([''])  # add an empty element to avoid index errors
    default_namespace = None if not namespaces else namespaces.get('')

    while True:
        try:
            flush()
            if path[end] in '"\'':
                advance(lambda x: x != path[start])
                end += 1
            elif path[end] == '{':
                advance(lambda x: x != '}')
                end += 1
                if path[end].isalpha():
                    advance(is_ncname_continuation)
            elif path[end].isalpha():
                advance(is_ncname_continuation)
                if path[end] == ':':
                    prefix = path[start:end]
                    end += 1
                    if path[end].isalpha():
                        advance(is_ncname_continuation)
                        if extended_names and namespaces and prefix in namespaces:
                            flush()
                            uri = namespaces[prefix]
                            chunks[-1] = f'{{{uri}}}{chunks[-1][len(prefix)+1:]}'

                elif default_namespace and not chunks[-1].endswith('@'):
                    flush()
                    chunks[-1] = f'{{{default_namespace}}}{chunks[-1]}'
            elif path[end] == '/':
                advance(lambda x: x == '/')
            else:
                end += 1

        except IndexError:
            if start < len(path):
                flush()
                if default_namespace and is_ncname(chunks[-1]) and chunks[-2] != '@':
                    chunks[-1] = f'{{{default_namespace}}}{chunks[-1]}'

            chunks.popleft()
            return chunks


class ElementSelector:
    """
    An XPath selector for selecting ElementTree elements. Raises an error
    if the path parse fails or is incompatible with the selector type.

    :param path: the XPath expression.
    :param namespaces: an optional namespace mapping.
    """

    path: str
    """The normalized XPath expression of the path provided by argument."""

    namespaces: Optional[dict[str, str]]
    """The namespaces mapping associated with the XPath expression path."""

    _parser: XPath2Parser
    _token: XPathToken

    @classmethod
    def cached_selector(cls, path: str, namespaces: Optional[NsmapType] = None) \
            -> 'ElementSelector':
        """A builder of ElementSelector instances based on a cache."""
        key: CacheKeyType = (path, cls)
        if namespaces is not None:
            key += tuple(sorted(namespaces.items()))

        try:
            return _selectors_cache[key]
        except KeyError:
            if len(_selectors_cache) > 100:
                _selectors_cache.clear()

            selector = cls(path, namespaces)
            _selectors_cache[key] = selector
            return selector

    def __init__(self, path: str, namespaces: Optional[NsmapType] = None) -> None:
        self.namespaces = None if namespaces is None else {k: v for k, v in namespaces.items()}
        self._parts = split_path(path, namespaces)

        self.path = ''.join(self._parts)

        self._parser = XPath2Parser(namespaces, strict=False)
        self._token = self._parser.parse(self.path)
        self.select(_dummy_element)

    def __repr__(self) -> str:
        return '%s(path=%r, )' % (self.__class__.__name__, self.path)

    @property
    def parts(self) -> list[str]:
        """Return a list with the parts of the parsed path."""
        return list(self._parts)

    @cached_property
    def relative_path(self) -> str:
        """The equivalent path expression relative to root element."""
        parts = self._parts.copy()
        if not parts:
            parts.appendleft('.')
        elif parts[0] == '//':
            parts.appendleft('.')
        elif parts[0] == '/':
            parts.popleft()
            while parts:
                if parts[0].startswith('/'):
                    break
                parts.popleft()
            parts.appendleft('.')
        return ''.join(parts)

    @cached_property
    def select_all(self) -> bool:
        """Returns `True` if the path is composed only by wildcards or path steps."""
        return all(c in ('*', '/', '.') for c in self._parts)

    @cached_property
    def depth(self) -> int:
        """Path depth, 0 means a self axis selector, -1 means an unlimited depth."""
        if not self._parts:
            return 0
        elif '//' in self._parts:
            return -1
        elif self._parts[0] == '/':
            return sum(s == '/' for s in self._parts) - 1
        elif self._parts[0] == '.':
            return sum(s == '/' for s in self._parts)
        else:
            return sum(s == '/' for s in self._parts) + 1

    def select(self, root: Union[ElementType, 'XMLResource']) -> list[ElementType]:
        return list(self.iter_select(root))

    def iter_select(self, root: Union[ElementType, 'XMLResource']) -> Iterator[Element]:
        if hasattr(root, 'xpath_root'):
            context = XPathContext(root.xpath_root)
        else:
            context = XPathContext(root)

        for item in self._token.select(context):
            if not isinstance(item, ElementNode):  # pragma: no cover
                msg = "XPath expressions on XML resources can select only elements"
                raise XMLSchemaTypeError(msg)
            yield cast(ElementType, item.obj)


class ElementPathSelector(ElementSelector):
    """
    An XPath selector that uses `xml.etree.ElementPath.iterfind()` for selecting elements.
    """
    def iter_select(self, root: Union[ElementType, 'XMLResource']) -> Iterator[ElementType]:
        if hasattr(root, 'root'):
            yield from root.root.iterfind(self.relative_path, self.namespaces)
        else:
            yield from root.iterfind(self.relative_path, self.namespaces)
