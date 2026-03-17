#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from typing import Any, Optional, cast, TypeVar

from elementpath.aliases import XPathParserType
from elementpath.helpers import LazyPattern
from .any_types import AnyAtomicType
from .untyped import UntypedAtomic

__all__ = ['AbstractQName', 'QName', 'Notation']

T = TypeVar('T', bound='AbstractQName')


class AbstractQName(AnyAtomicType):
    """
    XPath compliant QName, bound with a prefix and a namespace.
    """
    pattern = LazyPattern(
        r'^(?:(?P<prefix>[^\d\W][\w\-.\u00B7\u0300-\u036F\u0387\u06DD\u06DE\u203F\u2040]*):)?'
        r'(?P<local>[^\d\W][\w\-.\u00B7\u0300-\u036F\u0387\u06DD\u06DE\u203F\u2040]*)$',
    )

    @classmethod
    def make(cls: type[T], value: Any,
             namespaces: dict[str, str] | None = None,
             parser: XPathParserType | None = None,
             **kwargs: Any) -> T:

        if isinstance(value, AbstractQName):
            return cast(T, value)
        elif isinstance(value, UntypedAtomic) and parser and parser.version >= '3.0':
            value = value.value
        elif not isinstance(value, str):
            raise cls._invalid_type(value)

        if namespaces is None:
            namespaces = parser.namespaces if parser is not None else {}

        if ':' not in value:
            return cls(namespaces.get(''), value)
        else:
            return cls(namespaces[value[0:value.index(':')]], value)

    __slots__ = ('uri', 'qname', 'prefix', 'local_name')

    def __new__(cls, *args: Any, **kwargs: Any) -> 'AbstractQName':
        if cls.__name__ == 'Notation':
            raise TypeError("can't instantiate xs:NOTATION objects")
        return super().__new__(cls)

    def __init__(self, uri: Optional[str], qname: str) -> None:
        """
        :param uri: the bound namespace URI, must be a not empty \
        URI if a prefixed name is provided for the 2nd argument.
        :param qname: a prefixed name or a local name.
        """
        if uri is None:
            self.uri = ''
        elif isinstance(uri, str):
            self.uri = uri
        else:
            raise TypeError('the 1st argument has an invalid type %r' % type(uri))

        if not isinstance(qname, str):
            raise TypeError('the 2nd argument has an invalid type %r' % type(qname))
        self.qname = qname.strip()

        match = self.pattern.match(self.qname)
        if match is None:
            raise ValueError('invalid value {!r} for an xs:QName'.format(self.qname))

        self.prefix = match.groupdict()['prefix']
        self.local_name = match.groupdict()['local']
        if not uri and self.prefix:
            msg = '{!r}: cannot associate a non-empty prefix with no namespace'
            raise ValueError(msg.format(self))

    @property
    def namespace(self) -> str:
        return self.uri

    @namespace.setter
    def namespace(self, uri: str) -> None:
        self.uri = uri

    @property
    def expanded_name(self) -> str:
        return '{%s}%s' % (self.uri, self.local_name) if self.uri else self.local_name

    @property
    def braced_uri_name(self) -> str:
        return 'Q{%s}%s' % (self.uri, self.local_name)

    def __repr__(self) -> str:
        return '%s(uri=%r, qname=%r)' % (self.__class__.__name__, self.uri, self.qname)

    def __str__(self) -> str:
        return self.qname

    def __hash__(self) -> int:
        return hash((self.uri, self.local_name))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AbstractQName):
            return self.uri == other.uri and self.local_name == other.local_name
        elif isinstance(other, (str, UntypedAtomic)):
            return other == self.qname
        return NotImplemented


class QName(AbstractQName):
    name = 'QName'


class Notation(AbstractQName):
    name = 'NOTATION'
