#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from abc import abstractmethod
from typing import Union, Any
import codecs

from elementpath.aliases import XPath2ParserType
from elementpath.helpers import LazyPattern, collapse_white_spaces
from .any_types import AnyAtomicType
from .untyped import UntypedAtomic

__all__ = ['AbstractBinary', 'Base64Binary', 'HexBinary']


class AbstractBinary(AnyAtomicType):
    """
    Abstract base class for xs:base64Binary/xs:hexBinary datatypes.
    """
    value: bytes

    __slots__ = ('value', 'ordered')

    @classmethod
    def make(cls, value: Any,
             parser: XPath2ParserType | None = None,
             **kwargs: Any) -> 'AbstractBinary':
        ordered = parser is not None and parser.version >= '3.1'
        return cls(value, ordered)

    @property
    def xpath_versions(self) -> tuple[str, ...] | str:
        return '3.1+' if self.ordered else '2.0', '3.0'

    def __init__(self, value: Union[str, bytes, UntypedAtomic, 'AbstractBinary'],
                 ordered: bool = False) -> None:
        """
        :param value: a string or a binary data or an untyped atomic instance.
        :param ordered: a boolean that enable total ordering for the instance, `False` for default.
        """
        self.ordered = ordered

        match value:
            case x if isinstance(x, self.__class__):
                self.value = value.value
                return
            case AbstractBinary():
                self.value = self.encoder(value.decode())
                return
            case UntypedAtomic():
                value = collapse_white_spaces(value.value)
            case str():
                value = collapse_white_spaces(value)
            case bytes():
                value = collapse_white_spaces(value.decode('utf-8'))
            case _:
                raise self._invalid_type(value)  # noqa

        self.validate(value)
        self.value = value.replace(' ', '').encode('ascii')

    def __repr__(self) -> str:
        return '%s(%r)' % (self.__class__.__name__, self.value)

    def __bytes__(self) -> bytes:
        return self.value

    @classmethod
    def validate(cls, value: object) -> None:
        raise NotImplementedError()

    @classmethod
    def encoder(cls, value: bytes) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def decode(self) -> bytes:
        raise NotImplementedError()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AbstractBinary):
            return self.decode() == other.decode()
        else:
            return NotImplemented

    def __lt__(self, other: object) -> bool:
        if not self.ordered or not isinstance(other, AbstractBinary):
            return NotImplemented

        for oct1, oct2 in zip(self.decode(), other.decode()):
            if oct1 != oct2:
                return oct1 < oct2

        return len(self.decode()) < len(other.decode())

    def __le__(self, other: object) -> bool:
        if not self.ordered or not isinstance(other, AbstractBinary):
            return NotImplemented

        for oct1, oct2 in zip(self.decode(), other.decode()):
            if oct1 != oct2:
                return oct1 < oct2

        return len(self.decode()) <= len(other.decode())

    def __gt__(self, other: object) -> bool:
        if not self.ordered or not isinstance(other, AbstractBinary):
            return NotImplemented

        for oct1, oct2 in zip(self.decode(), other.decode()):
            if oct1 != oct2:
                return oct1 > oct2

        return len(self.decode()) > len(other.decode())

    def __ge__(self, other: object) -> bool:
        if not self.ordered or not isinstance(other, AbstractBinary):
            return NotImplemented

        for oct1, oct2 in zip(self.decode(), other.decode()):
            if oct1 != oct2:
                return oct1 > oct2

        return len(self.decode()) >= len(other.decode())


class Base64Binary(AbstractBinary):
    name = 'base64Binary'
    pattern = LazyPattern(
        r'((?:(?:[A-Za-z0-9+/] ?){4})*(?:(?:[A-Za-z0-9+/] ?){3}[A-Za-z0-9+/]|'
        r'(?:[A-Za-z0-9+/] ?){2}'
        r'[AEIMQUYcgkosw048] ?=|[A-Za-z0-9+/] ?[AQgw] ?= ?=))?'
    )

    @classmethod
    def validate(cls, value: object) -> None:
        if isinstance(value, cls):
            return
        elif isinstance(value, bytes):
            value = value.decode()
        elif not isinstance(value, str):
            raise cls._invalid_type(value)

        value = value.replace(' ', '')
        if value:
            match = cls.pattern.match(value)
            if match is None or match.group(0) != value:
                raise cls._invalid_value(value)

    def __str__(self) -> str:
        return self.value.decode('utf-8')

    def __hash__(self) -> int:
        return hash(self.value)

    def __len__(self) -> int:
        length = len(self.value)
        if length == 0:
            return 0
        elif self.value[-2] == ord('='):
            return length // 4 * 3 - 2
        elif self.value[-1] == ord('='):
            return length // 4 * 3 - 1
        return length // 4 * 3

    @classmethod
    def encoder(cls, value: bytes) -> bytes:
        return codecs.encode(value, 'base64').rstrip(b'\n')

    def decode(self) -> bytes:
        return codecs.decode(self.value, 'base64')


class HexBinary(AbstractBinary):
    name = 'hexBinary'
    pattern = LazyPattern(r'^([0-9a-fA-F]{2})*$')

    @classmethod
    def validate(cls, value: object) -> None:
        if isinstance(value, cls):
            return
        elif isinstance(value, bytes):
            value = value.decode()
        elif not isinstance(value, str):
            raise cls._invalid_type(value)

        value = value.strip()
        if cls.pattern.match(value) is None:
            raise cls._invalid_value(value)

    @classmethod
    def encoder(cls, value: bytes) -> bytes:
        return codecs.encode(value, 'hex')

    def decode(self) -> bytes:
        return codecs.decode(self.value, 'hex')

    def __str__(self) -> str:
        return self.value.decode('utf-8').upper()

    def __hash__(self) -> int:
        return hash(self.value.upper())

    def __len__(self) -> int:
        return len(self.value) // 2
