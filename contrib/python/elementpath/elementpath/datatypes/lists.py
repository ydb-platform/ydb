#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections.abc import Sequence
from types import MappingProxyType
from typing import Any, overload, TypeVar

from .any_types import AtomicTypeMeta, AnySimpleType, AnyAtomicType
from .untyped import UntypedAtomic
from .string import NMToken, Idref, Entity

__all__ = ['builtin_list_types', 'ListType', 'NMTokens', 'Idrefs', 'Entities']


_builtin_list_types: 'dict[str, AtomicTypeMeta]' = {}
builtin_list_types = MappingProxyType(_builtin_list_types)
"""Registry of builtin list types by expanded name."""

T = TypeVar('T', bound=AnyAtomicType)


class ListTypeMeta(AtomicTypeMeta):
    types_map = _builtin_list_types


class ListType(Sequence[AnyAtomicType], AnySimpleType, metaclass=ListTypeMeta):
    value: list[AnyAtomicType]
    item_type: type[AnyAtomicType]

    __slots__ = ('value',)

    def __init__(self, value: list[AnyAtomicType]) -> None:
        self.value = value

    @overload
    def __getitem__(self, index: int) -> AnyAtomicType: ...

    @overload
    def __getitem__(self, index: slice) -> list[AnyAtomicType]: ...

    def __getitem__(self, index: int | slice) -> AnyAtomicType | list[AnyAtomicType]:
        return self.value[index]

    def __len__(self) -> int:
        return len(self.value)

    @classmethod
    def validate(cls, obj: object) -> None:
        if isinstance(obj, cls):
            return
        elif not isinstance(obj, list) or \
                any(not isinstance(item, AnyAtomicType) for item in obj):
            raise cls._invalid_type(obj)
        else:
            for item in obj:
                cls.item_type.validate(item)

    @classmethod
    def decode(cls, value: Any) -> list[AnyAtomicType]:
        if isinstance(value, UntypedAtomic):
            values = value.value.split() or ['']
        elif isinstance(value, str):
            values = value.split() or ['']
        elif isinstance(value, list):
            values = value
        else:
            raise cls._invalid_type(value)

        try:
            return [cls.item_type(x) for x in values]
        except ValueError:
            raise cls._invalid_value(value)


###
# Builtin list types

class NMTokens(ListType):
    name = 'NMTOKENS'
    items: list[NMToken]
    item_type: type[NMToken] = NMToken


class Idrefs(ListType):
    name = 'IDREFS'
    items: list[Idref]
    item_type: type[Idref] = Idref


class Entities(ListType):
    name = 'ENTITIES'
    items: list[Entity]
    item_type: type[Entity] = Entity
