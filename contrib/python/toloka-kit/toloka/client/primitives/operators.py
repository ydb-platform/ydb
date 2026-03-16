__all__ = [
    'CompareOperator',
    'InclusionOperator',
    'IdentityOperator',
    'InclusionConditionMixin',
    'IdentityConditionMixin',
    'ComparableConditionMixin',
    'StatefulComparableConditionMixin'
]

from enum import unique, Enum
from typing import Type

import attr
import sys

from .base import BaseTolokaObjectMetaclass
from ...util._codegen import attribute
from ...util._extendable_enum import ExtendableStrEnum


@unique
class CompareOperator(ExtendableStrEnum):
    """Supports comparison operators:
        * `==`
        * `!=`
        * `>`
        * `>=`
        * `<`
        * `<=`
    """
    EQ = 'EQ'
    NE = 'NE'
    GT = 'GT'
    GTE = 'GTE'
    LT = 'LT'
    LTE = 'LTE'

    def __invert__(self):
        if self == CompareOperator.EQ:
            return CompareOperator.NE
        if self == CompareOperator.NE:
            return CompareOperator.EQ
        if self == CompareOperator.GT:
            return CompareOperator.LTE
        if self == CompareOperator.GTE:
            return CompareOperator.LT
        if self == CompareOperator.LT:
            return CompareOperator.GTE
        if self == CompareOperator.LTE:
            return CompareOperator.GT


@unique
class InclusionOperator(ExtendableStrEnum):
    """Supports the `include()` and `exclude()` methods in filter conditions.
    """
    IN = 'IN'
    NOT_IN = 'NOT_IN'

    def __invert__(self):
        if self == InclusionOperator.IN:
            return InclusionOperator.NOT_IN
        if self == InclusionOperator.NOT_IN:
            return InclusionOperator.IN


@unique
class IdentityOperator(ExtendableStrEnum):
    """Supports the `==` and `!=` operators.
    """
    EQ = 'EQ'
    NE = 'NE'

    def __invert__(self):
        if self == IdentityOperator.EQ:
            return IdentityOperator.NE
        if self == IdentityOperator.NE:
            return IdentityOperator.EQ


def _create_operator_metaclass_new(operator_enum: Type[Enum]):
    def __new__(mcs, name, bases, namespace, kw_only=False, **kwargs):
        annotations = namespace.get('__annotations__', {})
        namespace['__annotations__'] = dict(operator=operator_enum, **annotations)
        namespace = dict(operator=attribute(required=True), **namespace)
        return super(mcs, mcs).__new__(mcs, name, bases, namespace, kw_only=kw_only, **kwargs)

    return __new__


def _eq_compatible_with_help(cls, value):
    # help(toloka.conditions) does not work if we use our custom __eq__ logic here
    # so we need to check that __eq__ was called from help() and use standard logic if true
    is_called_from_help = False
    frame = sys._getframe()
    while frame:
        if frame.f_code.co_filename.endswith('pydoc.py') and frame.f_code.co_name == 'help':
            is_called_from_help = True
            break
        frame = frame.f_back

    return value is cls if isinstance(value, type) or is_called_from_help else cls.eq(value)


class _InclusionConditionMetaclass(BaseTolokaObjectMetaclass):

    def include(cls, *args, **kwargs):
        return cls(InclusionOperator.IN, *args, **kwargs)

    def exclude(cls, *args, **kwargs):
        return cls(InclusionOperator.NOT_IN, *args, **kwargs)

    in_ = include
    not_in = exclude

    __new__ = _create_operator_metaclass_new(InclusionOperator)


class _IdentityConditionMetaclass(BaseTolokaObjectMetaclass):

    def eq(cls, value):
        return cls(operator=IdentityOperator.EQ, value=value)

    def ne(cls, value):
        return cls(operator=IdentityOperator.NE, value=value)

    def __hash__(cls):
        return super().__hash__()

    def __ne__(cls, value):
        return value is not cls if isinstance(value, type) else cls.ne(value)

    __eq__ = _eq_compatible_with_help
    __new__ = _create_operator_metaclass_new(IdentityOperator)


class _ComparableConditionMetaclass(BaseTolokaObjectMetaclass):

    def lt(cls, value):
        return cls(operator=CompareOperator.LT, value=value)

    def lte(cls, value):
        return cls(operator=CompareOperator.LTE, value=value)

    def gt(cls, value):
        return cls(operator=CompareOperator.GT, value=value)

    def gte(cls, value):
        return cls(operator=CompareOperator.GTE, value=value)

    def eq(cls, value):
        return cls(operator=CompareOperator.EQ, value=value)

    def ne(cls, value):
        return cls(operator=CompareOperator.NE, value=value)

    def __hash__(cls):
        return super().__hash__()

    def __ne__(cls, value):
        return value is not cls if isinstance(value, type) else cls.ne(value)

    __lt__ = lt
    __le__ = lte
    __gt__ = gt
    __ge__ = gte
    __eq__ = _eq_compatible_with_help
    __new__ = _create_operator_metaclass_new(CompareOperator)


class InclusionConditionMixin(metaclass=_InclusionConditionMetaclass):
    pass


class IdentityConditionMixin(metaclass=_IdentityConditionMetaclass):
    pass


class ComparableConditionMixin(metaclass=_ComparableConditionMetaclass):
    pass


class StatefulComparableConditionMixin:

    def lt(self, value):
        return attr.evolve(self, operator=CompareOperator.LT, value=value)

    def lte(self, value):
        return attr.evolve(self, operator=CompareOperator.LTE, value=value)

    def gt(self, value):
        return attr.evolve(self, operator=CompareOperator.GT, value=value)

    def gte(self, value):
        return attr.evolve(self, operator=CompareOperator.GTE, value=value)

    def eq(self, value):
        return attr.evolve(self, operator=CompareOperator.EQ, value=value)

    def ne(self, value):
        return attr.evolve(self, operator=CompareOperator.NE, value=value)

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, value):
        if value.__class__ is not self.__class__:
            return self.eq(value)
        return attr.astuple(self, recurse=False) == attr.astuple(value, recurse=False)

    def __ne__(self, value):
        if value.__class__ is not self.__class__:
            return self.ne(value)
        return attr.astuple(self, recursive=False) != attr.astuple(value, recursive=False)

    __lt__ = lt
    __le__ = lte
    __gt__ = gt
    __ge__ = gte
