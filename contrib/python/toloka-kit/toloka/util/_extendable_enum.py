__all__ = [
    'extend_enum',
    'ExtendableStrEnumMetaclass',
    'ExtendableStrEnum',
]

from enum import Enum, EnumMeta, _is_dunder
from logging import getLogger

logger = getLogger(__name__)


def extend_enum(enumeration: Enum, name: str, value: str):
    # inspired by https://github.com/ethanfurman/aenum
    member_names = enumeration._member_names_
    member_map = enumeration._member_map_
    value2member_map = enumeration._value2member_map_

    if name in member_names:
        logger.warning(f'Trying to extend {enumeration} enum with an existing name: {name}')
        return member_map[name]

    # TODO: support a case when an enumeration is not wrapped by @unique
    if value in value2member_map:
        logger.warning(f'Trying to extend {enumeration} enum with an existing value: {value}')
        return value2member_map[value]

    new = getattr(enumeration, '__new_member__', enumeration._member_type_.__new__)
    new_member = new(enumeration)
    new_member._name_ = name
    new_member._value_ = value
    member_names.append(name)
    member_map[name] = new_member
    value2member_map[value] = new_member

    return new_member


class ExtendableStrEnumMetaclass(EnumMeta):
    def __getattr__(cls, name):
        if _is_dunder(name) or name.startswith('_'):
            raise AttributeError(name)
        try:
            return cls._member_map_[name]
        except KeyError:
            return extend_enum(cls, name, name)

    def __getitem__(cls, name):
        try:
            return cls._member_map_[name]
        except KeyError as e:
            if _is_dunder(name) or name.startswith('_'):
                raise e
            return extend_enum(cls, name, name)

    def __call__(cls, value, names=None, *, module=None, qualname=None, type=None, start=1):
        if names is None:  # simple value lookup
            try:
                return cls.__new__(cls, value)
            except ValueError:
                # Modify original __call__
                # We use value as name
                return cls.__getattr__(value)
        # otherwise, functional API: we're creating a new Enum type
        return cls._create_(value, names, module=module, qualname=qualname, type=type, start=start)


class ExtendableStrEnum(Enum, metaclass=ExtendableStrEnumMetaclass):
    pass
