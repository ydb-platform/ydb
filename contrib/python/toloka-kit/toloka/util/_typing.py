__all__: list = []
import sys
from typing import Union, Dict, TypeVar, Tuple


if sys.version_info[:2] < (3, 8):

    def get_args(obj):
        return obj.__args__

    def get_origin(obj):
        return getattr(obj, '__origin__', None)

else:
    from typing import get_origin, get_args

if sys.version_info[:2] < (3, 9):
    from typing import _GenericAlias

    def is_generic(obj):
        return isinstance(obj, _GenericAlias)

else:
    from types import GenericAlias
    from typing import _GenericAlias

    def is_generic(obj):
        return isinstance(obj, _GenericAlias) or isinstance(obj, GenericAlias)


def is_optional_of(obj):
    if get_origin(obj) is Union:
        args = get_args(obj)
        if len(args) == 2 and args[1] is type(None):  # noqa
            return args[0]

    return None


def generate_type_var_mapping(cls) -> Tuple[type, Dict[str, type]]:
    type_var_mapping = {}

    if is_generic(cls):
        base = get_origin(cls)
        type_var_mapping = _generate_mapping(cls, type_var_mapping)
        cls = base
    for base in getattr(cls, "__orig_bases__", ()):
        if is_generic(base) and not str(base).startswith("typing.Generic"):
            type_var_mapping = _generate_mapping(base, type_var_mapping)
            break
    return cls, type_var_mapping


def _generate_mapping(
    cls, old_mapping: Dict[str, type]
) -> Dict[str, type]:
    mapping = {}
    for p, t in zip(get_origin(cls).__parameters__, get_args(cls)):
        if isinstance(t, TypeVar):
            continue
        mapping[p.__name__] = t

    if not mapping:
        return old_mapping

    return mapping
