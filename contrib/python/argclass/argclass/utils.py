"""Utility functions for argclass."""

import configparser
import os
import types
from pathlib import Path
from typing import (
    Any,
    Dict,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

from .exceptions import ComplexTypeError
from .types import (
    CONTAINER_TYPES,
    TEXT_TRUE_VALUES,
    NoneType,
    UnionClass,
)


def read_ini_configs(
    *paths: Union[str, Path],
    **kwargs: Any,
) -> Tuple[Mapping[str, Any], Tuple[Path, ...]]:
    """Read configuration from INI files."""
    kwargs.setdefault("allow_no_value", True)
    kwargs.setdefault("strict", False)
    parser = configparser.ConfigParser(**kwargs)

    filenames = []
    for path in paths:
        path_obj = Path(path).expanduser().resolve()
        # check the access first, because the parent
        # directory may not be readable
        if not os.access(path_obj, os.R_OK) or not path_obj.exists():
            continue
        filenames.append(path_obj)

    config_paths = parser.read(filenames)

    result: Dict[str, Union[str, Dict[str, str]]] = dict(
        parser.items(parser.default_section, raw=True),
    )

    for section in parser.sections():
        config = dict(parser.items(section, raw=True))
        result[section] = config

    return result, tuple(map(Path, config_paths))


def deep_getattr(name: str, attrs: Dict[str, Any], *bases: Type) -> Any:
    """Get attribute from attrs dict or base classes."""
    if name in attrs:
        return attrs[name]
    for base in bases:
        if hasattr(base, name):
            return getattr(base, name)
    raise KeyError(f"Key {name} was not declared")


def merge_annotations(
    annotations: Dict[str, Any],
    *bases: Type,
) -> Dict[str, Any]:
    """Merge annotations from base classes following MRO."""
    result: Dict[str, Any] = {}

    # Walk the full MRO to collect all inherited annotations
    for base in bases:
        for cls in reversed(base.__mro__):
            result.update(getattr(cls, "__annotations__", {}))
    result.update(annotations)
    return result


def parse_bool(value: str) -> bool:
    """Parse a string to boolean."""
    return value.lower() in TEXT_TRUE_VALUES


def _is_union_type(typespec: Any) -> bool:
    """Check if typespec is a Union type (typing.Union or PEP 604)."""
    if typespec.__class__ == UnionClass:
        return True
    # PEP 604: float | None creates types.UnionType in Python 3.10+
    return hasattr(types, "UnionType") and isinstance(typespec, types.UnionType)


def unwrap_optional(typespec: Any) -> Optional[Any]:
    """Unwrap Optional[T] to T, return None if not Optional."""
    if not _is_union_type(typespec):
        return None

    union_args = [a for a in typespec.__args__ if a is not NoneType]

    if len(union_args) != 1:
        raise ComplexTypeError(
            "Union types with multiple non-None members "
            "cannot be used directly",
            typespec=typespec,
            hint="Use argclass.Argument() with an explicit "
            "converter or type function",
        )

    return union_args[0]


def _is_container_type(typespec: Any) -> bool:
    """Check if typespec is a container type like list[str], List[str], etc."""
    origin = get_origin(typespec)
    if origin is None:
        return False
    # Handle typing.List, typing.Set, etc. and built-in list[str], set[int]
    return origin in CONTAINER_TYPES


def _unwrap_container_type(typespec: Any) -> Optional[Tuple[type, type]]:
    """
    Unwrap a container type and return (container_origin, element_type).

    For list[str] or List[str], returns (list, str).
    For set[int] or Set[int], returns (set, int).
    Returns None if not a container type.
    """
    if not _is_container_type(typespec):
        return None

    origin = get_origin(typespec)
    args = get_args(typespec)

    # We know origin is not None because _is_container_type checks this
    assert origin is not None

    if not args:
        # list without type parameter - use str as default
        return (origin, str)

    # For tuple, we handle specially - just use the first type for now
    # (full tuple handling would need nargs=N for Tuple[int, str, bool])
    element_type = args[0]

    # Handle nested optionals like list[str | None]
    optional_inner = unwrap_optional(element_type)
    if optional_inner is not None:
        element_type = optional_inner

    return (origin, element_type)


def unwrap_literal(typespec: Any) -> Optional[Tuple[type, Tuple[Any, ...]]]:
    """
    Unwrap Literal[value1, value2, ...] and return (value_type, choices).

    For Literal["a", "b", "c"], returns (str, ("a", "b", "c")).
    For Literal[1, 2, 3], returns (int, (1, 2, 3)).
    Returns None if not a Literal type.
    """
    origin = get_origin(typespec)
    if origin is not Literal:
        return None

    args = get_args(typespec)
    if not args:
        return None

    # Determine the common type of all literal values
    # All values should have the same type for argparse choices to work
    value_type = type(args[0])

    return value_type, args
