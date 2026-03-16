import typing
from dataclasses import InitVar
from typing import Annotated, ClassVar, Final, TypeVar

from ..feature_requirement import HAS_PY_313, HAS_TYPED_DICT_REQUIRED
from .normalize_type import BaseNormType

_TYPE_TAGS = [Final, ClassVar, InitVar, Annotated]

if HAS_TYPED_DICT_REQUIRED:
    _TYPE_TAGS.extend([typing.Required, typing.NotRequired])
if HAS_PY_313:
    _TYPE_TAGS.extend([typing.ReadOnly, typing.TypeIs])  # type: ignore[attr-defined]


def strip_tags(norm: BaseNormType) -> BaseNormType:
    """Removes type hints that do not represent a type
    and that only indicates metadata
    """
    if norm.origin in _TYPE_TAGS:
        return strip_tags(norm.args[0])
    return norm


N = TypeVar("N", bound=BaseNormType)


def is_class_var(norm: BaseNormType) -> bool:
    if norm.origin == ClassVar:
        return True
    if norm.origin in _TYPE_TAGS:
        return is_class_var(norm.args[0])
    return False
