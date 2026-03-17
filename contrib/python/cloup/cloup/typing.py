__all__ = ['AnyCallable', 'MISSING', 'Possibly',  'Decorator', 'F']

from enum import Enum
from typing import Any, Callable, TypeVar, Union


# PEP-blessed solution for defining a Singleton type:
# https://peps.python.org/pep-0614/#motivation
class _Missing(Enum):
    flag = 'Missing'


MISSING = _Missing.flag
"""Singleton that works as a sentinel for a missing value.
Useful when None can't be used to play the role because it represents a valid
non-null value."""

_T = TypeVar('_T')
Possibly = Union[_Missing, _T]
"""Possibly[T] is like Optional[T] but uses MISSING for missing values."""

AnyCallable = Callable[..., Any]

F = TypeVar('F', bound=AnyCallable)
"""Type variable for a Callable."""

Decorator = Callable[[AnyCallable], AnyCallable]
"""Type alias for a simple function decorator."""
