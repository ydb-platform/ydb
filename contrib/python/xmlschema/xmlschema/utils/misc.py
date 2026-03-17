#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import warnings
from functools import wraps
from collections.abc import Callable, Iterator
from typing import cast, Any, TypeVar

from xmlschema.exceptions import XMLSchemaException, XMLSchemaValueError, \
    XMLSchemaTypeError, XMLSchemaAttributeError, XMLSchemaKeyError, \
    XMLSchemaRuntimeError


def is_subclass(t: type[Any], cls: type[Any]) -> bool:
    """A safe subclass checker."""
    return isinstance(t, type) and issubclass(t, cls)


def iter_class_slots(obj: Any) -> Iterator[str]:
    """Iterates slots defined for a class and its bases."""
    for cls in obj.__class__.__mro__:
        if hasattr(cls, '__slots__'):
            yield from cls.__slots__


FT = TypeVar('FT', bound=Callable[..., Any])
RT = TypeVar('RT')


def catchable_xmlschema_error(func: FT) -> FT:
    """Force a function to raise only an XMLSchemaException or a subclass of it."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except XMLSchemaException:
            raise
        except ValueError as err:
            raise XMLSchemaValueError(err)
        except TypeError as err:
            raise XMLSchemaTypeError(err)
        except AttributeError as err:
            raise XMLSchemaAttributeError(err)
        except KeyError as err:
            raise XMLSchemaKeyError(err)
        except RuntimeError as err:
            raise XMLSchemaRuntimeError(err)

    return cast(FT, wrapper)


def deprecated(version: str, stacklevel: int = 1, alt: str = '') \
        -> Callable[[Callable[..., RT]],  Callable[..., RT]]:

    def decorator(func: Callable[..., RT]) -> Callable[..., RT]:
        msg = f"{func.__qualname__!r} will be removed in v{version}."
        if alt:
            msg = f"{msg[:-1]}, {alt}."

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> RT:
            warnings.warn(msg, DeprecationWarning, stacklevel=stacklevel + 1)
            return func(*args, **kwargs)
        return wrapper

    return decorator


def will_change(version: str, stacklevel: int = 1, alt: str = '') \
        -> Callable[[Callable[..., RT]],  Callable[..., RT]]:

    def decorator(func: Callable[..., RT]) -> Callable[..., RT]:
        msg = f"{func.__qualname__!r} will change from v{version}."
        if alt:
            msg = f"{msg[:-1]}, {alt}."

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> RT:
            warnings.warn(msg, FutureWarning, stacklevel=stacklevel + 1)
            return func(*args, **kwargs)
        return wrapper

    return decorator
