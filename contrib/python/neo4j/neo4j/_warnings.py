# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import inspect
from functools import wraps
from warnings import warn

from . import _typing as t
from .warnings import PreviewWarning


if t.TYPE_CHECKING:
    _FuncT = t.TypeVar("_FuncT", bound=t.Callable)


# Copy globals as function locals to make sure that they are available
# during Python shutdown when the Pool is destroyed.
def deprecation_warn(message, stack_level=1, _warn=warn):
    _warn(message, category=DeprecationWarning, stacklevel=stack_level + 1)


def deprecated(message: str) -> t.Callable[[_FuncT], _FuncT]:
    """
    Decorate deprecated functions and methods.

    ::

        @deprecated("'foo' has been deprecated in favour of 'bar'")
        def foo(x):
            pass

        @property
        @deprecated("'bar' will be internal without a replacement")
        def bar(self):
            return "bar"

        @property
        def baz(self):
            return self._baz

        @baz.setter
        @deprecated("'baz' will be read-only in the future")
        def baz(self, value):
            self._baz = value

    """
    return _make_warning_decorator(message, deprecation_warn)


def preview_warn(message, stack_level=1):
    message += (
        " It might be changed without following the deprecation policy. "
        "See also "
        "https://github.com/neo4j/neo4j-python-driver/wiki/preview-features."
    )
    warn(message, category=PreviewWarning, stacklevel=stack_level + 1)


def preview(message) -> t.Callable[[_FuncT], _FuncT]:
    """
    Decorate functions and methods as preview.

    ::

        @preview("foo is a preview.")
        def foo(x):
            pass
    """
    return _make_warning_decorator(message, preview_warn)


if t.TYPE_CHECKING:

    class _WarningFunc(t.Protocol):
        def __call__(self, message: str, stack_level: int = 1) -> None: ...
else:
    _WarningFunc = object


def _make_warning_decorator(
    message: str,
    warning_func: _WarningFunc,
) -> t.Callable[[_FuncT], _FuncT]:
    def decorator(f):
        if inspect.iscoroutinefunction(f):

            @wraps(f)
            async def inner(*args, **kwargs):
                warning_func(message, stack_level=2)
                return await f(*args, **kwargs)

            inner._without_warning = f
            return inner

        if inspect.isclass(f):
            if hasattr(f, "__init__"):
                original_init = f.__init__

                @wraps(original_init)
                def inner(self, *args, **kwargs):
                    warning_func(message, stack_level=2)
                    return original_init(self, *args, **kwargs)

                def _without_warning(cls, *args, **kwargs):
                    obj = cls.__new__(cls, *args, **kwargs)
                    original_init(obj, *args, **kwargs)
                    return obj

                f.__init__ = inner
                f._without_warning = classmethod(_without_warning)
                return f
            raise TypeError("Cannot decorate class without __init__")

        else:

            @wraps(f)
            def inner(*args, **kwargs):
                warning_func(message, stack_level=2)
                return f(*args, **kwargs)

            inner._without_warning = f
            return inner

    return decorator


# Copy globals as function locals to make sure that they are available
# during Python shutdown when the Pool is destroyed.
def unclosed_resource_warn(obj, _warn=warn):
    cls_name = obj.__class__.__name__
    msg = f"unclosed  {cls_name}: {obj!r}."
    _warn(msg, ResourceWarning, stacklevel=2, source=obj)
