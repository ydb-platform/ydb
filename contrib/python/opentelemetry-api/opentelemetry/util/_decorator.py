# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import contextlib
import functools
import inspect
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Generic, TypeVar

V = TypeVar("V")
R = TypeVar("R")  # Return type
Pargs = TypeVar("Pargs")  # Generic type for arguments
Pkwargs = TypeVar("Pkwargs")  # Generic type for arguments

# We don't actually depend on typing_extensions but we can use it in CI with this conditional
# import. ParamSpec can be imported directly from typing after python 3.9 is dropped
# https://peps.python.org/pep-0612/.
if TYPE_CHECKING:
    from typing_extensions import ParamSpec

    P = ParamSpec("P")  # Generic type for all arguments


class _AgnosticContextManager(
    contextlib._GeneratorContextManager[R],
    Generic[R],
):  # pylint: disable=protected-access
    """Context manager that can decorate both async and sync functions.

    This is an overridden version of the contextlib._GeneratorContextManager
    class that will decorate async functions with an async context manager
    to end the span AFTER the entire async function coroutine finishes.

    Else it will report near zero spans durations for async functions.

    We are overriding the contextlib._GeneratorContextManager class as
    reimplementing it is a lot of code to maintain and this class (even if it's
    marked as protected) doesn't seems like to be evolving a lot.

    For more information, see:
    https://github.com/open-telemetry/opentelemetry-python/pull/3633
    """

    def __enter__(self) -> R:
        """Reimplementing __enter__ to avoid the type error.

        The original __enter__ method returns Any type, but we want to return R.
        """
        del self.args, self.kwds, self.func  # type: ignore
        try:
            return next(self.gen)  # type: ignore
        except StopIteration:
            raise RuntimeError("generator didn't yield") from None

    def __call__(self, func: V) -> V:  # pyright: ignore [reportIncompatibleMethodOverride]
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)  # type: ignore
            async def async_wrapper(*args: Pargs, **kwargs: Pkwargs) -> R:  # pyright: ignore [reportInvalidTypeVarUse]
                with self._recreate_cm():  # type: ignore
                    return await func(*args, **kwargs)  # type: ignore

            return async_wrapper  # type: ignore
        return super().__call__(func)  # type: ignore


def _agnosticcontextmanager(
    func: "Callable[P, Iterator[R]]",
) -> "Callable[P, _AgnosticContextManager[R]]":
    @functools.wraps(func)
    def helper(*args: Pargs, **kwargs: Pkwargs) -> _AgnosticContextManager[R]:  # pyright: ignore [reportInvalidTypeVarUse]
        return _AgnosticContextManager(func, args, kwargs)  # pyright: ignore [reportArgumentType]

    # Ignoring the type to keep the original signature of the function
    return helper  # type: ignore[return-value]
