import sys
import re
import asyncio
from typing import (
    Optional,
    Type,
    TypeVar,
    Union,
    Awaitable,
)

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.description import Description
from hamcrest.core.matcher import Matcher

__author__ = "David Keijser"
__copyright__ = "Copyright 2021 hamcrest.org"
__license__ = "BSD, see License.txt"

T = TypeVar("T")

if sys.version_info > (3, 9):
    # Same as used in typeshed for asyncio.ensure_future
    FutureT = asyncio.Future[T]
    FutureLike = Union[asyncio.Future[T], Awaitable[T]]
else:
    # Future is not a parametrised type in earlier version of python
    FutureT = asyncio.Future
    FutureLike = Union[asyncio.Future, Awaitable]


class FutureRaising(BaseMatcher[asyncio.Future]):
    def __init__(
        self,
        expected: Type[Exception],
        pattern: Optional[str] = None,
        matching: Optional[Matcher] = None,
    ) -> None:
        self.pattern = pattern
        self.matcher = matching
        self.expected = expected

    def _matches(self, future: asyncio.Future) -> bool:
        if not asyncio.isfuture(future):
            return False

        if not future.done():
            return False

        if future.cancelled():
            return False

        exc = future.exception()
        if exc is None:
            return False

        if isinstance(exc, self.expected):
            if self.pattern is not None:
                if re.search(self.pattern, str(exc)) is None:
                    return False
            if self.matcher is not None:
                if not self.matcher.matches(exc):
                    return False
            return True

        return False

    def describe_to(self, description: Description) -> None:
        description.append_text("Expected a completed future with exception %s" % self.expected)

    def describe_mismatch(self, future: asyncio.Future, description: Description) -> None:
        if not asyncio.isfuture(future):
            description.append_text("%s is not a future" % future)
            return

        if not future.done():
            description.append_text("%s is not completed yet" % future)
            return

        if future.cancelled():
            description.append_text("%s is cancelled" % future)
            return

        exc = future.exception()
        if exc is None:
            description.append_text("No exception raised.")
        elif isinstance(exc, self.expected):
            if self.pattern is not None or self.matcher is not None:
                description.append_text("Correct assertion type raised, but ")
                if self.pattern is not None:
                    description.append_text('the expected pattern ("%s") ' % self.pattern)
                if self.pattern is not None and self.matcher is not None:
                    description.append_text("and ")
                if self.matcher is not None:
                    description.append_description_of(self.matcher)
                    description.append_text(" ")
                description.append_text('not found. Exception message was: "%s"' % str(exc))
        else:
            description.append_text("%r of type %s was raised instead" % (exc, type(exc)))

    def describe_match(self, future: asyncio.Future, match_description: Description) -> None:
        exc = future.exception()
        match_description.append_text("%r of type %s was raised." % (exc, type(exc)))


def future_raising(
    exception: Type[Exception], pattern=None, matching=None
) -> Matcher[asyncio.Future]:
    """Matches a future with the expected exception.

    :param exception:  The class of the expected exception
    :param pattern:    Optional regular expression to match exception message.
    :param matching:   Optional Hamcrest matchers to apply to the exception.

    Expects the actual to be an already resolved future. The :py:func:`~hamcrest:core.core.future.resolved` helper can be used to wait for a future to resolve.
    Optional argument pattern should be a string containing a regular expression.  If provided,
    the string representation of the actual exception - e.g. `str(actual)` - must match pattern.

    Examples::

        assert_that(somefuture, future_exception(ValueError))
        assert_that(
            await resolved(async_http_get()),
            future_exception(HTTPError, matching=has_properties(status_code=500)
        )
    """
    return FutureRaising(exception, pattern, matching)


async def resolved(obj: FutureLike) -> FutureT:
    """Wait for an async operation to finish and return a resolved future object with the result.

    :param obj: A future like object or an awaitable object.
    """
    fut = asyncio.ensure_future(obj)
    await asyncio.wait([fut])
    return fut
