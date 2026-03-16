from asyncio import CancelledError, Event, Task, ensure_future, wait
from concurrent.futures import FIRST_COMPLETED
from inspect import isasyncgen, isawaitable
from types import TracebackType
from typing import Any, AsyncIterable, Callable, Optional, Set, Type, Union

__all__ = ["MapAsyncIterator"]


# noinspection PyAttributeOutsideInit
class MapAsyncIterator:
    """Map an AsyncIterable over a callback function.

    Given an AsyncIterable and a callback function, return an AsyncIterator which
    produces values mapped via calling the callback function.

    When the resulting AsyncIterator is closed, the underlying AsyncIterable will also
    be closed.
    """

    def __init__(self, iterable: AsyncIterable, callback: Callable) -> None:
        self.iterator = iterable.__aiter__()
        self.callback = callback
        self._close_event = Event()

    def __aiter__(self) -> "MapAsyncIterator":
        """Get the iterator object."""
        return self

    async def __anext__(self) -> Any:
        """Get the next value of the iterator."""
        if self.is_closed:
            if not isasyncgen(self.iterator):
                raise StopAsyncIteration
            value = await self.iterator.__anext__()
        else:
            aclose = ensure_future(self._close_event.wait())
            anext = ensure_future(self.iterator.__anext__())

            try:
                pending: Set[Task] = (
                    await wait([aclose, anext], return_when=FIRST_COMPLETED)
                )[1]
            except CancelledError:
                # cancel underlying tasks and close
                aclose.cancel()
                anext.cancel()
                await self.aclose()
                raise  # re-raise the cancellation

            for task in pending:
                task.cancel()

            if aclose.done():
                raise StopAsyncIteration

            error = anext.exception()
            if error:
                raise error

            value = anext.result()

        result = self.callback(value)

        return await result if isawaitable(result) else result

    async def athrow(
        self,
        type_: Union[BaseException, Type[BaseException]],
        value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        """Throw an exception into the asynchronous iterator."""
        if self.is_closed:
            return
        if isinstance(type_, BaseException):
            value = type_
            type_ = type(value)
            traceback = value.__traceback__
        athrow = getattr(self.iterator, "athrow", None)
        if athrow:
            await athrow(type_ if value is None else value)
        else:
            await self.aclose()
            if value is None:
                if traceback is None:
                    raise type_  # pragma: no cover
                value = type_ if isinstance(value, BaseException) else type_()
            if traceback is not None:
                value = value.with_traceback(traceback)
            raise value

    async def aclose(self) -> None:
        """Close the iterator."""
        if not self.is_closed:
            aclose = getattr(self.iterator, "aclose", None)
            if aclose:
                try:
                    await aclose()
                except RuntimeError:
                    pass
            self.is_closed = True

    @property
    def is_closed(self) -> bool:
        """Check whether the iterator is closed."""
        return self._close_event.is_set()

    @is_closed.setter
    def is_closed(self, value: bool) -> None:
        """Mark the iterator as closed."""
        if value:
            self._close_event.set()
        else:
            self._close_event.clear()
