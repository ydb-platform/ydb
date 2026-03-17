from typing import TypeVar, Iterator, AsyncIterator

T = TypeVar("T")


class AnyIterator(Iterator[T], AsyncIterator[T]):
    """
    AnyIterator supports iteration through both `for ... in <AnyIterator>` and `async for ... in <AnyIterator> syntaxes.
    """

    def __init__(
        self, iterator: Iterator[T], async_iterator: AsyncIterator[T]
    ) -> None:
        self._iterator = iterator
        self._async_iterator = async_iterator

        self._sync_iterated = False
        self._async_iterated = False

    def __next__(self) -> T:
        if self._async_iterated:
            raise RuntimeError(
                "AnyIterator error: cannot mix sync and async iteration"
            )
        self._sync_iterated = True
        return self._iterator.__next__()

    async def __anext__(self) -> T:
        if self._sync_iterated:
            raise RuntimeError(
                "AnyIterator error: cannot mix sync and async iteration"
            )
        self._async_iterated = True
        return await self._async_iterator.__anext__()
