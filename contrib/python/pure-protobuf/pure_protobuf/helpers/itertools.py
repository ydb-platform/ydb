from collections.abc import Iterator
from typing import Callable, Generic, TypeVar

from typing_extensions import ParamSpec

from pure_protobuf.interfaces._repr import ReprWithInner

P = ParamSpec("P")
R = TypeVar("R")


class ReadCallback(ReprWithInner, Generic[P, R]):
    """Convert a reader, which returns a singular scalar value, into an iterable reader."""

    __slots__ = ("inner",)

    # noinspection PyProtocol
    inner: Callable[P, R]

    def __init__(self, inner: Callable[P, R]) -> None:  # noqa: D107
        self.inner = inner

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Iterator[R]:
        yield self.inner(*args, **kwargs)
