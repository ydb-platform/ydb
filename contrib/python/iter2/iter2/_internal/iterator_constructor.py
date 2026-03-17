import typing as tp

from .interfaces.iterator2 import Iterator2
from .interfaces.iterable2 import Iterable2

from .iterator_proxy import Iterator2Proxy


# ---

@tp.overload
def construct_iterator2[SomeIterator2: Iterator2](
    iterable: Iterable2[SomeIterator2],
) -> SomeIterator2: ...


@tp.overload
def construct_iterator2[Item](
    iterable: tp.Iterable[Item],
) -> Iterator2Proxy[Item]: ...


def construct_iterator2(iterable):
    try:
        return iterable.__iter2__()
    except AttributeError:
        return Iterator2Proxy(iter(iterable))
