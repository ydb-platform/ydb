import typing as tp

from .iterator2 import Iterator2


# ---

class Iterable2[SomeIterator2: Iterator2](tp.Protocol):
    def __iter2__(self) -> SomeIterator2: ...
