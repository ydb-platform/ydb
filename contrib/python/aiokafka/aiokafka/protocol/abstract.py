import abc
from io import BytesIO
from typing import Generic, TypeVar

T = TypeVar("T")


class AbstractType(Generic[T], metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def encode(cls, value: T) -> bytes: ...

    @classmethod
    @abc.abstractmethod
    def decode(cls, data: BytesIO) -> T: ...

    @classmethod
    def repr(cls, value: T) -> str:
        return repr(value)
