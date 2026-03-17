from abc import abstractmethod
from typing import IO, Protocol

from pure_protobuf.interfaces._repr import Repr
from pure_protobuf.interfaces._vars import FieldT_contra


class Write(Repr, Protocol[FieldT_contra]):
    """Serializes and writes the value into the stream."""

    @abstractmethod
    def __call__(self, __value: FieldT_contra, __io: IO[bytes]) -> None:
        raise NotImplementedError
