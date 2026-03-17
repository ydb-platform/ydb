from abc import abstractmethod
from collections.abc import Iterator
from typing import IO, Protocol

from pure_protobuf.interfaces._repr import Repr
from pure_protobuf.interfaces._vars import RecordT_co
from pure_protobuf.io.wire_type import WireType


class ReadSingular(Repr, Protocol[RecordT_co]):
    @abstractmethod
    def __call__(self, __io: IO[bytes]) -> RecordT_co:
        raise NotImplementedError


class Read(Repr, Protocol[RecordT_co]):
    """Deserializes a value from the stream and yields it as an iterator."""

    @abstractmethod
    def __call__(self, __io: IO[bytes]) -> Iterator[RecordT_co]:
        raise NotImplementedError


class ReadTyped(Repr, Protocol[RecordT_co]):
    @abstractmethod
    def __call__(self, __io: IO[bytes], __actual_wire_type: WireType) -> Iterator[RecordT_co]:
        raise NotImplementedError
