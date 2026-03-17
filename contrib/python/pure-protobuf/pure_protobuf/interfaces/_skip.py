from abc import abstractmethod
from typing import IO, Protocol

from pure_protobuf.interfaces._repr import Repr


class Skip(Repr, Protocol):
    """
    Knows how to skip a value. It's equivalent to `Read`, but is supposed to be a little
    more efficient because it doesn't need to actually parse the field data.
    """

    @abstractmethod
    def __call__(self, io: IO[bytes]) -> None:
        raise NotImplementedError


class SkipNoOperation(Skip):
    def __call__(self, io: IO[bytes]) -> None:
        """Do nothing. This is useful for the deprecated groups feature."""


skip_no_operation = SkipNoOperation()
