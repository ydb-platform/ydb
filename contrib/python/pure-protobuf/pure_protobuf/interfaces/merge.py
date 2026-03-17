from abc import abstractmethod
from typing import Optional, Protocol

from pure_protobuf.interfaces._repr import Repr
from pure_protobuf.interfaces._vars import FieldT


class Merge(Repr, Protocol[FieldT]):
    """Defines how to merge two values of the same field."""

    @abstractmethod
    def __call__(
        self,
        __accumulator: Optional[FieldT],
        __value: Optional[FieldT],
    ) -> Optional[FieldT]:
        raise NotImplementedError(f"{type(self)}.__call__()")
