from abc import abstractmethod
from collections.abc import Iterable
from typing import Optional, Protocol

from pure_protobuf.interfaces._repr import Repr
from pure_protobuf.interfaces._vars import FieldT, RecordT_contra


class Accumulate(Repr, Protocol[FieldT, RecordT_contra]):
    """
    Accumulates the value in the accumulator (also known as «fold» or «aggregate»).
    Implementation of the protocol defines how a record value from the input buffer
    should be «merged» into a corresponding attribute.
    """

    @abstractmethod
    def __call__(
        self,
        __accumulator: Optional[FieldT],
        __values: Iterable[RecordT_contra],
    ) -> FieldT:
        raise NotImplementedError(f"{type(self)}.__call__()")
