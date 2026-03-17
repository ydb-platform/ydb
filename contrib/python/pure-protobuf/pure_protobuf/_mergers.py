from typing import Generic, Optional

from pure_protobuf._accumulators import AccumulateMessages
from pure_protobuf.interfaces._repr import ReprWithInner
from pure_protobuf.interfaces._vars import FieldT, MessageT, RecordT
from pure_protobuf.interfaces.merge import Merge


class MergeLastOneWins(Merge[FieldT], Generic[FieldT]):
    """
    Always returns the right-hand side.

    See Also:
        - https://developers.google.com/protocol-buffers/docs/encoding#last-one-wins
    """

    def __call__(self, _lhs: Optional[FieldT], rhs: Optional[FieldT]) -> Optional[FieldT]:
        return rhs


class MergeConcatenate(Merge[list[RecordT]], Generic[RecordT]):
    def __call__(
        self,
        lhs: Optional[list[RecordT]],
        rhs: Optional[list[RecordT]],
    ) -> Optional[list[RecordT]]:
        if lhs is None:
            return rhs
        if rhs is None:
            return lhs
        lhs.extend(rhs)
        return lhs


class MergeMessages(Merge[MessageT], ReprWithInner):
    __slots__ = ("inner",)

    # noinspection PyProtocol
    def __init__(self, inner: AccumulateMessages[MessageT]) -> None:
        self.inner = inner

    def __call__(self, lhs: Optional[MessageT], rhs: Optional[MessageT]) -> Optional[MessageT]:
        """
        Merge the two messages.

        Returns:
            Merged message: it'll be either possibly updated `lhs` or non-updated `rhs`.

        Notes:
            - **Never** reuse either `lhs` or `rhs` after calling this method, consider
            them consumed.
        """
        if rhs is not None:
            # noinspection PyTypeChecker
            return self.inner(lhs, (rhs,))
        return lhs
