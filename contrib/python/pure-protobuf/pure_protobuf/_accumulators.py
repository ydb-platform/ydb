from collections.abc import Iterable
from typing import Generic, Optional

from pure_protobuf.interfaces._repr import ReprWithInner
from pure_protobuf.interfaces._vars import MessageT, RecordT
from pure_protobuf.interfaces.accumulate import Accumulate


class AccumulateLastOneWins(Accumulate[RecordT, RecordT], Generic[RecordT]):
    """
    The simplest accumulator which keeps only the last seen item.

    Typical use case is fields of primitive scalar types.

    See Also:
        - https://developers.google.com/protocol-buffers/docs/encoding#last-one-wins
    """

    def __call__(self, accumulator: Optional[RecordT], other: Iterable[RecordT]) -> RecordT:
        for accumulator in other:  # noqa: B007
            pass
        return accumulator


class AccumulateAppend(Accumulate[list[RecordT], RecordT]):
    def __call__(
        self,
        accumulator: Optional[list[RecordT]],
        other: Iterable[RecordT],
    ) -> list[RecordT]:
        """Append all items from the `other` into the accumulator."""
        if accumulator is None:
            accumulator = []
        accumulator.extend(other)
        return accumulator


class AccumulateMessages(Accumulate[MessageT, MessageT], ReprWithInner):
    inner: type[MessageT]

    __slots__ = ("inner",)

    # noinspection PyProtocol
    def __init__(self, inner: type[MessageT]) -> None:
        self.inner = inner

    def __call__(self, lhs: Optional[MessageT], rhs: Iterable[MessageT]) -> MessageT:
        """
        Merge the two messages into the left one.

        Notes:
            - **Never** reuse `lhs` or `rhs` afterwards, consider them consumed by the method.
        """
        for other in rhs:
            assert other is not None, "right-hand side items must not be `None`"
            if lhs is not None:
                for name, descriptor in other.__PROTOBUF_FIELDS_BY_NUMBER__.values():
                    setattr(
                        lhs,
                        name,
                        descriptor.merge(getattr(lhs, name), getattr(other, name)),
                    )
                    one_of = descriptor.one_of
                    if one_of is not None:
                        one_of._keep_attribute(lhs, descriptor.number)
            else:
                lhs = other

        assert lhs is not None, "there must be at least one right-hand side item"
        return lhs
