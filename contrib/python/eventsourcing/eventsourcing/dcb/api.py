from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from eventsourcing.persistence import ProgrammingError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from typing_extensions import Self


@dataclass
class DCBQueryItem:
    types: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)


@dataclass
class DCBQuery:
    items: list[DCBQueryItem] = field(default_factory=list)


@dataclass
class DCBAppendCondition:
    fail_if_events_match: DCBQuery = field(default_factory=DCBQuery)
    after: int | None = None


@dataclass
class DCBEvent:
    type: str
    data: bytes
    tags: list[str] = field(default_factory=list)


@dataclass
class DCBSequencedEvent:
    event: DCBEvent
    position: int


class DCBReadResponse(Iterator[DCBSequencedEvent], ABC):
    @property
    @abstractmethod
    def head(self) -> int | None:
        pass  # pragma: no cover

    @abstractmethod
    def __next__(self) -> DCBSequencedEvent:
        pass  # pragma: no cover

    # @abstractmethod
    # def next_batch(self) -> list[DCBSequencedEvent]:
    #     """
    #     Returns a batch of events as a list.
    #     Updates the head position similar to __next__.
    #     """


class DCBRecorder(ABC):
    @abstractmethod
    def read(
        self,
        query: DCBQuery | None = None,
        *,
        after: int | None = None,
        limit: int | None = None,
    ) -> DCBReadResponse:
        """
        Returns all events, unless 'after' is given then only those with position
        greater than 'after', and unless any query items are given, then only those
        that match at least one query item. An event matches a query item if its type
        is in the item types or there are no item types, and if all the item tags are
        in the event tags.
        """

    @abstractmethod
    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        """
        Appends given events to the event store, unless the condition fails.
        """

    @abstractmethod
    def subscribe(
        self,
        query: DCBQuery | None = None,
        *,
        after: int | None = None,
    ) -> DCBSubscription[Self]:
        """
        Returns all events, unless 'after' is given then only those with position
        greater than 'after', and unless any query items are given, then only those
        that match at least one query item. An event matches a query item if its type
        is in the item types or there are no item types, and if all the item tags are
        in the event tags. The subscription will block when the last recorded event
        is received, and then continue when new events are recorded.
        """


TDCBRecorder_co = TypeVar("TDCBRecorder_co", bound=DCBRecorder, covariant=True)


class DCBSubscription(Iterator[DCBSequencedEvent], Generic[TDCBRecorder_co]):
    def __init__(
        self,
        recorder: TDCBRecorder_co,
        query: DCBQuery | None = None,
        after: int | None = None,
    ) -> None:
        self._recorder = recorder
        self._query = query
        self._has_been_entered = False
        self._has_been_stopped = False
        self._last_position: int = after or 0

    def __enter__(self) -> Self:
        if self._has_been_entered:
            msg = "Already entered subscription context manager"
            raise ProgrammingError(msg)
        self._has_been_entered = True
        return self

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        if not self._has_been_entered:
            msg = "Not already entered subscription context manager"
            raise ProgrammingError(msg)
        self.stop()

    def stop(self) -> None:
        """Stops the subscription."""
        self._has_been_stopped = True

    def __iter__(self) -> Self:
        return self

    @abstractmethod
    def __next__(self) -> DCBSequencedEvent:
        """Returns the next DCBEvent in the sequence."""
