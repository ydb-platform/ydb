from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Generic

from eventsourcing.dcb.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBQuery,
    DCBQueryItem,
    DCBReadResponse,
    DCBRecorder,
    DCBSequencedEvent,
    DCBSubscription,
    TDCBRecorder_co,
)
from eventsourcing.dcb.domain import (
    Selector,
    Tagged,
    TDecision,
)
from eventsourcing.persistence import (
    BaseInfrastructureFactory,
    Queue,
    ShutDown,
    TTrackingRecorder,
)
from eventsourcing.utils import get_topic

if TYPE_CHECKING:
    from collections.abc import Sequence


class DCBMapper(ABC, Generic[TDecision]):
    @abstractmethod
    def to_dcb_event(self, event: Tagged[TDecision]) -> DCBEvent:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def to_domain_event(self, event: DCBEvent) -> Tagged[TDecision]:
        raise NotImplementedError  # pragma: no cover


class DCBEventStore(Generic[TDecision]):
    def __init__(self, mapper: DCBMapper[TDecision], recorder: DCBRecorder):
        self.mapper = mapper
        self.recorder = recorder

    def append(
        self,
        events: Sequence[Tagged[TDecision]],
        cb: Selector | Sequence[Selector] | None = None,
        after: int | None = None,
    ) -> int:
        if len(events) == 0:
            return 0
        condition = (
            None
            if cb is None and after is None
            else DCBAppendCondition(
                fail_if_events_match=self._cb_to_dcb_query(cb),
                after=after,
            )
        )
        return self.recorder.append(
            events=[self.mapper.to_dcb_event(e) for e in events],
            condition=condition,
        )

    def read(
        self,
        cb: Selector | Sequence[Selector] | None = None,
        *,
        after: int | None = None,
    ) -> DCBEventStoreReadResponse[TDecision]:
        query = self._cb_to_dcb_query(cb)
        read_response = self.recorder.read(
            query=query,
            after=after,
        )
        return DCBEventStoreReadResponse(read_response, self.mapper)

    @staticmethod
    def _cb_to_dcb_query(
        cb: Selector | Sequence[Selector] | None = None,
    ) -> DCBQuery:
        cb = [cb] if isinstance(cb, Selector) else cb or []
        return DCBQuery(
            items=[
                DCBQueryItem(
                    types=[get_topic(t) for t in s.types],
                    tags=list(s.tags),
                )
                for s in cb
            ]
        )


class DCBEventStoreReadResponse(Iterator[Tagged[TDecision]]):
    def __init__(
        self, dcb_read_response: DCBReadResponse, mapper: DCBMapper[TDecision]
    ):
        self._dcb_read_response = dcb_read_response
        self._mapper = mapper

    @property
    def head(self) -> int | None:
        return self._dcb_read_response.head

    def __next__(self) -> Tagged[TDecision]:
        dcb_sequenced_event = self._dcb_read_response.__next__()
        return self._mapper.to_domain_event(dcb_sequenced_event.event)


class NotFoundError(Exception):
    pass


class DCBInfrastructureFactory(BaseInfrastructureFactory[TTrackingRecorder], ABC):
    @abstractmethod
    def dcb_recorder(self) -> DCBRecorder:
        pass  # pragma: no cover


class DCBListenNotifySubscription(DCBSubscription[TDCBRecorder_co]):
    def __init__(
        self,
        recorder: TDCBRecorder_co,
        query: DCBQuery | None = None,
        after: int | None = None,
    ) -> None:
        super().__init__(recorder=recorder, query=query, after=after)
        self.select_limit = 500
        self._events: Sequence[DCBSequencedEvent] = []
        self._events_index: int = 0
        self._events_queue: Queue[Sequence[DCBSequencedEvent]] = Queue(maxsize=10)
        self._has_been_notified = threading.Event()
        self._thread_error: BaseException | None = None
        self._pull_thread = threading.Thread(target=self._loop_on_pull)
        self._pull_thread.start()

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        super().__exit__(*args, **kwargs)
        self._pull_thread.join()

    def stop(self) -> None:
        """Stops the subscription."""
        super().stop()
        self._events_queue.shutdown(  # pyright: ignore[reportAttributeAccessIssue]
            immediate=True
        )
        self._has_been_notified.set()

    def __next__(self) -> DCBSequencedEvent:
        # If necessary, get a new list of events from the recorder.
        if self._events_index == len(self._events) and not self._has_been_stopped:
            try:
                self._events = self._events_queue.get()
            except ShutDown:
                pass
            else:
                self._events_queue.task_done()
                self._events_index = 0

        # Stop the iteration if subscription has been stopped.
        if self._has_been_stopped:
            # Maybe raise thread error.
            if self._thread_error is not None:
                raise self._thread_error
            raise StopIteration

        # Return an event from previously obtained list.
        event = self._events[self._events_index]
        self._events_index += 1
        return event

    def _loop_on_pull(self) -> None:
        try:
            self._pull()  # Already recorded events.
            while not self._has_been_stopped:
                self._has_been_notified.wait()
                self._pull()  # Newly recorded events.
        except BaseException as e:  # pragma: no cover
            if self._thread_error is None:
                self._thread_error = e
            self.stop()

    def _pull(self) -> None:
        while not self._has_been_stopped:
            self._has_been_notified.clear()
            events = list(
                self._recorder.read(
                    query=self._query,
                    after=self._last_position,
                    limit=self.select_limit,
                )
            )
            if len(events) > 0:
                try:
                    self._events_queue.put(events)
                except ShutDown:  # pragma: no cover
                    # TODO: Cover this with a test...
                    break
                self._last_position = events[-1].position
            if len(events) < self.select_limit:
                break
