from __future__ import annotations

import contextlib
import os
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Sequence
from copy import deepcopy
from dataclasses import dataclass
from itertools import chain
from threading import Event, Lock
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    TypeVar,
    cast,
)
from warnings import warn

from eventsourcing.domain import (
    Aggregate,
    BaseAggregate,
    CanMutateProtocol,
    CollectEventsProtocol,
    DomainEventProtocol,
    EventSourcingError,
    MutableOrImmutableAggregate,
    SDomainEvent,
    SnapshotProtocol,
    TAggregateID,
    TDomainEvent,
    TMutableOrImmutableAggregate,
    datetime_now_with_tzinfo,
)
from eventsourcing.persistence import (
    ApplicationRecorder,
    DatetimeAsISO,
    DecimalAsStr,
    EventStore,
    InfrastructureFactory,
    JSONTranscoder,
    Mapper,
    Notification,
    Recording,
    Tracking,
    Transcoder,
    UUIDAsHex,
)
from eventsourcing.utils import Environment, EnvType, strtobool

if TYPE_CHECKING:
    from types import TracebackType
    from uuid import UUID

    from typing_extensions import Self

ProjectorFunction = Callable[
    [TMutableOrImmutableAggregate | None, Iterable[TDomainEvent]],
    TMutableOrImmutableAggregate | None,
]

MutatorFunction = Callable[
    [TDomainEvent, TMutableOrImmutableAggregate | None],
    TMutableOrImmutableAggregate | None,
]


class ProgrammingError(Exception):
    pass


def project_aggregate(
    aggregate: TMutableOrImmutableAggregate | None,
    domain_events: Iterable[DomainEventProtocol[Any]],
) -> TMutableOrImmutableAggregate | None:
    """Projector function for aggregate projections, which works
    by successively calling aggregate mutator function mutate()
    on each of the given list of domain events in turn.
    """
    for domain_event in domain_events:
        assert isinstance(domain_event, CanMutateProtocol)
        aggregate = domain_event.mutate(aggregate)
    return aggregate


S = TypeVar("S")
T = TypeVar("T")


class Cache(Generic[S, T]):
    def __init__(self) -> None:
        self.cache: dict[S, Any] = {}

    def get(self, key: S, *, evict: bool = False) -> T:
        if evict:
            return self.cache.pop(key)
        return self.cache[key]

    def put(self, key: S, value: T | None) -> None:
        if value is not None:
            self.cache[key] = value


class LRUCache(Cache[S, T]):
    """Size limited caching that tracks accesses by recency.

    This is basically copied from functools.lru_cache. But
    we need to know when there was a cache hit, so we can
    fast-forward the aggregate with new stored events.
    """

    sentinel = object()  # unique object used to signal cache misses
    PREV, NEXT, KEY, RESULT = 0, 1, 2, 3  # names for the link fields

    def __init__(self, maxsize: int):
        # Constants shared by all lru cache instances:
        super().__init__()
        self.maxsize = maxsize
        self.full = False
        self.lock = Lock()  # because linkedlist updates aren't threadsafe
        self.root: list[Any] = []  # root of the circular doubly linked list
        self.clear()

    def clear(self) -> None:
        self.root[:] = [
            self.root,
            self.root,
            None,
            None,
        ]  # initialize by pointing to self

    def get(self, key: S, *, evict: bool = False) -> T:
        with self.lock:
            link = self.cache.get(key)
            if link is not None:
                link_prev, link_next, _key, result = link
                if not evict:
                    # Move the link to the front of the circular queue.
                    link_prev[self.NEXT] = link_next
                    link_next[self.PREV] = link_prev
                    last = self.root[self.PREV]
                    last[self.NEXT] = self.root[self.PREV] = link
                    link[self.PREV] = last
                    link[self.NEXT] = self.root
                else:
                    # Remove the link.
                    link_prev[self.NEXT] = link_next
                    link_next[self.PREV] = link_prev
                    del self.cache[key]
                    self.full = self.cache.__len__() >= self.maxsize

                return result
            raise KeyError

    def put(self, key: S, value: T | None) -> Any | None:
        evicted_key = None
        evicted_value = None
        with self.lock:
            link = self.cache.get(key)
            if link is not None:
                # Set value.
                link[self.RESULT] = value
                # Move the link to the front of the circular queue.
                link_prev, link_next, _key, _ = link
                link_prev[self.NEXT] = link_next
                link_next[self.PREV] = link_prev
                last = self.root[self.PREV]
                last[self.NEXT] = self.root[self.PREV] = link
                link[self.PREV] = last
                link[self.NEXT] = self.root
            elif self.full:
                # Use the old root to store the new key and result.
                oldroot = self.root
                oldroot[self.KEY] = key
                oldroot[self.RESULT] = value
                # Empty the oldest link and make it the new root.
                # Keep a reference to the old key and old result to
                # prevent their ref counts from going to zero during the
                # update. That will prevent potentially arbitrary object
                # clean-up code (i.e. __del__) from running while we're
                # still adjusting the links.
                self.root = oldroot[self.NEXT]
                evicted_key = self.root[self.KEY]
                evicted_value = self.root[self.RESULT]
                self.root[self.KEY] = self.root[self.RESULT] = None
                # Now update the cache dictionary.
                del self.cache[evicted_key]
                # Save the potentially reentrant cache[key] assignment
                # for last, after the root and links have been put in
                # a consistent state.
                self.cache[key] = oldroot
            else:
                # Put result in a new link at the front of the queue.
                last = self.root[self.PREV]
                link = [last, self.root, key, value]
                last[self.NEXT] = self.root[self.PREV] = self.cache[key] = link
                # Use the __len__() bound method instead of the len() function
                # which could potentially be wrapped in an lru_cache itself.
                self.full = self.cache.__len__() >= self.maxsize
        return evicted_key, evicted_value


class Repository(Generic[TAggregateID]):
    """Reconstructs aggregates from events in an
    :class:`~eventsourcing.persistence.EventStore`,
    possibly using snapshot store to avoid replaying
    all events.
    """

    FASTFORWARD_LOCKS_CACHE_MAXSIZE = 50

    def __init__(
        self,
        event_store: EventStore[TAggregateID],
        *,
        snapshot_store: EventStore[TAggregateID] | None = None,
        cache_maxsize: int | None = None,
        fastforward: bool = True,
        fastforward_skipping: bool = False,
        deepcopy_from_cache: bool = True,
    ):
        """Initialises repository with given event store (an
        :class:`~eventsourcing.persistence.EventStore` for aggregate
        :class:`~eventsourcing.domain.AggregateEvent` objects)
        and optionally a snapshot store (an
        :class:`~eventsourcing.persistence.EventStore` for aggregate
        :class:`~eventsourcing.domain.Snapshot` objects).
        """
        self.event_store: EventStore[TAggregateID] = event_store
        self.snapshot_store: EventStore[TAggregateID] | None = snapshot_store

        if cache_maxsize is None:
            self.cache: (
                Cache[TAggregateID, MutableOrImmutableAggregate[TAggregateID]] | None
            ) = None
        elif cache_maxsize <= 0:
            self.cache = Cache()
        else:
            self.cache = LRUCache(maxsize=cache_maxsize)
        self.fastforward = fastforward
        self.fastforward_skipping = fastforward_skipping
        self.deepcopy_from_cache = deepcopy_from_cache

        # Because fast-forwarding a cached aggregate isn't thread-safe.
        self._fastforward_locks_lock = Lock()
        self._fastforward_locks_cache: LRUCache[TAggregateID, Lock] = LRUCache(
            maxsize=self.FASTFORWARD_LOCKS_CACHE_MAXSIZE
        )
        self._fastforward_locks_inuse: dict[TAggregateID, tuple[Lock, int]] = {}

    def get(
        self,
        aggregate_id: TAggregateID,
        *,
        version: int | None = None,
        projector_func: ProjectorFunction[
            TMutableOrImmutableAggregate, TDomainEvent
        ] = project_aggregate,
        fastforward_skipping: bool = False,
        deepcopy_from_cache: bool = True,
    ) -> TMutableOrImmutableAggregate:
        """Reconstructs an :class:`~eventsourcing.domain.Aggregate` for a
        given ID from stored events, optionally at a particular version.
        """
        if self.cache and version is None:
            try:
                # Look for aggregate in the cache.
                aggregate = cast(
                    "TMutableOrImmutableAggregate", self.cache.get(aggregate_id)
                )
            except KeyError:
                # Reconstruct aggregate from stored events.
                aggregate = self._reconstruct_aggregate(
                    aggregate_id, None, projector_func
                )
                # Put aggregate in the cache.
                self.cache.put(aggregate_id, aggregate)
            else:
                if self.fastforward:
                    # Fast-forward cached aggregate.
                    fastforward_lock = self._use_fastforward_lock(aggregate_id)
                    # TODO: Should this be 'fastforward or self.fastforward_skipping'?
                    blocking = not (fastforward_skipping or self.fastforward_skipping)
                    try:
                        if fastforward_lock.acquire(blocking=blocking):
                            try:
                                new_events = self.event_store.get(
                                    originator_id=aggregate_id, gt=aggregate.version
                                )
                                _aggregate = projector_func(
                                    aggregate,
                                    cast(
                                        "Iterable[TDomainEvent]",
                                        new_events,
                                    ),
                                )
                                if _aggregate is None:
                                    raise AggregateNotFoundError(aggregate_id)
                                aggregate = _aggregate
                            finally:
                                fastforward_lock.release()
                    finally:
                        self._disuse_fastforward_lock(aggregate_id)

            # Copy mutable aggregates for commands, so bad mutations don't corrupt.
            if deepcopy_from_cache and self.deepcopy_from_cache:
                aggregate = deepcopy(aggregate)
        else:
            # Reconstruct historical version of aggregate from stored events.
            aggregate = self._reconstruct_aggregate(
                aggregate_id, version, projector_func
            )
        return aggregate

    def _reconstruct_aggregate(
        self,
        aggregate_id: TAggregateID,
        version: int | None,
        projector_func: ProjectorFunction[TMutableOrImmutableAggregate, TDomainEvent],
    ) -> TMutableOrImmutableAggregate:
        gt: int | None = None

        if self.snapshot_store is not None:
            # Try to get a snapshot.
            snapshots = list(
                self.snapshot_store.get(
                    originator_id=aggregate_id,
                    desc=True,
                    limit=1,
                    lte=version,
                ),
            )
            if snapshots:
                gt = snapshots[0].originator_version
        else:
            snapshots = []

        # Get aggregate events.
        aggregate_events = self.event_store.get(
            originator_id=aggregate_id,
            gt=gt,
            lte=version,
        )

        # Reconstruct the aggregate from its events.
        initial: TMutableOrImmutableAggregate | None = None
        aggregate = projector_func(
            initial,
            chain(
                cast("Iterable[TDomainEvent]", snapshots),
                cast("Iterable[TDomainEvent]", aggregate_events),
            ),
        )

        # Raise exception if "not found".
        if aggregate is None:
            msg = f"Aggregate {aggregate_id!r} version {version!r} not found."
            raise AggregateNotFoundError(msg)
        # Return the aggregate.
        return aggregate

    def _use_fastforward_lock(self, aggregate_id: TAggregateID) -> Lock:
        lock: Lock | None = None
        with self._fastforward_locks_lock:
            num_users = 0
            with contextlib.suppress(KeyError):
                lock, num_users = self._fastforward_locks_inuse[aggregate_id]
            if lock is None:
                with contextlib.suppress(KeyError):
                    lock = self._fastforward_locks_cache.get(aggregate_id, evict=True)
            if lock is None:
                lock = Lock()
            num_users += 1
            self._fastforward_locks_inuse[aggregate_id] = (lock, num_users)
            return lock

    def _disuse_fastforward_lock(self, aggregate_id: TAggregateID) -> None:
        with self._fastforward_locks_lock:
            lock_, num_users = self._fastforward_locks_inuse[aggregate_id]
            num_users -= 1
            if num_users == 0:
                del self._fastforward_locks_inuse[aggregate_id]
                self._fastforward_locks_cache.put(aggregate_id, lock_)
            else:
                self._fastforward_locks_inuse[aggregate_id] = (lock_, num_users)

    def __contains__(self, item: TAggregateID) -> bool:
        """Tests to see if an aggregate exists in the repository."""
        try:
            self.get(aggregate_id=item)
        except AggregateNotFoundError:
            return False
        else:
            return True


@dataclass(frozen=True)
class Section:
    """Frozen dataclass that represents a section from a :class:`NotificationLog`.
    The :data:`items` attribute contains a list of
    :class:`~eventsourcing.persistence.Notification` objects.
    The :data:`id` attribute is the section ID, two integers
    separated by a comma that described the first and last
    notification ID that are included in the section.
    The :data:`next_id` attribute describes the section ID
    of the next section, and will be set if the section contains
    as many notifications as were requested.

    Constructor arguments:

    :param Optional[str] id: section ID of this section e.g. "1,10"
    :param list[Notification] items: a list of event notifications
    :param Optional[str] next_id: section ID of the following section
    """

    id: str | None
    items: Sequence[Notification]
    next_id: str | None


class NotificationLog(ABC):
    """Abstract base class for notification logs."""

    @abstractmethod
    def __getitem__(self, section_id: str) -> Section:
        """Returns a :class:`Section` of
        :class:`~eventsourcing.persistence.Notification` objects
        from the notification log.
        """

    @abstractmethod
    def select(
        self,
        start: int | None,
        limit: int,
        stop: int | None = None,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> Sequence[Notification]:
        """Returns a selection of
        :class:`~eventsourcing.persistence.Notification` objects
        from the notification log.
        """


class LocalNotificationLog(NotificationLog):
    """Notification log that presents sections of event notifications
    retrieved from an :class:`~eventsourcing.persistence.ApplicationRecorder`.
    """

    DEFAULT_SECTION_SIZE = 10

    def __init__(
        self,
        recorder: ApplicationRecorder,
        section_size: int = DEFAULT_SECTION_SIZE,
    ):
        """Initialises a local notification object with given
        :class:`~eventsourcing.persistence.ApplicationRecorder`
        and an optional section size.

        Constructor arguments:

        :param ApplicationRecorder recorder: application recorder from which event
            notifications will be selected
        :param int section_size: number of notifications to include in a section

        """
        self.recorder = recorder
        self.section_size = section_size

    def __getitem__(self, requested_section_id: str) -> Section:
        """Returns a :class:`Section` of event notifications
        based on the requested section ID. The section ID of
        the returned section will describe the event
        notifications that are actually contained in
        the returned section, and may vary from the
        requested section ID if there are fewer notifications
        in the recorder than were requested, or if there
        are gaps in the sequence of recorded event notification.
        """
        # Interpret the section ID.
        parts = requested_section_id.split(",")
        part1 = int(parts[0])
        part2 = int(parts[1])
        start = max(1, part1)
        limit = min(max(0, part2 - start + 1), self.section_size)

        # Select notifications.
        notifications = self.select(start, limit)

        # Get next section ID.
        actual_section_id: str | None
        next_id: str | None
        if len(notifications):
            last_notification_id = notifications[-1].id
            actual_section_id = self.format_section_id(
                notifications[0].id, last_notification_id
            )
            if len(notifications) == limit:
                next_id = self.format_section_id(
                    last_notification_id + 1, last_notification_id + limit
                )
            else:
                next_id = None
        else:
            actual_section_id = None
            next_id = None

        # Return a section of the notification log.
        return Section(
            id=actual_section_id,
            items=notifications,
            next_id=next_id,
        )

    def select(
        self,
        start: int | None,
        limit: int,
        stop: int | None = None,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> Sequence[Notification]:
        """Returns a selection of
        :class:`~eventsourcing.persistence.Notification` objects
        from the notification log.
        """
        if limit > self.section_size:
            msg = (
                f"Requested limit {limit} greater than section size {self.section_size}"
            )
            raise ValueError(msg)
        return self.recorder.select_notifications(
            start=start,
            limit=limit,
            stop=stop,
            topics=topics,
            inclusive_of_start=inclusive_of_start,
        )

    @staticmethod
    def format_section_id(first_id: int, last_id: int) -> str:
        return f"{first_id},{last_id}"


class ProcessingEvent(Generic[TAggregateID]):
    """Keeps together a :class:`~eventsourcing.persistence.Tracking`
    object, which represents the position of a domain event notification
    in the notification log of a particular application, and the
    new domain events that result from processing that notification.
    """

    def __init__(self, tracking: Tracking | None = None):
        """Initialises the process event with the given tracking object."""
        self.tracking = tracking
        self.events: list[DomainEventProtocol[TAggregateID]] = []
        self.aggregates: dict[
            TAggregateID, MutableOrImmutableAggregate[TAggregateID]
        ] = {}
        self.saved_kwargs: dict[Any, Any] = {}

    def collect_events(
        self,
        *objs: MutableOrImmutableAggregate[TAggregateID]
        | DomainEventProtocol[TAggregateID]
        | None,
        **kwargs: Any,
    ) -> None:
        """Collects pending domain events from the given aggregate."""
        for obj in objs:
            if obj is None:
                continue
            if isinstance(obj, DomainEventProtocol):
                self.events.append(obj)
            else:
                if isinstance(obj, CollectEventsProtocol):
                    for event in obj.collect_events():
                        self.events.append(event)
                self.aggregates[obj.id] = obj

        self.saved_kwargs.update(kwargs)

    def save(
        self,
        *aggregates: MutableOrImmutableAggregate[TAggregateID]
        | DomainEventProtocol[TAggregateID]
        | None,
        **kwargs: Any,
    ) -> None:
        warn(
            "'save()' is deprecated, use 'collect_events()' instead",
            DeprecationWarning,
            stacklevel=2,
        )

        self.collect_events(*aggregates, **kwargs)


class Application(Generic[TAggregateID]):
    """Base class for event-sourced applications."""

    name = "Application"
    env: ClassVar[dict[str, str]] = {}
    is_snapshotting_enabled: bool = False
    snapshotting_intervals: ClassVar[
        dict[type[MutableOrImmutableAggregate[Any]], int]
    ] = {}
    snapshotting_projectors: ClassVar[
        dict[
            type[MutableOrImmutableAggregate[Any]],
            ProjectorFunction[Any, Any],
        ]
    ] = {}
    snapshot_class: type[SnapshotProtocol[TAggregateID]] | None = None
    log_section_size = 10
    notify_topics: Sequence[str] = []

    AGGREGATE_CACHE_MAXSIZE = "AGGREGATE_CACHE_MAXSIZE"
    AGGREGATE_CACHE_FASTFORWARD = "AGGREGATE_CACHE_FASTFORWARD"
    AGGREGATE_CACHE_FASTFORWARD_SKIPPING = "AGGREGATE_CACHE_FASTFORWARD_SKIPPING"
    DEEPCOPY_FROM_AGGREGATE_CACHE = "DEEPCOPY_FROM_AGGREGATE_CACHE"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        if "name" not in cls.__dict__:
            cls.name = cls.__name__

    def __init__(self, env: EnvType | None = None) -> None:
        """Initialises an application with an
        :class:`~eventsourcing.persistence.InfrastructureFactory`,
        a :class:`~eventsourcing.persistence.Mapper`,
        an :class:`~eventsourcing.persistence.ApplicationRecorder`,
        an :class:`~eventsourcing.persistence.EventStore`,
        a :class:`~eventsourcing.application.Repository`, and
        a :class:`~eventsourcing.application.LocalNotificationLog`.
        """
        self.closing = Event()
        self.env = self.construct_env(self.name, env)  # type: ignore[misc]
        self.factory = self.construct_factory(self.env)
        self.mapper: Mapper[TAggregateID] = self.construct_mapper()
        self.recorder = self.construct_recorder()
        self.events: EventStore[TAggregateID] = self.construct_event_store()
        self.snapshots: EventStore[TAggregateID] | None = None
        if self.factory.is_snapshotting_enabled():
            self.snapshots = self.construct_snapshot_store()
        self._repository: Repository[TAggregateID] = self.construct_repository()
        self._notification_log = self.construct_notification_log()

    @property
    def repository(self) -> Repository[TAggregateID]:
        """An application's repository reconstructs aggregates from stored events."""
        return self._repository

    @property
    def notification_log(self) -> LocalNotificationLog:
        """An application's notification log presents all the aggregate events
        of an application in the order they were recorded as a sequence of event
        notifications.
        """
        return self._notification_log

    @property
    def log(self) -> LocalNotificationLog:
        warn(
            "'log' is deprecated, use 'notification_log' instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._notification_log

    def construct_env(self, name: str, env: EnvType | None = None) -> Environment:
        """Constructs environment from which application will be configured."""
        _env = dict(type(self).env)
        if type(self).is_snapshotting_enabled or type(self).snapshotting_intervals:
            _env["IS_SNAPSHOTTING_ENABLED"] = "y"
        _env.update(os.environ)
        if env is not None:
            _env.update(env)
        return Environment(name, _env)

    def construct_factory(self, env: Environment) -> InfrastructureFactory:
        """Constructs an :class:`~eventsourcing.persistence.InfrastructureFactory`
        for use by the application.
        """
        return InfrastructureFactory.construct(env)

    def construct_mapper(self) -> Mapper[TAggregateID]:
        """Constructs a :class:`~eventsourcing.persistence.Mapper`
        for use by the application.
        """
        return self.factory.mapper(transcoder=self.construct_transcoder())

    def construct_transcoder(self) -> Transcoder:
        """Constructs a :class:`~eventsourcing.persistence.Transcoder`
        for use by the application.
        """
        transcoder = self.factory.transcoder()
        if isinstance(transcoder, JSONTranscoder):
            self.register_transcodings(transcoder)
        return transcoder

    def register_transcodings(self, transcoder: JSONTranscoder) -> None:
        """Registers :class:`~eventsourcing.persistence.Transcoding`
        objects on given :class:`~eventsourcing.persistence.JSONTranscoder`.
        """
        transcoder.register(UUIDAsHex())
        transcoder.register(DecimalAsStr())
        transcoder.register(DatetimeAsISO())

    def construct_recorder(self) -> ApplicationRecorder:
        """Constructs an :class:`~eventsourcing.persistence.ApplicationRecorder`
        for use by the application.
        """
        return self.factory.application_recorder()

    def construct_event_store(self) -> EventStore[TAggregateID]:
        """Constructs an :class:`~eventsourcing.persistence.EventStore`
        for use by the application to store and retrieve aggregate
        :class:`~eventsourcing.domain.AggregateEvent` objects.
        """
        return self.factory.event_store(
            mapper=self.mapper,
            recorder=self.recorder,
        )

    def construct_snapshot_store(self) -> EventStore[TAggregateID]:
        """Constructs an :py:class:`~eventsourcing.persistence.EventStore`
        for use by the application to store and retrieve aggregate
        :class:`~eventsourcing.domain.Snapshot` objects.
        """
        recorder = self.factory.aggregate_recorder(purpose="snapshots")
        return self.factory.event_store(
            mapper=self.mapper,
            recorder=recorder,
        )

    def construct_repository(self) -> Repository[TAggregateID]:
        """Constructs a :py:class:`Repository` for use by the application."""
        cache_maxsize_envvar = self.env.get(self.AGGREGATE_CACHE_MAXSIZE)
        cache_maxsize = int(cache_maxsize_envvar) if cache_maxsize_envvar else None
        return Repository(
            event_store=self.events,
            snapshot_store=self.snapshots,
            cache_maxsize=cache_maxsize,
            fastforward=strtobool(self.env.get(self.AGGREGATE_CACHE_FASTFORWARD, "y")),
            fastforward_skipping=strtobool(
                self.env.get(self.AGGREGATE_CACHE_FASTFORWARD_SKIPPING, "n")
            ),
            deepcopy_from_cache=strtobool(
                self.env.get(self.DEEPCOPY_FROM_AGGREGATE_CACHE, "y")
            ),
        )

    def construct_notification_log(self) -> LocalNotificationLog:
        """Constructs a :class:`LocalNotificationLog` for use by the application."""
        return LocalNotificationLog(self.recorder, section_size=self.log_section_size)

    def save(
        self,
        *objs: MutableOrImmutableAggregate[TAggregateID]
        | DomainEventProtocol[TAggregateID]
        | None,
        **kwargs: Any,
    ) -> list[Recording[TAggregateID]]:
        """Collects pending events from given aggregates and
        puts them in the application's event store.
        """
        processing_event: ProcessingEvent[TAggregateID] = ProcessingEvent()
        processing_event.collect_events(*objs, **kwargs)
        recordings = self._record(processing_event)
        self._take_snapshots(processing_event)
        self._notify(recordings)
        self.notify(processing_event.events)  # Deprecated.
        return recordings

    def _record(
        self, processing_event: ProcessingEvent[TAggregateID]
    ) -> list[Recording[TAggregateID]]:
        """Records given process event in the application's recorder."""
        recordings = self.events.put(
            processing_event.events,
            tracking=processing_event.tracking,
            **processing_event.saved_kwargs,
        )
        if self.repository.cache and not self.repository.fastforward:
            for aggregate_id, aggregate in processing_event.aggregates.items():
                self.repository.cache.put(aggregate_id, aggregate)
        return recordings

    def _take_snapshots(self, processing_event: ProcessingEvent[TAggregateID]) -> None:
        # Take snapshots using IDs and types.
        if self.snapshots and self.snapshotting_intervals:
            for event in processing_event.events:
                try:
                    aggregate = processing_event.aggregates[event.originator_id]
                except KeyError:
                    continue
                interval = self.snapshotting_intervals.get(type(aggregate))
                if interval is not None and event.originator_version % interval == 0:
                    try:
                        projector_func = self.snapshotting_projectors[type(aggregate)]
                    except KeyError:
                        if not isinstance(event, CanMutateProtocol):
                            msg = (
                                f"Cannot take snapshot for {type(aggregate)} with "
                                "default project_aggregate() function, because its "
                                f"domain event {type(event)} does not implement "
                                "the 'can mutate' protocol (see CanMutateProtocol)."
                                f" Please define application class {type(self)}"
                                " with class variable 'snapshotting_projectors', "
                                f"to be a dict that has {type(aggregate)} as a key "
                                "with the aggregate projector function for "
                                f"{type(aggregate)} as the value for that key."
                            )
                            raise ProgrammingError(msg) from None

                        projector_func = project_aggregate
                    self.take_snapshot(
                        aggregate_id=event.originator_id,
                        version=event.originator_version,
                        projector_func=projector_func,
                    )

    def take_snapshot(
        self,
        aggregate_id: TAggregateID,
        version: int | None = None,
        projector_func: ProjectorFunction[Any, Any] = project_aggregate,
    ) -> None:
        """Takes a snapshot of the recorded state of the aggregate,
        and puts the snapshot in the snapshot store.
        """
        if self.snapshots is None:
            msg = (
                "Can't take snapshot without snapshots store. Please "
                "set environment variable IS_SNAPSHOTTING_ENABLED to "
                "a true value (e.g. 'y'), or set 'is_snapshotting_enabled' "
                "on application class, or set 'snapshotting_intervals' on "
                "application class."
            )
            raise AssertionError(msg)
        aggregate: BaseAggregate[TAggregateID] = self.repository.get(
            aggregate_id, version=version, projector_func=projector_func
        )
        snapshot_class = getattr(type(aggregate), "Snapshot", type(self).snapshot_class)
        if snapshot_class is None:
            msg = (
                "Neither application nor aggregate have a snapshot class. "
                f"Please either define a nested 'Snapshot' class on {type(aggregate)} "
                f"or set class attribute 'snapshot_class' on {type(self)}."
            )
            raise AssertionError(msg)

        snapshot = snapshot_class.take(aggregate)
        self.snapshots.put([snapshot])

    def notify(self, new_events: list[DomainEventProtocol[TAggregateID]]) -> None:
        """Deprecated.

        Called after new aggregate events have been saved. This
        method on this class doesn't actually do anything,
        but this method may be implemented by subclasses that
        need to take action when new domain events have been saved.
        """

    def _notify(self, recordings: list[Recording[TAggregateID]]) -> None:
        """Called after new aggregate events have been saved. This
        method on this class doesn't actually do anything,
        but this method may be implemented by subclasses that
        need to take action when new domain events have been saved.
        """

    def close(self) -> None:
        self.closing.set()
        self.factory.close()

    def __enter__(self) -> Self:
        self.factory.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
        self.factory.__exit__(exc_type, exc_val, exc_tb)

    def __del__(self) -> None:
        with contextlib.suppress(AttributeError):
            self.close()


TApplication = TypeVar("TApplication", bound=Application[Any])


class AggregateNotFoundError(EventSourcingError):
    """Raised when an :class:`~eventsourcing.domain.Aggregate`
    object is not found in a :class:`Repository`.
    """


class EventSourcedLog(Generic[TDomainEvent]):
    """Constructs a sequence of domain events, like an aggregate.
    But unlike an aggregate the events can be triggered
    and selected for use in an application without
    reconstructing a current state from all the events.

    This allows an indefinitely long sequence of events to be
    generated and used without the practical restrictions of
    projecting the events into a current state before they
    can be used, which is useful e.g. for logging and
    progressively discovering all the aggregate IDs of a
    particular type in an application.
    """

    def __init__(
        self,
        events: EventStore[Any],
        originator_id: UUID,
        logged_cls: type[TDomainEvent],  # TODO: Rename to 'event_class' in v10.
    ):
        self.events = events
        self.originator_id = originator_id
        self.logged_cls = logged_cls  # TODO: Rename to 'event_class' in v10.

    def trigger_event(
        self,
        next_originator_version: int | None = None,
        **kwargs: Any,
    ) -> TDomainEvent:
        """Constructs and returns a new log event."""
        return self._trigger_event(
            logged_cls=self.logged_cls,
            next_originator_version=next_originator_version,
            **kwargs,
        )

    def _trigger_event(
        self,
        logged_cls: type[SDomainEvent],
        next_originator_version: int | None = None,
        **kwargs: Any,
    ) -> SDomainEvent:
        """Constructs and returns a new log event."""
        if next_originator_version is None:
            last_logged = self.get_last()
            if last_logged is None:
                next_originator_version = Aggregate.INITIAL_VERSION
            else:
                next_originator_version = last_logged.originator_version + 1

        return logged_cls(
            originator_id=self.originator_id,
            originator_version=next_originator_version,
            timestamp=datetime_now_with_tzinfo(),
            **kwargs,
        )

    def get_first(self) -> TDomainEvent | None:
        """Selects the first logged event."""
        try:
            return next(self.get(limit=1))
        except StopIteration:
            return None

    def get_last(self) -> TDomainEvent | None:
        """Selects the last logged event."""
        try:
            return next(self.get(desc=True, limit=1))
        except StopIteration:
            return None

    def get(
        self,
        *,
        gt: int | None = None,
        lte: int | None = None,
        desc: bool = False,
        limit: int | None = None,
    ) -> Iterator[TDomainEvent]:
        """Selects a range of logged events with limit,
        with ascending or descending order.
        """
        return cast(
            "Iterator[TDomainEvent]",
            self.events.get(
                originator_id=self.originator_id,
                gt=gt,
                lte=lte,
                desc=desc,
                limit=limit,
            ),
        )
