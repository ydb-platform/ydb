__all__ = [
    'AssignmentsObserver',
    'BaseObserver',
    'PoolStatusObserver',
]

import asyncio
import attr
import datetime
import inspect
import logging

from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple, Union

from ..client.primitives.base import autocast_to_enum
from ..client.assignment import Assignment
from ..client.pool import Pool
from ..util.async_utils import AsyncInterfaceWrapper, ComplexException, ensure_async, get_task_traceback
from ..util._managing_headers import add_headers
from .cursor import AssignmentCursor, TolokaClientSyncOrAsyncType, DEFAULT_LAG
from .event import AssignmentEvent

logger = logging.getLogger(__name__)


@attr.s
class BaseObserver:
    name: Optional[str] = attr.ib(default=None, kw_only=True)
    _enabled: bool = attr.ib(default=True, init=False)
    _deleted: bool = attr.ib(default=False, init=False)

    def get_unique_key(self) -> Tuple:
        """This method should return identifier for this observer that is unique in the current pipeline context.
        """

        return (self.__class__.__name__, self.name or '')

    def inject(self, injection: Any) -> None:
        raise NotImplementedError

    async def __call__(self) -> None:
        raise NotImplementedError

    async def should_resume(self) -> bool:
        return False

    def delete(self) -> None:
        """Schedule observer to be removed from the pipeline."""
        self._deleted = True

    def disable(self) -> None:
        """Prevent observer from being called."""
        self._enabled = False

    def enable(self) -> None:
        """Enable observer to be called during pipeline execution."""
        self._enabled = True

    @add_headers('streaming')
    async def run(self, period: datetime.timedelta = datetime.timedelta(seconds=60)) -> None:
        """For standalone usage (out of a Pipeline)."""
        while True:
            await self.__call__()
            if await self.should_resume():
                logger.debug('Sleep for %d seconds', period.total_seconds())
                await asyncio.sleep(period.total_seconds())
            else:
                return


def _wrap_client_to_async_converter(client: TolokaClientSyncOrAsyncType):
    """Simple converter that is needed to specify annotation"""
    return AsyncInterfaceWrapper(client)


def _unwrap_callback(callback: Callable) -> Callable:
    wrapped = getattr(callback, '__wrapped__', None)
    if wrapped:
        return _unwrap_callback(wrapped)
    return callback


def _get_callback_unique_key(callback: Callable) -> str:
    callback = _unwrap_callback(callback)
    name = getattr(callback, '__name__', None)
    if name:
        return name
    return type(callback).__name__


def _get_callbacks_unique_key(callbacks: Iterable[Callable]) -> Tuple[str]:
    return tuple(sorted(map(_get_callback_unique_key, callbacks)))


def _inject_callback(callback_initial: Callable, injection_loaded: Callable) -> Callable:
    """Inject state got from the storage (injection) to the given callback.

    By default, injection should be done as follows;
        * If the callback is a class instance,
            it's __dict__ should be updated with the injection's one.
        * It the callback is a function,
            it should be entirely replaced by the new one.
    """

    # Both exising callback and the injection may be wrapped, so we need to unwrap them first.
    callback = _unwrap_callback(callback_initial)
    injection = _unwrap_callback(injection_loaded)

    # You may define `inject(self, injection: YourCallbackType) -> None` in your callback class to manage this process.
    if getattr(callback, 'inject', None):
        callback.inject(injection)
    elif getattr(callback, '__setstate__', None) and getattr(injection, '__getstate__', None):
        callback.__setstate__(injection.__getstate__())
    elif getattr(callback, '__call__', None) and not inspect.isfunction(callback):
        callback.__dict__.update(injection.__dict__)
    else:
        return injection_loaded
    return callback_initial


def _inject_callbacks(callbacks: Iterable[Callable], injections: Iterable[Callable]) -> List[Callable]:
    injection_by_key = {_get_callback_unique_key(injection): injection for injection in injections}
    return [_inject_callback(callback, injection_by_key[_get_callback_unique_key(callback)])
            for callback in callbacks]


@attr.s
class BasePoolObserver(BaseObserver):

    toloka_client: AsyncInterfaceWrapper[TolokaClientSyncOrAsyncType] = attr.ib(converter=_wrap_client_to_async_converter)
    pool_id: str = attr.ib()

    def get_unique_key(self) -> Tuple:
        return super().get_unique_key() + (self.pool_id,)

    @add_headers('streaming')
    async def should_resume(self) -> bool:
        logger.info('Check resume by pool status: %s', self.pool_id)
        pool = await self.toloka_client.get_pool(self.pool_id)
        logger.info('Pool status for %s: %s', self.pool_id, pool.status)
        if pool.is_open():
            return True

        logger.info('Check resume by pool active assignments: %s', self.pool_id)
        response = await self.toloka_client.find_assignments(pool_id=self.pool_id, status=[Assignment.ACTIVE], limit=1)
        logger.info('Pool %s has active assignments: %s', self.pool_id, bool(response.items))
        return bool(response.items)


CallbackForPoolSyncType = Callable[[Pool], None]
CallbackForPoolAsyncType = Callable[[Pool], Awaitable[None]]
CallbackForPoolType = Union[CallbackForPoolSyncType, CallbackForPoolAsyncType]


@attr.s
class PoolStatusObserver(BasePoolObserver):
    """Observer for pool status change.
    For usage with Pipeline.

    Allow to register callbacks using the following methods:
        * on_open
        * on_closed
        * on_archieved
        * on_locked
        * on_status_change

    The Pool object will be passed to the triggered callbacks.

    Attributes:
        toloka_client: TolokaClient instance or async wrapper around it.
        pool_id: Pool ID.

    Examples:
        Bind to the pool's close to make some aggregations.

        >>> def call_this_on_close(pool: Pool) -> None:
        >>>     assignments = client.get_assignments_df(pool_id=pool.id, status=['APPROVED'])
        >>>     do_some_aggregation(assignments)
        >>>
        >>> observer = PoolStatusObserver(toloka_client, pool_id='123')
        >>> observer.on_close(call_this_on_close)
        ...

        Call something at any status change.

        >>> observer.on_status_change(lambda pool: ...)
        ...
    """

    _callbacks: Dict[Pool.Status, List[CallbackForPoolAsyncType]] = attr.ib(factory=dict, init=False)
    _previous_status: Optional[Pool.Status] = attr.ib(default=None, init=False)

    def get_unique_key(self) -> Tuple:
        return super().get_unique_key() + tuple(sorted(
            (status.value, _get_callbacks_unique_key(callbacks))
            for status, callbacks in self._callbacks.items()
        ))

    def inject(self, injection: 'PoolStatusObserver') -> None:
        injection_callbacks_by_status = {
            status: callbacks
            for status, callbacks in injection._callbacks.items()
        }
        self._callbacks = {
            status: _inject_callbacks(self._callbacks[status], injection_callbacks_by_status[status])
            for status, callbacks in self._callbacks.items()
        }

    @autocast_to_enum
    def register_callback(
        self,
        callback: CallbackForPoolType,
        changed_to: Pool.Status
    ) -> CallbackForPoolType:
        """Register given callable for pool status change to given value.

        Args:
            callback: Sync or async callable that pass Pool object.
            changed_to: Pool status value to register for.

        Returns:
            The same callable passed as callback.
        """

        self._callbacks.setdefault(changed_to, []).append(ensure_async(callback))
        return callback

    def on_open(self, callback: CallbackForPoolType) -> CallbackForPoolType:
        return self.register_callback(callback, Pool.Status.OPEN)

    def on_closed(self, callback: CallbackForPoolType) -> CallbackForPoolType:
        return self.register_callback(callback, Pool.Status.CLOSED)

    def on_archieved(self, callback: CallbackForPoolType) -> CallbackForPoolType:
        return self.register_callback(callback, Pool.Status.ARCHIEVED)

    def on_locked(self, callback: CallbackForPoolType) -> CallbackForPoolType:
        return self.register_callback(callback, Pool.Status.LOCKED)

    def on_status_change(self, callback: CallbackForPoolType) -> CallbackForPoolType:
        for status in Pool.Status.__members__:
            self.register_callback(callback, status)
        return callback

    @add_headers('streaming')
    async def __call__(self) -> None:
        if not self._callbacks:
            return

        pool = await self.toloka_client.get_pool(self.pool_id)
        current_status = pool.status

        if current_status != self._previous_status:
            logger.info('Pool %s status change: %s -> %s', self.pool_id, self._previous_status, current_status)
            if self._callbacks.get(current_status):
                loop = asyncio.get_event_loop()
                done, _ = await asyncio.wait([loop.create_task(callback(pool))
                                              for callback in self._callbacks[current_status]])
                errored = [task for task in done if task.exception() is not None]
                if errored:
                    for task in errored:
                        logger.error('Got error while handling pool %s status change to: %s\n%s',
                                     self.pool_id, current_status, get_task_traceback(task))
                    raise ComplexException([task.exception() for task in errored])

        self._previous_status = current_status


CallbackForAssignmentEventsSyncType = Callable[[List[AssignmentEvent]], None]
CallbackForAssignmentEventsAsyncType = Callable[[List[AssignmentEvent]], Awaitable[None]]
CallbackForAssignmentEventsType = Union[CallbackForAssignmentEventsSyncType, CallbackForAssignmentEventsAsyncType]


@attr.s
class _CallbacksCursorConsumer:
    """Store cursor and related callbacks.
    Allow to run callbacks at fetched data and move the cursor in case of success.
    """
    cursor: AssignmentCursor = attr.ib()
    callbacks: List[CallbackForAssignmentEventsAsyncType] = attr.ib(factory=list, init=False)

    def get_unique_key(self) -> Tuple:
        return self.cursor._event_type.value, _get_callbacks_unique_key(self.callbacks)

    def inject(self, injection: '_CallbacksCursorConsumer') -> None:
        self.cursor.inject(injection.cursor)
        self.callbacks = _inject_callbacks(self.callbacks, injection.callbacks)

    def add_callback(self, callback: CallbackForAssignmentEventsType) -> None:
        self.callbacks.append(ensure_async(callback))

    @add_headers('streaming')
    async def __call__(self, pool_id: str) -> None:
        async with self.cursor.try_fetch_all() as fetched:
            if not fetched:
                return

            logger.info('Got pool %s events count of type %s: %d', pool_id, fetched[0].event_type, len(fetched))
            loop = asyncio.get_event_loop()
            callback_by_task = {loop.create_task(callback(fetched)): callback
                                for callback in self.callbacks}
            done, _ = await asyncio.wait(callback_by_task)
            errored = [task for task in done if task.exception() is not None]
            if errored:
                for task in errored:
                    logger.error('Got error in callback: %s\n%s', callback_by_task[task], get_task_traceback(task))
                raise ComplexException([task.exception() for task in errored])


@attr.s
class AssignmentsObserver(BasePoolObserver):
    """Observer for the pool's assignment events.
    For usage with Pipeline.

    Allow to register callbacks using the following methods:
        * on_created
        * on_submitted
        * on_accepted
        * on_rejected
        * on_skipped
        * on_expired

    Corresponding assignment events will be passed to the triggered callbacks.

    Attributes:
        toloka_client: TolokaClient instance or async wrapper around it.
        pool_id: Pool ID.
        cursor_time_lag: Time lag for cursor. This controls time lag between assignments being added and them being
            seen by this observer. See BaseCursor.time_lag for details and reasoning behind this.

    Examples:
        Send submitted assignments for verification.

        >>> def handle_submitted(evets: List[AssignmentEvent]) -> None:
        >>>     verification_tasks = [create_veridication_task(item.assignment) for item in evets]
        >>>     toloka_client.create_tasks(verification_tasks, open_pool=True)
        >>>
        >>> observer = AssignmentsObserver(toloka_client, pool_id='123')
        >>> observer.on_submitted(handle_submitted)
        ...
    """

    cursor_time_lag: datetime.timedelta = attr.ib(default=DEFAULT_LAG)
    _callbacks: Dict[AssignmentEvent.Type, _CallbacksCursorConsumer] = attr.ib(factory=dict, init=False)

    def get_unique_key(self) -> Tuple:
        return super().get_unique_key() + tuple(sorted(
            consumer.get_unique_key()
            for consumer in self._callbacks.values()
        ))

    def inject(self, injection: 'AssignmentsObserver') -> None:
        injection_consumer_by_key = {
            consumer.get_unique_key(): consumer
            for consumer in injection._callbacks.values()
        }
        for consumer in self._callbacks.values():
            key = consumer.get_unique_key()
            consumer.inject(injection_consumer_by_key[key])

    # Setup section.

    @autocast_to_enum
    def register_callback(
        self,
        callback: CallbackForAssignmentEventsType,
        event_type: AssignmentEvent.Type,
    ) -> CallbackForAssignmentEventsType:
        """Register given callable for given event type.
        Callback will be called multiple times if it has been registered for multiple event types.

        Args:
            callback: Sync or async callable that pass List[AssignmentEvent] of desired event type.
            event_type: Selected event type.

        Returns:
            The same callable passed as callback.
        """
        if event_type not in self._callbacks:
            cursor = AssignmentCursor(
                pool_id=self.pool_id,
                event_type=event_type,
                toloka_client=self.toloka_client,
                time_lag=self.cursor_time_lag
            )
            self._callbacks[event_type] = _CallbacksCursorConsumer(cursor)
        self._callbacks[event_type].add_callback(callback)
        return callback

    def on_any_event(self, callback: CallbackForAssignmentEventsType) -> CallbackForAssignmentEventsType:
        for event_type in AssignmentEvent.Type.__members__.values():
            self.register_callback(callback, event_type)
        return callback

    def on_created(self, callback: CallbackForAssignmentEventsType) -> CallbackForAssignmentEventsType:
        return self.register_callback(callback, AssignmentEvent.Type.CREATED)

    def on_submitted(self, callback: CallbackForAssignmentEventsType) -> CallbackForAssignmentEventsType:
        return self.register_callback(callback, AssignmentEvent.Type.SUBMITTED)

    def on_accepted(self, callback: CallbackForAssignmentEventsType) -> CallbackForAssignmentEventsType:
        return self.register_callback(callback, AssignmentEvent.Type.ACCEPTED)

    def on_rejected(self, callback: CallbackForAssignmentEventsType) -> CallbackForAssignmentEventsType:
        return self.register_callback(callback, AssignmentEvent.Type.REJECTED)

    def on_skipped(self, callback: CallbackForAssignmentEventsType) -> CallbackForAssignmentEventsType:
        return self.register_callback(callback, AssignmentEvent.Type.SKIPPED)

    def on_expired(self, callback: CallbackForAssignmentEventsType) -> CallbackForAssignmentEventsType:
        return self.register_callback(callback, AssignmentEvent.Type.EXPIRED)

    # Run section.

    @add_headers('streaming')
    async def __call__(self) -> None:
        if not self._callbacks:
            return

        loop = asyncio.get_event_loop()
        event_type_by_task = {loop.create_task(cursor_and_callbacks(self.pool_id)): event_type
                              for event_type, cursor_and_callbacks in self._callbacks.items()
                              if cursor_and_callbacks.callbacks}
        logger.info('Gathering pool %s event of types: %s', self.pool_id, list(event_type_by_task.values()))

        done, _ = await asyncio.wait(event_type_by_task)
        errored = [task for task in done if task.exception() is not None]
        if errored:
            for task in errored:
                logger.error('Got error while handling pool %s assignment events of type: %s',
                             self.pool_id, event_type_by_task[task])
            raise ComplexException([task.exception() for task in errored])
