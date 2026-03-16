__all__ = [
    'AssignmentCursor',
    'BaseCursor',
    'DATETIME_MIN',
    'MessageThreadCursor',
    'TaskCursor',
    'TolokaClientSyncOrAsyncType',
    'UserBonusCursor',
    'UserRestrictionCursor',
    'UserSkillCursor',
]

import asyncio
import attr
import copy
import functools
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Awaitable, Callable, Iterator, List, Optional, Set, Tuple, Union

from toloka.util._docstrings import inherit_docstrings
from typing_extensions import Protocol

from ..async_client import AsyncTolokaClient
from ..client import (
    Assignment,
    MessageThread,
    Task,
    TolokaClient,
    UserBonus,
    UserRestriction,
    UserSkill,
    expand,
    search_requests,
    search_results,
    structure,
)
from ..client.search_requests import BaseSearchRequest
from ..util._codegen import fix_attrs_converters
from .event import AssignmentEvent, BaseEvent, MessageThreadEvent, TaskEvent, UserBonusEvent, UserRestrictionEvent, UserSkillEvent
from ..util.async_utils import ensure_async


class ResponseObjectType(Protocol):
    items: Optional[List[Any]]
    has_more: Optional[bool]


TolokaClientSyncOrAsyncType = Union[TolokaClient, AsyncTolokaClient]

DATETIME_MIN = datetime.fromtimestamp(0, tz=timezone.utc)
DEFAULT_LAG = timedelta(minutes=1)


@attr.s
class _ByIdCursor:
    """Iterate by id only."""
    fetcher: Callable[[BaseSearchRequest], ResponseObjectType] = attr.ib()
    request: BaseSearchRequest = attr.ib()

    def __iter__(self) -> Iterator[Any]:
        while True:
            response = self.fetcher(self.request, sort='id')  # Diff between sync and async.
            if response.items:
                for item in response.items:
                    yield item
                self.request = attr.evolve(self.request, id_gt=item.id)
            if not response.has_more:
                return

    async def __aiter__(self) -> AsyncIterator[Any]:
        while True:
            response = await ensure_async(self.fetcher)(self.request, sort='id')  # Diff between sync and async.
            if response.items:
                for item in response.items:
                    yield item
                self.request = attr.evolve(self.request, id_gt=item.id)
            if not response.has_more:
                return


@attr.s
class BaseCursor:
    """Base class for all cursors.

    Args:
        _time_lag: Time lag between cursor time field upper bound and real time. Default is 1 minute. This lag is
            required to keep cursor consistent. Lowering this value will make cursor process events faster, but raises
            probability of missing some events in case of concurrent operations.
    """

    toloka_client: TolokaClientSyncOrAsyncType = attr.ib()
    _request: BaseSearchRequest = attr.ib()
    _time_lag: timedelta = attr.ib(default=DEFAULT_LAG)
    _prev_response: Optional[ResponseObjectType] = attr.ib(default=None, init=False)
    _seen_ids: Set[str] = attr.ib(factory=set, init=False)

    @attr.s
    class CursorFetchContext:
        """Context manager to return from `BaseCursor.try_fetch_all method`.
        Commit cursor state only if no error occured.
        """
        _cursor: 'BaseCursor' = attr.ib()
        _start_state: Optional[Tuple] = attr.ib(default=None, init=False)
        _finish_state: Optional[Tuple] = attr.ib(default=None, init=False)

        def __enter__(self) -> List[BaseEvent]:
            self._start_state = copy.deepcopy(self._cursor._get_state())
            res = [item for item in self._cursor]
            self._finish_state = self._cursor._get_state()
            self._cursor._set_state(self._start_state)
            return res

        async def __aenter__(self) -> List[BaseEvent]:
            self._start_state = copy.deepcopy(self._cursor._get_state())
            res = [item async for item in self._cursor]
            self._finish_state = self._cursor._get_state()
            self._cursor._set_state(self._start_state)
            return res

        def __exit__(self, exc_type, exc_value, traceback) -> None:
            if exc_type is None:
                self._cursor._set_state(self._finish_state)

        async def __aexit__(self, exc_type, exc_value, traceback) -> Awaitable[None]:
            if exc_type is None:
                self._cursor._set_state(self._finish_state)

    def _get_state(self) -> Tuple:
        return self._request, self._prev_response, self._seen_ids

    def _set_state(self, state: Tuple) -> None:
        self._request, self._prev_response, self._seen_ids = state

    def inject(self, injection: 'BaseCursor') -> None:
        self._set_state(injection._get_state())

    def try_fetch_all(self) -> CursorFetchContext:
        return self.CursorFetchContext(self)

    def __attrs_post_init__(self):
        if not getattr(self._request, self._time_field_gte):
            self._request = attr.evolve(self._request, **{self._time_field_gte: DATETIME_MIN})

    def _get_fetcher(self) -> Callable[..., ResponseObjectType]:
        """Return toloka_client method from here."""
        raise NotImplementedError

    def _get_time_field(self) -> str:
        """Getter for time field."""
        raise NotImplementedError

    def _construct_event(self, item: Any) -> BaseEvent:
        """Create event based on object."""
        raise NotImplementedError

    def _get_time(self, item: Any) -> datetime:
        return getattr(item, self._get_time_field())

    @property
    def _time_field_gte(self) -> str:
        return f'{self._get_time_field()}_gte'

    @property
    def _time_field_lte(self) -> str:
        return f'{self._get_time_field()}_lte'  # To iterate by id for fixed time.

    @property
    def _time_field_gt(self) -> str:
        return f'{self._get_time_field()}_gt'  # To use after iteration by id.

    def __iter__(self) -> Iterator[BaseEvent]:
        fetcher = self._get_fetcher()
        self._request = attr.evolve(
            self._request, **{self._time_field_lte: datetime.now(tz=timezone.utc) - self._time_lag}
        )
        while True:
            response = fetcher(self._request, sort=self._get_time_field())  # Diff between sync and async.
            if response.items:
                max_time = self._get_time(response.items[-1])
                self._prev_response = response
                for item in response.items:
                    if item.id not in self._seen_ids:
                        self._request = attr.evolve(self._request, **{self._time_field_gte: self._get_time(item)})
                        self._seen_ids.add(item.id)
                        yield self._construct_event(item)

                if not response.has_more:
                    return

                # Multiple items can have the same time field value. If items with the same time field value are split
                # between responses due to the fetcher limit they will be fetched twice and filtered by _seen_ids field
                # afterward. But there is a corner case when all items in the response have the same time field value.
                # As the result we will fetch the same items over and over again. To avoid this we need fallback to
                # iteration over id field.
                if self._get_time(response.items[0]) == max_time:
                    fixed_time_request = attr.evolve(self._request, **{self._time_field_lte: max_time})
                    for item in _ByIdCursor(fetcher, fixed_time_request):  # Diff between sync and async.
                        if item.id not in self._seen_ids:
                            self._seen_ids.add(item.id)
                            yield self._construct_event(item)
                    self._request = attr.evolve(self._request, **{self._time_field_gt: max_time})

                # Strip it to the current response size.
                self._seen_ids = {item.id for item in response.items}
            else:
                return

    async def __aiter__(self) -> AsyncIterator[BaseEvent]:
        fetcher = self._get_fetcher()
        self._request = attr.evolve(
            self._request, **{self._time_field_lte: datetime.now(tz=timezone.utc) - self._time_lag}
        )
        while True:
            response = await ensure_async(fetcher)(self._request, sort=self._get_time_field())  # Diff between sync and async.
            if response.items:
                max_time = self._get_time(response.items[-1])
                self._prev_response = response
                for item in response.items:
                    if item.id not in self._seen_ids:
                        self._request = attr.evolve(self._request, **{self._time_field_gte: self._get_time(item)})
                        self._seen_ids.add(item.id)
                        yield self._construct_event(item)

                if not response.has_more:
                    return

                # Multiple items can have the same time field value. If items with the same time field value are split
                # between responses due to the fetcher limit they will be fetched twice and filtered by _seen_ids field
                # afterward. But there is a corner case when all items in the response have the same time field value.
                # As the result we will fetch the same items over and over again. To avoid this we need fallback to
                # iteration over id field.
                if self._get_time(response.items[0]) == max_time:
                    fixed_time_request = attr.evolve(self._request, **{self._time_field_lte: max_time})
                    async for item in _ByIdCursor(fetcher, fixed_time_request):  # Diff between sync and async.
                        if item.id not in self._seen_ids:
                            self._seen_ids.add(item.id)
                            yield self._construct_event(item)
                    self._request = attr.evolve(self._request, **{self._time_field_gt: max_time})

                # Strip it to the current response size.
                self._seen_ids = {item.id for item in response.items}
            else:
                return


@expand('request')
@fix_attrs_converters
@inherit_docstrings
@attr.s
class AssignmentCursor(BaseCursor):
    """Iterator over Assignment objects of selected AssignmentEventType.

    Args:
        toloka_client: TolokaClient object that is being used to search assignments.
        request: Base request to search assignments by.
        event_type: Assignments event's type to search.

    Examples:
        Iterate over assignment acceptances events.

        >>> it = AssignmentCursor(pool_id='123', event_type='ACCEPTED', toloka_client=toloka_client)
        >>> current_events = list(it)
        >>> # ... new events may occur ...
        >>> new_events = list(it)  # Contains only new events, occured since the previous call.
        ...
    """

    BATCH_SIZE = 1000

    _event_type: AssignmentEvent.Type = attr.ib(converter=functools.partial(structure, cl=AssignmentEvent.Type))
    _request: search_requests.AssignmentSearchRequest = attr.ib(
        factory=search_requests.AssignmentSearchRequest,
    )
    _time_lag: timedelta = attr.ib(default=DEFAULT_LAG)

    def _get_fetcher(self) -> Callable[..., search_results.AssignmentSearchResult]:
        async def _async_fetcher(*args, **kwargs):
            return await self.toloka_client.find_assignments(*args, limit=self.BATCH_SIZE, **kwargs)

        method = self.toloka_client.find_assignments
        if asyncio.iscoroutinefunction(method) or asyncio.iscoroutinefunction(getattr(method, '__call__', None)):
            return _async_fetcher
        return functools.partial(self.toloka_client.find_assignments, limit=self.BATCH_SIZE)

    def _get_time_field(self) -> str:
        return self._event_type.time_key

    def _construct_event(self, item: Assignment) -> AssignmentEvent:
        return AssignmentEvent(assignment=item,
                               event_type=self._event_type,
                               event_time=getattr(item, self._event_type.time_key))


@expand('request')
@fix_attrs_converters
@inherit_docstrings
@attr.s
class TaskCursor(BaseCursor):
    """Iterator over tasks by create time.

    Args:
        toloka_client: TolokaClient object that is being used to search tasks.
        request: Base request to search tasks by.

    Examples:
        Iterate over tasks.

        >>> it = TaskCursor(pool_id='123', toloka_client=toloka_client)
        >>> current_tasks = list(it)
        >>> # ... new tasks could appear ...
        >>> new_tasks = list(it)  # Contains only new tasks, appeared since the previous call.
        ...
    """

    _request: search_requests.TaskSearchRequest = attr.ib(
        factory=search_requests.TaskSearchRequest,
    )

    def _get_fetcher(self) -> Callable[..., search_results.TaskSearchResult]:
        return self.toloka_client.find_tasks

    def _get_time_field(self) -> str:
        return 'created'

    def _construct_event(self, item: Task) -> TaskEvent:
        return TaskEvent(task=item, event_time=item.created)


@expand('request')
@fix_attrs_converters
@inherit_docstrings
@attr.s
class UserBonusCursor(BaseCursor):
    """Iterator over `UserBonus` instances by create time.

    Args:
        toloka_client: TolokaClient object that is being used to search `UserBonus` instances.
        request: Base request to search `UserBonus` instances by.

    Examples:
        Iterate over `UserBonus` instances.

        >>> it = UserBonusCursor(toloka_client=toloka_client)
        >>> current_bonuses = list(it)
        >>> # ... new `UserBonus` instances could appear ...
        >>> new_bonuses = list(it)  # Contains only new `UserBonus` instances, appeared since the previous call.
        ...
    """

    _request: search_requests.UserBonusSearchRequest = attr.ib(
        factory=search_requests.UserBonusSearchRequest,
    )

    def _get_fetcher(self) -> Callable[..., search_results.UserBonusSearchResult]:
        return self.toloka_client.find_user_bonuses

    def _get_time_field(self) -> str:
        return 'created'

    def _construct_event(self, item: UserBonus) -> UserBonusEvent:
        return UserBonusEvent(user_bonus=item, event_time=item.created)


@expand('request')
@fix_attrs_converters
@inherit_docstrings
@attr.s
class UserSkillCursor(BaseCursor):
    """Iterator over UserSkillEvent objects of a selected event_type.

    Args:
        toloka_client: TolokaClient object that is being used to search skills.
        request: Base request to search skills by.
        event_type: Skill event type to search.

    Examples:
        Iterate over skills acceptances events.

        >>> it = UserSkillCursor(event_type='MODIFIED', toloka_client=toloka_client)
        >>> current_events = list(it)
        >>> # ... new skills could be set ...
        >>> new_events = list(it)  # Contains only new events, occurred since the previous call.
        ...
    """

    _event_type: UserSkillEvent.Type = attr.ib(converter=functools.partial(structure, cl=UserSkillEvent.Type))
    _request: search_requests.UserSkillSearchRequest = attr.ib(
        factory=search_requests.UserSkillSearchRequest,
    )
    _time_lag: timedelta = attr.ib(default=DEFAULT_LAG)

    def _get_fetcher(self) -> Callable[..., search_results.UserSkillSearchResult]:
        return self.toloka_client.find_user_skills

    def _get_time_field(self) -> str:
        return self._event_type.time_key

    def _construct_event(self, item: UserSkill) -> UserSkillEvent:
        return UserSkillEvent(user_skill=item,
                              event_type=self._event_type,
                              event_time=getattr(item, self._event_type.time_key))


@expand('request')
@fix_attrs_converters
@inherit_docstrings
@attr.s
class UserRestrictionCursor(BaseCursor):
    """Iterator over Toloker restrictions by create time.

    Args:
        toloka_client: TolokaClient object that is being used to search Toloker restrictions.
        request: Base request to search Toloker restrictions.

    Examples:
        Iterate over Toloker restrictions in a project.

        >>> it = UserRestrictionCursor(toloka_client=toloka_client, project_id=my_proj_id)
        >>> current_restrictions = list(it)
        >>> # ... new restrictions could appear ...
        >>> new_restrictions = list(it)  # Contains only new restrictions, appeared since the previous call.
        ...
    """

    _request: search_requests.UserRestrictionSearchRequest = attr.ib(
        factory=search_requests.UserRestrictionSearchRequest,
    )

    def _get_fetcher(self) -> Callable[..., search_results.UserRestrictionSearchResult]:
        return self.toloka_client.find_user_restrictions

    def _get_time_field(self) -> str:
        return 'created'

    def _construct_event(self, item: UserRestriction) -> UserRestrictionEvent:
        return UserRestrictionEvent(user_restriction=item, event_time=getattr(item, self._get_time_field()))


@expand('request')
@fix_attrs_converters
@inherit_docstrings
@attr.s
class MessageThreadCursor(BaseCursor):
    """Iterator over messages by create time.

    Args:
        toloka_client: TolokaClient object that is being used to search messages.
        request: Base request to search messages.

    Examples:
        Iterate over all messages.

        >>> it = MessageThreadCursor(toloka_client=toloka_client)
        >>> all_messages = list(it)
        >>> # ... new messages could appear ...
        >>> new_messages = list(it)  # Contains only new messages, appeared since the previous call.
        ...
    """

    _request: search_requests.MessageThreadSearchRequest = attr.ib(
        factory=search_requests.MessageThreadSearchRequest,
    )

    def _get_fetcher(self) -> Callable[..., search_results.MessageThreadSearchResult]:
        return self.toloka_client.find_message_threads

    def _get_time_field(self) -> str:
        return 'created'

    def _construct_event(self, item: MessageThread) -> MessageThreadEvent:
        return MessageThreadEvent(message_thread=item, event_time=getattr(item, self._get_time_field()))
