__all__ = [
    'BaseMetric',
    'Balance',
    'bind_client',
    'NewMessageThreads',
    'NewUserBonuses',
    'NewUserSkills',
]

from collections import defaultdict
import datetime
from decimal import Decimal
from itertools import groupby
from operator import attrgetter
import re
import sys

if sys.version_info[:2] >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

import attr
from typing import Any, Optional, Dict, List, Tuple
from ..async_client import AsyncTolokaClient
from ..client import (
    Requester,
    TolokaClient,
)
from ..util._managing_headers import add_headers
from ..util.async_utils import Cooldown
from ..streaming import cursor
from ..streaming.cursor import TolokaClientSyncOrAsyncType, DEFAULT_LAG


def bind_client(metrics: List['BaseMetric'], toloka_client: TolokaClientSyncOrAsyncType) -> None:
    """Sets/updates toloka_client for all metrics in list.

    Examples:
        How to bind same client for all metrics:
        >>> import toloka.client as toloka
        >>> from toloka.metrics import AssignmentsInPool, Balance, bind_client, MetricCollector
        >>>
        >>> toloka_client = toloka.TolokaClient(auth_token, 'PRODUCTION')
        >>>
        >>> collector = MetricCollector(
        >>>     [
        >>>         Balance(),
        >>>         AssignmentsInPool(pool_id),
        >>>     ],
        >>> )
        >>> bind_client(collector.metrics, toloka_client)
        ...

        How to bind several clients:
        >>> metrics_1 = bind_client([Balance(), AssignmentsInPool(pool_id_1)], toloka_client_1)
        >>> metrics_2 = bind_client([Balance(), AssignmentsInPool(pool_id_2)], toloka_client_2)
        >>> collector = MetricCollector(metrics_1 + metrics_2)
        ...
    """
    for metric in metrics:
        if not isinstance(metric, BaseMetric):
            raise TypeError(f'One of element is not instance of BaseMetric: "{metric}"')
        if isinstance(toloka_client, TolokaClient):
            toloka_client = AsyncTolokaClient.from_sync_client(toloka_client)
        metric.set_client(toloka_client)


@attr.s(auto_attribs=True)
class BaseMetric:
    """Base class for all metrics.

    Stores TolokaClient instance for this metric.
    """
    toloka_client: TolokaClient = attr.ib(kw_only=True, default=None)
    atoloka_client: AsyncTolokaClient = attr.ib(kw_only=True, default=None)
    timeout: datetime.timedelta = attr.ib(kw_only=True, factory=lambda: datetime.timedelta(seconds=10))

    def __attrs_post_init__(self):
        assert not (self.toloka_client and self.atoloka_client), \
            f"TolokaClient and AsyncTolokaClient can't be provided to {type(self)} at the same time. " \
            f"Please provide either TolokaClient or AsyncTolokaClient (the other client will be implicitly created)."
        if self.toloka_client or self.atoloka_client:
            self.set_client(self.toloka_client or self.atoloka_client)

    def set_client(self, toloka_client: TolokaClientSyncOrAsyncType):
        """Sets both TolokaClient and AsyncTolokaClient for the object.

        New instance of AsyncTolokaClient is created is case of TolokaClient being passed.
        """

        if isinstance(toloka_client, TolokaClient):
            self.toloka_client = toloka_client
            self.atoloka_client = AsyncTolokaClient.from_sync_client(toloka_client)
        else:
            self.toloka_client = toloka_client.sync_client
            self.atoloka_client = toloka_client

    @add_headers('metrics')
    async def get_lines(self) -> Dict[str, List[Tuple[Any, Any]]]:
        """Gather and return metrics

        All metrics returned in the same format: named list, contain pairs of: datetime of some event, metric value.
        Could not return some metrics in dict on iteration or return it with empty list:
        means that is nothing being gathered on this step. This is not zero value!

        Return example:

        >>> {
        >>>     'rejected_assignments_in_pool': [(datetime.datetime(2021, 8, 12, 10, 4, 44, 895232), 0)],
        >>>     'submitted_assignments_in_pool': [(datetime.datetime(2021, 8, 12, 10, 4, 45, 321904), 75)],
        >>>     'accepted_assignments_in_pool': [(datetime.datetime(2021, 8, 12, 10, 4, 45, 951156), 75)],
        >>>     'accepted_events_in_pool': [(datetime.datetime(2021, 8, 11, 15, 13, 3, 65000), 1), ... ],
        >>>     'rejected_events_in_pool': [],
        >>>     # no toloka_requester_balance on this iteration
        >>> }
        ...
        """
        async with self.coldown:
            result = await self._get_lines_impl()
            return result

    async def _get_lines_impl(self) -> Dict[str, List[Tuple[Any, Any]]]:
        """Real method for gathering metrics"""
        raise NotImplementedError

    @cached_property
    def coldown(self) -> Cooldown:
        return Cooldown(self.timeout.total_seconds())

    def get_line_names(self) -> List[str]:
        """Returns a list of metric names that can be generated by this class instance.
        For example, if a class can generate 5 metrics, but some instance generate only 3 of them,
        this method will return a list with exactly 3 strings. If you call 'get_metrics' on this instance,
        it could return from 0 to 3 metrics.
        """
        raise NotImplementedError

    @cached_property
    def beautiful_name(self) -> str:
        """Used for creating chart from raw metric in jupyter notebook"""
        words = [word for word in re.split(r'([A-Z][a-z]*)', self.__class__.__name__) if word]
        return ' '.join(words).capitalize()


@attr.s(auto_attribs=True)
class Balance(BaseMetric):
    """Traking your Toloka balance.

    Returns only one metric: don't spend and don't reserved money on your acount.

    Args:
        balance_name: Metric name. Default 'toloka_requester_balance'.

    Raises:
        ValueError: If all metric names are set to None.

    Example:
        How to collect this metrics:
        >>> def print_metric(metric_dict):
        >>>     print(metric_dict)
        >>>
        >>> collector = MetricCollector([Balance(toloka_client=toloka_client)], print_metric)
        >>> asyncio.run(collector.run())
        ...

        >>> {
        >>>     toloka_requester_balance: [(datetime.datetime(2021, 8, 30, 10, 30, 59, 628239), Decimal('123.4500'))],
        >>> }
        ...
    """

    balance_name: Optional[str] = None

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        if self.balance_name is None:
            self.balance_name = 'toloka_requester_balance'

    def get_line_names(self) -> List[str]:
        """Returns a list of metric names that can be generated by this class instance.
        """
        return [self.balance_name]

    async def _get_lines_impl(self) -> Dict[str, List[Tuple[Any, Any]]]:
        requester: Requester = await self.atoloka_client.get_requester()
        result = {self.balance_name: [(datetime.datetime.now(datetime.timezone.utc), requester.balance)]}
        return result


@attr.s(auto_attribs=True)
class NewUserBonuses(BaseMetric):
    """Tracking bonuses for Tolokers: bonus count or money amount.

    Args:
        cursor_time_lag: Time lag for cursor. This controls time lag between user bonuses being added and this metric
            being updated. See BaseCursor.time_lag for details and reasoning behind this.
        count_name: Metric name for a count of new bonuses.
        money_name: Metric name for amount of money in new bonuses.
        join_events: Count all events in one point.  Default `False`.

    Example:
        How to collect this metrics:
        >>> def print_metric(metric_dict):
        >>>     print(metric_dict)
        >>>
        >>> collector = MetricCollector([NewUserBonuses(toloka_client=toloka_client)], print_metric)
        >>> asyncio.run(collector.run())
        ...

        >>> {
        >>>     'bonus_count': [(datetime.datetime(2021, 11, 18, 8, 29, 9, 734373), 0)],
        >>>     'bonus_money': [(datetime.datetime(2021, 11, 18, 8, 29, 9, 734377), Decimal('0'))]
        >>> }
        ...
    """
    _cursor_time_lag: datetime.timedelta = DEFAULT_LAG
    _count_name: Optional[str] = None
    _money_name: Optional[str] = None

    _join_events: bool = False

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        metric_names = self.get_line_names()
        if not metric_names:
            self._count_name = 'bonus_count'
            self._money_name = 'bonus_money'
        elif len(metric_names) != len(set(metric_names)):
            raise ValueError('Duplicate metric names.')

    def get_line_names(self) -> List[str]:
        """Returns a list of metric names that can be generated by this class instance.
        """
        return [name for name in (self._count_name, self._money_name) if name is not None]

    @cached_property
    def _cursor(self) -> cursor.UserBonusCursor:
        return cursor.UserBonusCursor(
            toloka_client=self.atoloka_client,
            created_gte=datetime.datetime.now(datetime.timezone.utc),
            time_lag=self._cursor_time_lag,
        )

    async def _get_lines_impl(self) -> Dict[str, List[Tuple[Any, Any]]]:
        result = {}

        it = self._cursor
        event_list = [event async for event in it]
        if self._join_events:
            count = len(event_list)
            money = sum(event.user_bonus.amount for event in event_list)
            if self._count_name is not None:
                result[self._count_name] = [(event_list[-1].event_time, count)] if count else [(datetime.datetime.now(datetime.timezone.utc), 0)]
            if self._money_name is not None:
                result[self._money_name] = [(event_list[-1].event_time, money)] if money else [(datetime.datetime.now(datetime.timezone.utc), Decimal(0))]
        else:
            if self._count_name is not None:
                result[self._count_name] = [
                    (event_time, len(events))
                    for event_time, events in groupby(event_list, attrgetter('event_time'))
                ]
            if self._money_name is not None:
                result[self._money_name] = [
                    (event_time, sum(event.user_bonus.amount for event in events))
                    for event_time, events in groupby(event_list, attrgetter('event_time'))
                ]

        return result


@attr.s(auto_attribs=True)
class NewUserSkills(BaseMetric):
    """Tracking Tolokers' skills.

    Args:
        skill_id: Which skill we will be tracking.
        cursor_time_lag: Time lag for cursor. This controls time lag between user skills being updated and this metric
            being updated. See BaseCursor.time_lag for details and reasoning behind this.
        count_name: Metric name for a count of new skill assignments. When skill changes it counts to.
        value_name: Metric name for exact values of new skill level for each skill assignment. It could be useful to track mean value or some medians.
        join_events: Count all events in one point.  Default `False`. "Values" never join.

    Example:
        How to collect this metrics:
        >>> def print_metric(metric_dict):
        >>>     print(metric_dict)
        >>>
        >>> collector = MetricCollector
        >>> (
        >>>     [
        >>>         NewUserSkills(
        >>>             toloka_client=toloka_client,
        >>>             count_name='count',
        >>>             value_name='values',
        >>>         )
        >>>     ],
        >>>     print_metric
        >>> )
        >>> asyncio.run(collector.run())
        ...

        >>> {
        >>>     'count': [(datetime.datetime(2021, 11, 18, 8, 31, 54, 11000), 1)],
        >>>     'values':  [(datetime.datetime(2021, 11, 18, 8, 31, 54, 11000), Decimal('50.000000000000'))],
        >>> }
        ...
    """
    _skill_id: str = attr.ib(kw_only=False)
    _cursor_time_lag: datetime.timedelta = DEFAULT_LAG
    _count_name: Optional[str] = None
    _value_name: Optional[str] = None

    _join_events: bool = False

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        metric_names = self.get_line_names()
        if not metric_names:
            self._value_name = 'skill_values'
        elif len(metric_names) != len(set(metric_names)):
            raise ValueError('Duplicate metric names.')

    def get_line_names(self) -> List[str]:
        """Returns a list of metric names that can be generated by this class instance.
        """
        return [name for name in (self._count_name, self._value_name) if name is not None]

    @cached_property
    def _cursor(self) -> cursor.UserSkillCursor:
        return cursor.UserSkillCursor(
            toloka_client=self.atoloka_client,
            skill_id=self._skill_id,
            created_gte=datetime.datetime.now(datetime.timezone.utc),
            event_type='MODIFIED',
            time_lag=self._cursor_time_lag,
        )

    async def _get_lines_impl(self) -> Dict[str, List[Tuple[Any, Any]]]:
        result = {}

        it = self._cursor
        event_list = [event async for event in it]
        if self._count_name is not None:
            if self._join_events:
                count = len(event_list)
                result[self._count_name] = [(event_list[-1].event_time, count)] if count else [(datetime.datetime.now(datetime.timezone.utc), 0)]
            else:
                result[self._count_name] = [
                    (event_time, len(events))
                    for event_time, events in groupby(event_list, attrgetter('event_time'))
                ]
        if self._value_name is not None:
            result[self._value_name] = [
                (event.event_time, event.user_skill.exact_value)
                for event in event_list
            ]

        return result


@attr.s(auto_attribs=True)
class NewMessageThreads(BaseMetric):
    """Tracking the new messages

    Could count messages in projects or pools. If you want to track messages count in several projects/pools, don't get several
    NewMessageThreads instance. You can gather all in one instance.

    Args:
        cursor_time_lag: Time lag for cursor. This controls time lag between message threads being created and this
            metric being updated. See BaseCursor.time_lag for details and reasoning behind this.
        count_name: Metric name for a count of new messages.
        projects_name: Dictionary that allows count messages on exact projects. {project_id: line_name}
        pools_name: Dictionary that allows count messages on exact pools. {pool_id: line_name}
        join_events: Count all events in one point. Default `False`. "Values" never join.

    Example:
        How to collect this metrics:
        >>> def print_metric(metric_dict):
        >>>     print(metric_dict)
        >>>
        >>> collector = MetricCollector
        >>> (
        >>>     [
        >>>         NewMessageThreads(
        >>>             toloka_client=toloka_client,
        >>>             count_name='messages_count'
        >>>             pools_name={'123': 'my_train_pool', '456': 'my_working_pool'},
        >>>             projects_name={'01': 'pedestrian_proj', '02': 'checking_proj'},
        >>>             join_events=True,
        >>>         )
        >>>     ],
        >>>     print_metric
        >>> )
        >>> asyncio.run(collector.run())
        ...

        >>> {
        >>>     # all messages in all projects and pools
        >>>     'messages_count': [(datetime.datetime(2021, 11, 19, 9, 40, 15, 970000), 10)],
        >>>     # messages on this exact pool
        >>>     'my_train_pool': [(datetime.datetime(2021, 11, 19, 12, 42, 50, 554830), 4)],
        >>>     # with 'join_events=True' it will be zero if no messages
        >>>     'my_working_pool': [(datetime.datetime(2021, 11, 19, 12, 42, 50, 554830), 0)],
        >>>     'pedestrian_proj': [(datetime.datetime(2021, 11, 19, 12, 42, 50, 554830), 1)],
        >>>     # total count != sum of other counts, because could exist different pools and projects
        >>>     'checking_proj': [(datetime.datetime(2021, 11, 19, 12, 42, 50, 554830), 1)],
        >>> }
        ...
    """
    _cursor_time_lag: datetime.timedelta = DEFAULT_LAG
    _count_name: Optional[str] = None
    _projects_name: Dict[str, str] = {}  # {project_id: line_name}
    _pools_name: Dict[str, str] = {}  # {pool_id: line_name}
    _join_events: bool = False

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        metric_names = self.get_line_names()
        if not metric_names:
            self._count_name = 'messages_count'
        elif len(metric_names) != len(set(metric_names)):
            raise ValueError('Duplicate metric names.')

    def get_line_names(self) -> List[str]:
        """Returns a list of metric names that can be generated by this class instance.
        """
        line_names = []
        if self._projects_name:
            line_names.extend(self._projects_name.values())
        if self._pools_name:
            line_names.extend(self._pools_name.values())
        if self._count_name is not None:
            line_names.append(self._count_name)
        return line_names

    @cached_property
    def _cursor(self) -> cursor.MessageThreadCursor:
        return cursor.MessageThreadCursor(
            toloka_client=self.atoloka_client,
            created_gte=datetime.datetime.now(datetime.timezone.utc),
            time_lag=self._cursor_time_lag,
        )

    async def _get_lines_impl(self) -> Dict[str, List[Tuple[Any, Any]]]:
        result = defaultdict(list)

        it = self._cursor
        event_list = [event async for event in it]
        if self._join_events:
            if self._count_name is not None:
                result[self._count_name] = [(event_list[-1].event_time, len(event_list))] if event_list else [(datetime.datetime.now(datetime.timezone.utc), 0)]

            if self._projects_name or self._pools_name:
                pools_count = defaultdict(int)
                projects_count = defaultdict(int)
                for event in event_list:
                    pool_id = event.message_thread.meta.pool_id
                    project_id = event.message_thread.meta.project_id
                    if pool_id in self._pools_name:
                        pools_count[pool_id] += 1
                    if project_id in self._projects_name:
                        projects_count[project_id] += 1

                datetime_now = datetime.datetime.now(datetime.timezone.utc)
                for project_id, line_name in self._projects_name.items():
                    result[line_name] = [(datetime_now, projects_count.get(project_id, 0))]
                for pool_id, line_name in self._pools_name.items():
                    result[line_name] = [(datetime_now, pools_count.get(pool_id, 0))]
        else:
            if self._count_name is not None:
                result[self._count_name] = [
                    (event_time, len(events))
                    for event_time, events in groupby(event_list, attrgetter('event_time'))
                ]
            if self._projects_name is not None:
                for project_id, events in groupby(event_list, attrgetter('message_thread.meta.project_id')):
                    if project_id in self._projects_name:
                        result[self._projects_name[project_id]].extend(
                            [
                                (event_time, len(sub_events))
                                for event_time, sub_events in groupby(events, attrgetter('event_time'))
                            ]
                        )
            if self._pools_name is not None:
                for pool_id, events in groupby(event_list, attrgetter('message_thread.meta.pool_id')):
                    if pool_id in self._pools_name:
                        result[self._pools_name[pool_id]].extend(
                            [
                                (event_time, len(sub_events))
                                for event_time, sub_events in groupby(events, attrgetter('event_time'))
                            ]
                        )

        return result
