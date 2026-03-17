__all__ = [
    'MetricCollector',
]

import asyncio
import logging

from typing import Any, Callable, Dict, List, Tuple

from .metrics import BaseMetric
from ..util.async_utils import get_task_traceback
from ..util._managing_headers import add_headers

logger = logging.getLogger(__name__)

NamedMetrics = Dict[str, List[Tuple[Any, Any]]]


class MetricCollector:
    """Gather metrics

    Raises:
        TypeError: If toloka_client param isn't instance of toloka.clien.TolokaClient
        TypeError: If some of other positional arguments isn't instance of toloka.metrics.BaseMetric

    Example:
        How to gather metrics and sends it to zabbix:

        >>> import toloka.client as toloka
        >>> from toloka.metrics import MetricCollector, Balance, AssignmentsInPool
        >>>
        >>> toloka_client = toloka.TolokaClient(token, 'PRODUCTION')
        >>>
        >>> def send_metric_to_zabbix(metric_dict):
        >>>     ### do something
        >>>     pass
        >>>
        >>> collector = MetricCollector(
        >>>     [
        >>>         Balance(),
        >>>         AssignmentsInPool('12345678'),
        >>>     ],
        >>>     send_metric_to_zabbix,
        >>> )
        >>>
        >>> bind_client(collector.metrics, toloka_client)
        >>>
        >>> asyncio.run(collector.run())
    """

    metrics: List[BaseMetric]
    _callback: Callable[[NamedMetrics], None]

    def __init__(self, metrics: List[BaseMetric], callback: Callable[[NamedMetrics], None]):
        self._callback = callback
        self.metrics = []
        all_lines_names = set()
        for i, element in enumerate(metrics):
            if not isinstance(element, BaseMetric):
                raise TypeError(f'{i+1} positional argument must be an instance of toloka.metrics.BaseMetric, now it\'s {type(element)}')
            self.metrics.append(element)

            for name in element.get_line_names():
                assert name not in all_lines_names, f'Duplicated metrics name detected: "{name}".'
                all_lines_names.add(name)

    @staticmethod
    def create_async_tasks(coro):
        task = asyncio.Task(coro())
        task.new_coro = coro
        return task

    @add_headers('metrics')
    async def run(self):
        """Starts collecting metrics. And never stops."""
        tasks = [MetricCollector.create_async_tasks(m.get_lines) for m in self.metrics]

        while True:
            done, pending = await asyncio.wait(tasks, timeout=1)
            # check errors
            errored = [task for task in done if task.exception() is not None]
            for task in errored:
                logger.error('Got error in metric:\n%s', get_task_traceback(task))

            # rerun completed tasks
            tasks = pending | {MetricCollector.create_async_tasks(done_task.new_coro) for done_task in done}

            # join metrics
            metrics_points = {}
            for done_task in done:
                if done_task.exception() is not None:
                    continue
                for name, points in done_task.result().items():
                    if name in metrics_points:
                        logger.error(f'Duplicated metrics name detected: "{name}". Only one metric was returned.')
                    metrics_points[name] = points

            if metrics_points:
                self._callback(metrics_points)
