__all__ = [
    'MetricCollector',
]
import toloka.metrics.metrics
import typing


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

    def __init__(
        self,
        metrics: typing.List[toloka.metrics.metrics.BaseMetric],
        callback: typing.Callable[[typing.Dict[str, typing.List[typing.Tuple[typing.Any, typing.Any]]]], None]
    ): ...

    @staticmethod
    def create_async_tasks(coro): ...

    async def run(self):
        """Starts collecting metrics. And never stops.
        """
        ...

    metrics: typing.List[toloka.metrics.metrics.BaseMetric]
    _callback: typing.Callable[[typing.Dict[str, typing.List[typing.Tuple[typing.Any, typing.Any]]]], None]
