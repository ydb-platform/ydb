"""Classes for creating online dashboards with tolokas metrics in jupyter notebooks.

{% note warning %}

Requires toloka-kit[jupyter-metrics] extras. Install it with the following command:

```shell
pip install toloka-kit[jupyter-metrics]
```

{% endnote %}

For usage examples see DashBoard.
"""

__all__ = [
    'DashBoard',
    'Chart',
]
import asyncio
import asyncio.events
import datetime
import jupyter_dash.jupyter_app  # type: ignore
import plotly.graph_objs._figure  # type: ignore
import threading
import toloka.metrics.metrics
import typing


class Chart:
    """One chart on the dashboard. Could include several metrics.
    If you want to draw really cool chart that displays several metrics in the same coordinates.

    Args:
        name: The header for this chart. Could be None, Chart create name from the first metric.
        metrics: List of metrics, that will be displayed on this chart (in the same coordinates).

    Example:
        How to display all submitted and accepted answers from some pool and its checking pool, in one chart.

        >>> Chart(
        >>>     'Answers count',
        >>>     [
        >>>         metrics.AssignmentsInPool('123', submitted_name='submitted in 123', accepted_name='accepted in 123', toloka_client=client1),
        >>>         metrics.AssignmentsInPool('456', submitted_name='submitted in 456', accepted_name='accepted in 456', toloka_client=client2),
        >>>     ]
        >>> )
        ...
    """

    class LineStats(tuple):
        """LineStats(x, y, name)
        """

        @staticmethod
        def __new__(
            _cls,
            x,
            y,
            name
        ):
            """Create new instance of LineStats(x, y, name)
            """
            ...

    def __init__(
        self,
        name: typing.Optional[str],
        metrics: typing.List[toloka.metrics.metrics.BaseMetric],
        loop=None
    ): ...

    @staticmethod
    def create_async_tasks(metric: toloka.metrics.metrics.BaseMetric, loop: asyncio.events.AbstractEventLoop): ...

    def update_metrics(self):
        """Gathers all metrics, and stores them in lines.
        """
        ...

    def create_figure(self) -> plotly.graph_objs._figure.Figure:
        """Create figure for this chart. Called at each step.

        Returns:
            Exactly one Figure for this chart.
        """
        ...

    _name: str
    metrics: typing.List[toloka.metrics.metrics.BaseMetric]
    _id: str
    _lines: typing.Dict[str, LineStats]
    _event_loop: asyncio.events.AbstractEventLoop
    _tasks: typing.List[asyncio.Task]


class DashBoard:
    """Toloka dashboard with metrics. Only for jupyter.

    Args:
        metrics: List of metrics or charts, that will be displayed on the dashboard.
            Each element will be displayed in a separate chart (coordinates).
            If you want to draw several metrics in one coordinates, wrap it into an instance of the class Chart.
        header: Your pretty header for this dashboard.
        update_seconds: Count of seconds between dash updates.
        min_time_range: The minimum time range for all charts.
        max_time_range: The maximum time range for all charts. If you have more data, you will see only the last range on charts.

    Examples:
        How to create online dashboard in jupyter.

        >>> import toloka.metrics as metrics
        >>> from toloka.metrics.jupyter_dashboard import Chart, DashBoard
        >>> import toloka.client as toloka
        >>> toloka_client = toloka.TolokaClient(oauth_token, 'PRODUCTION')
        >>> new_dash = DashBoard(
        >>>     [
        >>>         metrics.Balance(),
        >>>         metrics.AssignmentsInPool('123'),
        >>>         metrics.AssignmentEventsInPool('123', submitted_name='submitted', join_events=True),
        >>>         Chart(
        >>>             'Manualy configured chart',
        >>>             [metrics.AssignmentsInPool('123'), metrics.AssignmentsInPool('345'),]
        >>>         )
        >>>     ],
        >>>     header='My cool dash',
        >>> )
        >>> metrics.bind_client(new_dash.metrics, toloka_client)
        >>> # Then in new cell:
        >>> new_dash.run_dash()
        >>> # If you want to stop it:
        >>> new_dash.stop_dash()
        ...
    """

    def __init__(
        self,
        metrics: typing.List[typing.Union[toloka.metrics.metrics.BaseMetric, Chart]],
        header: str = 'Toloka metrics dashboard',
        update_seconds: int = 10,
        min_time_range: datetime.timedelta = ...,
        max_time_range: datetime.timedelta = ...
    ): ...

    def update_charts(self, n_intervals: int) -> typing.Union[plotly.graph_objs._figure.Figure, typing.List[plotly.graph_objs._figure.Figure]]:
        """Redraws all charts on each iteration

        Args:
            n_intervals (int): inner parameter that don't used right now

        Returns:
            Union[plotly.graph_objects.Figure(), List[plotly.graph_objects.Figure()]]: all new Figure's.
                Must have the same length on each iteration.
        """
        ...

    def run_dash(
        self,
        mode: str = 'inline',
        height: int = None,
        host: str = '127.0.0.1',
        port: str = '8050'
    ):
        """Starts dashboard. Starts server for online updating charts.

        You can stop it, by calling 'stop_dash()' for the same dashboard instance.

        Args:
            mode: Same as 'mode' in jupyter_dash.JupyterDash().run_server(). Defaults to 'inline'.
            height: If you don't want auto-computed height. Defaults to None - auto-compute.
            host: Host for server. Defaults to '127.0.0.1'.
            port: Port fo server. Defaults to '8050'.
        """
        ...

    def stop_dash(self):
        """Stops server. And stops updating dashboard.
        """
        ...

    def __del__(self): ...

    _charts: typing.Dict[str, Chart]
    _dashboard: jupyter_dash.jupyter_app.JupyterDash
    _host: typing.Optional[str]
    _port: typing.Optional[str]
    _time_from: typing.Optional[datetime.datetime]
    _time_delta: int
    _min_time_range: datetime.timedelta
    _max_time_range: datetime.timedelta
    _update_seconds: int
    _toloka_thread: threading.Thread
    _event_loop: asyncio.events.AbstractEventLoop
