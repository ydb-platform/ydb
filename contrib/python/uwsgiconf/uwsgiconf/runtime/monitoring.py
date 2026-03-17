from typing import Union

from .signals import _automate_signal, Signal
from .. import uwsgi
from ..typehints import Strint


def register_file_monitor(filename: str, *, target: Union[Strint, Signal] = None):
    """Maps a specific file/directory modification event to a signal.

    :param filename: File or a directory to watch for its modification.

    :param target: Existing signal to raise
        or Signal Target to register signal implicitly.

        Available targets:

            * ``workers``  - run the signal handler on all the workers
            * ``workerN`` - run the signal handler only on worker N
            * ``worker``/``worker0`` - run the signal handler on the first available worker
            * ``active-workers`` - run the signal handlers on all the active [non-cheaped] workers

            * ``mules`` - run the signal handler on all of the mules
            * ``muleN`` - run the signal handler on mule N
            * ``mule``/``mule0`` - run the signal handler on the first available mule

            * ``spooler`` - run the signal on the first available spooler
            * ``farmN/farm_XXX``  - run the signal handler in the mule farm N or named XXX

    :raises ValueError: If unable to register monitor.

    """
    return _automate_signal(target, func=lambda sig: uwsgi.add_file_monitor(int(sig), filename))


class Metric:
    """User metric related stuff.

    .. note:: One needs to register user metric beforehand.
        E.g.:: ``section.monitoring.register_metric(section.monitoring.metric_types.absolute('mymetric'))``

    """
    def __init__(self, name: str):
        """
        :param name: Metric name.

        """
        self.name = name

    @property
    def value(self) -> int:
        """Current metric value."""
        return uwsgi.metric_get(self.name)

    def set(self, value: int, *, mode: str = None) -> bool:
        """Sets metric value.

        :param value: New value.

        :param mode: Update mode.

            * None - Unconditional update.
            * max - Sets metric value if it is greater that the current one.
            * min - Sets metric value if it is less that the current one.

        """
        if mode == 'max':
            func = uwsgi.metric_set_max

        elif mode == 'min':
            func = uwsgi.metric_set_min

        else:
            func = uwsgi.metric_set

        return func(self.name, value)

    def incr(self, delta: int = 1) -> bool:
        """Increments the specified metric key value by the specified value.

        :param delta:

        """
        return uwsgi.metric_inc(self.name, delta)

    def decr(self, delta: int = 1) -> bool:
        """Decrements the specified metric key value by the specified value.

        :param delta:

        """
        return uwsgi.metric_dec(self.name, delta)

    def mul(self, value: int = 1) -> bool:
        """Multiplies the specified metric key value by the specified value.

        :param value:

        """
        return uwsgi.metric_mul(self.name, value)

    def div(self, value: int = 1) -> bool:
        """Divides the specified metric key value by the specified value.

        :param value:

        """
        return uwsgi.metric_div(self.name, value)
