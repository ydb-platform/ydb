from datetime import datetime
from functools import partial, wraps
from typing import Union, Callable

from .signals import _automate_signal, Signal
from .. import uwsgi
from ..exceptions import RuntimeConfigurationError
from ..typehints import Strint

TypeTarget = Union[Strint, Signal]
TypeRegResult = Union[Callable, bool]


def register_timer(period: int, *, target: TypeTarget = None) -> TypeRegResult:
    """Add timer.

    Can be used as a decorator:

        .. code-block:: python

            @register_timer(3)
            def repeat():
                do()

    :param int period: The interval (seconds) at which to raise the signal.

    :param target: Existing signal to raise or Signal Target to register signal implicitly.

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

    :raises ValueError: If unable to add timer.

    """
    return _automate_signal(target, func=lambda sig: uwsgi.add_timer(int(sig), period))


def register_timer_rb(period: int, *, repeat: int = None, target: TypeTarget = None) -> TypeRegResult:
    """Add a red-black timer (based on black-red tree).

        .. code-block:: python

            @register_timer_rb(3)
            def repeat():
                do()

    :param period: The interval (seconds) at which the signal is raised.

    :param repeat: How many times to send signal. Will stop after ther number is reached.
        Default: None - infinitely.

    :param target: Existing signal to raise or Signal Target to register signal implicitly.

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

    :raises ValueError: If unable to add timer.

    """
    return _automate_signal(target, func=lambda sig: uwsgi.add_rb_timer(int(sig), period, repeat or 0))


def register_timer_ms(period: int, *, target: TypeTarget = None) -> TypeRegResult:
    """Add a millisecond resolution timer.

        .. code-block:: python

            @register_timer_ms(300)
            def repeat():
                do()

    :param period: The interval (milliseconds) at which the signal is raised.

    :param target: Existing signal to raise or Signal Target to register signal implicitly.

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

    :raises ValueError: If unable to add timer.

    """
    return _automate_signal(target, func=lambda sig: uwsgi.add_ms_timer(int(sig), period))


def register_cron(
        *,
        weekday: Strint = None,
        month: Strint = None,
        day: Strint = None,
        hour: Strint = None,
        minute: Strint = None,
        target: TypeTarget = None
) -> TypeRegResult:
    """Adds cron. The interface to the uWSGI signal cron facility.

        .. code-block:: python

            @register_cron(hour=-3)  # Every 3 hours.
            def repeat():
                do()

    .. note:: Arguments work similarly to a standard crontab,
        but instead of "*", use -1,
        and instead of "/2", "/3", etc. use -2 and -3, etc.

    .. note:: Periods - rules like hour='10-18/2' (from 10 till 18 every 2 hours) - are allowed,
        but they are emulated by uwsgiconf. Use strings to define periods.

        Keep in mind, that your actual function will be wrapped into another one, which will check
        whether it is time to call your function.

    :param weekday: Day of a the week number. Defaults to `each`.
        0 - Sunday  1 - Monday  2 - Tuesday  3 - Wednesday
        4 - Thursday  5 - Friday  6 - Saturday

    :param month: Month number 1-12. Defaults to `each`.

    :param day: Day of the month number 1-31. Defaults to `each`.

    :param hour: Hour 0-23. Defaults to `each`.

    :param minute: Minute 0-59. Defaults to `each`.

    :param target: Existing signal to raise or Signal Target to register signal implicitly.

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

    :raises ValueError: If unable to add cron rule.

    """
    task_args_initial = {name: val for name, val in locals().items() if val is not None and name != 'target'}
    task_args_casted = {}

    def skip_task(check_funcs):
        now = datetime.now()
        allright = all((func(now) for func in check_funcs))
        return not allright

    def check_date(now, attr, target_range):
        attr = getattr(now, attr)

        if callable(attr):  # E.g. weekday.
            attr = attr()

        return attr in target_range

    check_date_funcs = []

    for name, val in task_args_initial.items():

        # uWSGI won't accept strings, so we emulate ranges here.
        if isinstance(val, str):

            # Rules like 10-18/2 (from 10 till 18 every 2 hours).
            val, _, step = val.partition('/')
            step = int(step) if step else 1
            start, _, end = val.partition('-')

            if not (start and end):
                raise RuntimeConfigurationError(
                    'String cron rule without a range is not supported. Use integer notation.')

            start = int(start)
            end = int(end)

            now_attr_name = name

            period_range = set(range(start, end+1, step))

            if name == 'weekday':
                # Special case for weekday indexes: swap uWSGI Sunday 0 for ISO Sunday 7.
                now_attr_name = 'isoweekday'

                if 0 in period_range:
                    period_range.discard(0)
                    period_range.add(7)

            # Gather date checking functions in one place.
            check_date_funcs.append(partial(check_date, attr=now_attr_name, target_range=period_range))

            # Use minimal duration (-1).
            val = None

        task_args_casted[name] = val

    if not check_date_funcs:
        # No special handling of periods, quit early.
        args = [(-1 if arg is None else arg) for arg in (minute, hour, day, month, weekday)]
        return _automate_signal(target, func=lambda sig: uwsgi.add_cron(int(sig), *args))

    skip_task = partial(skip_task, check_date_funcs)

    def decor(func_action):
        """Decorator wrapping."""

        @wraps(func_action)
        def func_action_wrapper(*args, **kwargs):
            """Action function wrapper to handle periods in rules."""

            if skip_task():
                # todo Maybe allow user defined value for this return.
                return None

            return func_action(*args, **kwargs)

        args = []
        for arg_name in ['minute', 'hour', 'day', 'month', 'weekday']:
            arg = task_args_casted.get(arg_name, None)
            args.append(-1 if arg is None else arg)

        return _automate_signal(target, func=lambda sig: uwsgi.add_cron(int(sig), *args))(func_action_wrapper)

    return decor
