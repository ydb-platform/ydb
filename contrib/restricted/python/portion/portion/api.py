import functools
import importlib
import importlib.machinery
import importlib.util

from .const import Bound, inf
from .dict import IntervalDict
from .func import closed, closedopen, empty, iterate, open, openclosed, singleton
from .io import from_data, from_string, to_data, to_string

__all__ = ["create_api"]


def partial(wrapped, *args, **kwargs):
    """
    Convenient helper that combines functools.update_wrapper and
    functools.partial. It has exactly the same signature than functools.partial.
    """
    return functools.update_wrapper(
        functools.partial(wrapped, *args, **kwargs), wrapped
    )


def create_api(interval, *, interval_dict=None, name=None):
    """Create a spe

    Dynamically create a module whose API is similar to the one of portion, but
    configured to use given Interval class. Unless specified, a new IntervalDict
    subclass is automatically generated to use given Interval subclass.

    This feature is experimental, and may be changed even in minor or patch
    updates of portion.

    :param interval: a subclass of Interval.
    :param interval_dict: a subclass of IntervalDict.
    :param name: the name of the new module.
    """
    module_name = "portion_" + interval.__name__ if name is None else name

    if interval_dict is None:
        interval_dict = type(
            interval.__name__ + "Dict",
            (IntervalDict,),
            {"_klass": interval},
        )

    objects = {
        "inf": inf,
        "CLOSED": Bound.CLOSED,
        "OPEN": Bound.OPEN,
        "Interval": interval,
        "open": partial(open, klass=interval),
        "closed": partial(closed, klass=interval),
        "openclosed": partial(openclosed, klass=interval),
        "closedopen": partial(closedopen, klass=interval),
        "singleton": partial(singleton, klass=interval),
        "empty": partial(empty, klass=interval),
        "iterate": iterate,
        "from_string": partial(from_string, klass=interval),
        "to_string": to_string,
        "from_data": partial(from_data, klass=interval),
        "to_data": to_data,
        "IntervalDict": interval_dict,
    }

    module = importlib.util.module_from_spec(
        importlib.machinery.ModuleSpec(module_name, None)
    )

    # module.__all__ = list(objects.keys())
    for name, obj in objects.items():
        setattr(module, name, obj)

    return module
