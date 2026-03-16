import operator
from functools import partial

from .const import Bound, inf
from .interval import Interval


def open(lower, upper, *, klass=Interval):
    """
    Create an open interval with given bounds.

    :param lower: value of the lower bound.
    :param upper: value of the upper bound.
    :param klass: class to use for creating intervals (default to Interval).
    :return: an interval.
    """
    return klass.from_atomic(Bound.OPEN, lower, upper, Bound.OPEN)


def closed(lower, upper, *, klass=Interval):
    """
    Create a closed interval with given bounds.

    :param lower: value of the lower bound.
    :param upper: value of the upper bound.
    :param klass: class to use for creating intervals (default to Interval).
    :return: an interval.
    """
    return klass.from_atomic(Bound.CLOSED, lower, upper, Bound.CLOSED)


def openclosed(lower, upper, *, klass=Interval):
    """
    Create a left-open interval with given bounds.

    :param lower: value of the lower bound.
    :param upper: value of the upper bound.
    :param klass: class to use for creating intervals (default to Interval).
    :return: an interval.
    """
    return klass.from_atomic(Bound.OPEN, lower, upper, Bound.CLOSED)


def closedopen(lower, upper, *, klass=Interval):
    """
    Create a right-open interval with given bounds.

    :param lower: value of the lower bound.
    :param upper: value of the upper bound.
    :param klass: class to use for creating intervals (default to Interval).
    :return: an interval.
    """
    return klass.from_atomic(Bound.CLOSED, lower, upper, Bound.OPEN)


def singleton(value, *, klass=Interval):
    """
    Create a singleton interval.

    :param value: value of the lower and upper bounds.
    :param klass: class to use for creating intervals (default to Interval).
    :return: an interval.
    """
    return klass.from_atomic(Bound.CLOSED, value, value, Bound.CLOSED)


def empty(*, klass=Interval):
    """
    Create an empty interval.

    :param klass: class to use for creating intervals (default to Interval).
    :return: an interval.
    """
    return klass()


def iterate(interval, step, *, base=None, reverse=False):
    """
    Iterate on the (discrete) values of given interval.

    This function returns a (lazy) iterator over the values of given interval,
    starting from its lower bound and ending on its upper bound (if interval is
    not open). Each returned value merely corresponds to lower + i * step, where
    "step" defines the step between consecutive values. If reverse=True, a
    negative step must be passed (as in Python's range function).
    It also accepts a callable that is used to compute the next possible
    value based on the current one.

    When a non-atomic interval is provided, this function chains the iterators obtained
    by calling itself on the underlying atomic intervals.

    The values returned by the iterator can be aligned with a base value with the
    "base" parameter. This parameter must be a callable that accepts the lower bound
    of the (atomic) interval as input, and returns the first value that needs to be
    considered for the iteration.
    By default, the identity function is used. If reverse=True, then the upper bound
    will be passed instead of the lower one.

    :param interval: an interval.
    :param step: step between values, or a callable that returns the next value.
    :param base: a callable that accepts a bound and returns an initial value.
    :param reverse: set to True for descending order.
    :return: a lazy iterator.
    """
    if base is None:

        def base(x):
            return x

    if not reverse:

        def exclude(v, i):
            return v < i.lower or (i.left is Bound.OPEN and v <= i.lower)

        def include(v, i):
            return v < i.upper or (i.right is Bound.CLOSED and v <= i.upper)
    else:

        def exclude(v, i):
            return v > i.upper or (i.right is Bound.OPEN and v >= i.upper)

        def include(v, i):
            return v > i.lower or (i.left is Bound.CLOSED and v >= i.lower)

    step = step if callable(step) else partial(operator.add, step)

    value = base(interval.lower if not reverse else interval.upper)
    if (value == -inf and not reverse) or (value == inf and reverse):
        raise ValueError("Cannot start iteration with infinity.")

    for i in interval if not reverse else reversed(interval):
        value = base(i.lower if not reverse else i.upper)

        while exclude(value, i):
            value = step(value)

        while include(value, i):
            yield value
            value = step(value)
