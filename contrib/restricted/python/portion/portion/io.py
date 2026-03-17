import re

from .const import Bound, inf
from .interval import Interval


def from_string(
    string,
    conv,
    *,
    bound=r".+?",
    disj=r" ?\| ?",
    sep=r", ?",
    left_open=r"\(",
    left_closed=r"\[",
    right_open=r"\)",
    right_closed=r"\]",
    pinf=r"\+inf",
    ninf=r"-inf",
    klass=Interval,
):
    """
    Parse given string and create an Interval instance.
    A converter function has to be provided to convert a bound (as string) to a value.
    This function raises a ValueError if given string cannot be parsed to an interval.

    :param string: string to parse.
    :param conv: function to convert a bound (as string) to an object.
    :param bound: regex pattern for a value.
    :param disj: regex pattern for disjunctive operator (default matches '|' and ' | ').
    :param sep: regex pattern for bound separator (default matches ',').
    :param left_open: regex pattern for left open boundary (default matches '(').
    :param left_closed: regex pattern for left closed boundary (default
        matches '[').
    :param right_open: regex pattern for right open boundary (default matches ')').
    :param right_closed: regex pattern for right closed boundary (default
        matches ']').
    :param pinf: regex pattern for positive infinity (default matches '+inf').
    :param ninf: regex pattern for negative infinity (default matches '-inf').
    :param klass: class to use for creating intervals (default to Interval).
    :return: an interval.
    """

    re_left_boundary = rf"(?P<left>{left_open}|{left_closed})"
    re_right_boundary = rf"(?P<right>{right_open}|{right_closed})"
    re_bounds = rf"(?P<lower>{bound})({sep}(?P<upper>{bound}))?"
    re_interval = rf"{re_left_boundary}(|{re_bounds}){re_right_boundary}"

    intervals = []
    has_more = True
    source = string

    def _convert(bound):
        if re.match(pinf, bound):
            return inf
        elif re.match(ninf, bound):
            return -inf
        else:
            return conv(bound)

    while has_more:
        match = re.match(re_interval, string)
        if match is None:
            raise ValueError(f'"{source}" cannot be parsed to an interval.')

        # Parse atomic interval
        group = match.groupdict()

        left = (
            Bound.CLOSED if re.match(left_closed + "$", group["left"]) else Bound.OPEN
        )
        right = (
            Bound.CLOSED if re.match(right_closed + "$", group["right"]) else Bound.OPEN
        )
        lower = group.get("lower", None)
        upper = group.get("upper", None)
        lower = _convert(lower) if lower is not None else inf
        upper = _convert(upper) if upper is not None else lower

        intervals.append(klass.from_atomic(left, lower, upper, right))
        string = string[match.end() :]

        # Are there more atomic intervals?
        if len(string) > 0:
            match = re.match(disj, string)
            if match is None:
                raise ValueError(f'"{source}" cannot be parsed to an interval.')
            else:
                string = string[match.end() :]
        else:
            has_more = False

    return klass(*intervals)


def to_string(
    interval,
    conv=repr,
    *,
    disj=" | ",
    sep=",",
    left_open="(",
    left_closed="[",
    right_open=")",
    right_closed="]",
    pinf="+inf",
    ninf="-inf",
):
    """
    Export given interval to string.

    :param interval: an interval.
    :param conv: function that is used to represent a bound (default is `repr`).
    :param disj: string representing disjunctive operator (default is ' | ').
    :param sep: string representing bound separator (default is ',').
    :param left_open: string representing left open boundary (default is '(').
    :param left_closed: string representing left closed boundary (default is '[').
    :param right_open: string representing right open boundary (default is ')').
    :param right_closed: string representing right closed boundary (default is ']').
    :param pinf: string representing a positive infinity (default is '+inf').
    :param ninf: string representing a negative infinity (default is '-inf').
    :return: a string representation for given interval.
    """
    if interval.empty:
        return left_open + right_open

    def _convert(bound):
        if bound == inf:
            return pinf
        elif bound == -inf:
            return ninf
        else:
            return conv(bound)

    exported_intervals = []
    for item in interval:
        left = left_open if item.left == Bound.OPEN else left_closed
        right = right_open if item.right == Bound.OPEN else right_closed

        lower = _convert(item.lower)
        upper = _convert(item.upper)

        if item.lower == item.upper:
            exported_intervals.append(left + lower + right)
        else:
            exported_intervals.append(left + lower + sep + upper + right)

    return disj.join(exported_intervals)


def from_data(
    data, conv=None, *, pinf=float("inf"), ninf=float("-inf"), klass=Interval
):
    """
    Import an interval from a list of 4-uples (left, lower, upper, right).

    :param data: a list of 4-uples (left, lower, upper, right).
    :param conv: function to convert bound values, default to identity.
    :param pinf: value used to represent positive infinity.
    :param ninf: value used to represent negative infinity.
    :param klass: class to use for creating intervals (default to Interval).
    :return: an interval.
    """
    intervals = []
    conv = (lambda v: v) if conv is None else conv

    def _convert(bound):
        if bound == pinf:
            return inf
        elif bound == ninf:
            return -inf
        else:
            return conv(bound)

    for item in data:
        left, lower, upper, right = item
        intervals.append(
            klass.from_atomic(
                Bound(left),
                _convert(lower),
                _convert(upper),
                Bound(right),
            )
        )
    return klass(*intervals)


def to_data(interval, conv=None, *, pinf=float("inf"), ninf=float("-inf")):
    """
    Export given interval to a list of 4-uples (left, lower, upper, right).

    :param interval: an interval.
    :param conv: function to convert bound values, default to identity.
    :param pinf: value used to encode positive infinity.
    :param ninf: value used to encode negative infinity.
    :return: a list of 4-uples (left, lower, upper, right)
    """
    conv = (lambda v: v) if conv is None else conv

    data = []

    def _convert(bound):
        if bound == inf:
            return pinf
        elif bound == -inf:
            return ninf
        else:
            return conv(bound)

    for item in interval:
        data.append(
            (
                item.left.value,
                _convert(item.lower),
                _convert(item.upper),
                item.right.value,
            )
        )
    return data
