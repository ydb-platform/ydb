from ..base import ParametrizedValue
from ..utils import KeyValue, filter_locals
from .monitoring_metric_types import Metric


class Collector(ParametrizedValue):

    name_separator = ','
    name_separator_strip = True
    args_joiner = ','

    def __init__(self, *args, **kwargs):
        args = list(args)

        # Adding children for metric to use.
        children = kwargs.pop('children', None)

        if children:
            children_ = []

            for child in children:
                if isinstance(child, Metric):
                    child = child.name
                children_.append(child)

            args.append(f"children={';'.join(children_)}")

        super().__init__(*args)


class CollectorPointer(Collector):
    """The value is collected from memory pointer."""

    name = 'ptr'


class CollectorFile(Collector):
    """The value is collected from a file."""

    name = 'file'

    def __init__(self, fpath, *, get_slot=None):
        """
        :param str fpath: File path.

        :param int get_slot: Get value from the given slot number.
            Slots: the content is split (using \\n, \\t, spaces, \\r and zero as separator)
            and the item (the returned array is zero-based) used as the return value.

        """
        value = KeyValue(locals(), aliases={'fpath': 'arg1', 'get_slot': 'arg1n'})

        super().__init__(value)


class CollectorFunction(Collector):
    """The value is computed calling a specific C function every time.

    .. note::
        * The argument it takes is a ``uwsgi_metric`` pointer.
          You generally do not need to parse the metric, so just casting to void will avoid headaches.

        * The function must returns an ``int64_t`` value.

    """

    name = 'func'

    def __init__(self, func):
        """
        :param str func: Function to call.
        """
        value = KeyValue(locals(), aliases={'func': 'arg1'})

        super().__init__(value)


class CollectorSum(Collector):
    """The value is the sum of other metrics."""

    name = 'sum'

    def __init__(self, what):
        super().__init__(children=what)


class CollectorAvg(Collector):
    """The value is the algebraic average of the children.

    .. note:: Since 1.9.20

    """
    name = 'avg'

    def __init__(self, what):
        super().__init__(children=what)


class CollectorAccumulator(Collector):
    """Always add the sum of children to the final value.

    Example:

        * Round 1: child1 = 22, child2 = 17 -> metric_value = 39
        * Round 2: child1 = 26, child2 = 30 -> metric_value += 56

    """
    name = 'accumulator'

    def __init__(self, what):
        super().__init__(children=what)


class CollectorAdder(Collector):
    """Add the specified argument (arg1n) to the sum of children.

    """
    name = 'adder'

    def __init__(self, what, value):
        """
        :param int value: Value to add (multiply if it is CollectorMultiplier).

        """
        value = KeyValue(filter_locals(locals(), drop=['what']), aliases={'value': 'arg1n'})

        super().__init__(value, children=what)


class CollectorMultiplier(CollectorAdder):
    """Multiply the sum of children by the specified argument.

    Example:

        * child1 = 22, child2 = 17, arg1n = 3 -> metric_value = (22+17)*3

    """
    name = 'multiplier'
