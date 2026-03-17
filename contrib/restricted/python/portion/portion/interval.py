import warnings
from collections import namedtuple

from .const import Bound, inf

Atomic = namedtuple("Atomic", ["left", "lower", "upper", "right"])


def mergeable(a, b):
    """
    Tester whether two atomic intervals can be merged (i.e. they overlap or
    are adjacent).

    :param a: an atomic interval.
    :param b: an atomic interval.
    :return: True if mergeable, False otherwise.
    """
    warnings.warn(
        "This function is deprecated, use Interval._mergeable instead.",
        DeprecationWarning,
    )
    return Interval._mergeable(a, b)


class Interval:
    """
    This class represents an interval.

    An interval is an (automatically simplified) union of atomic intervals.
    It can be created with Interval.from_atomic(...) or by passing Interval
    instances to __init__.
    """

    __slots__ = ("_intervals",)
    __match_args__ = ("left", "lower", "upper", "right")

    def __init__(self, *intervals):
        """
        Create a disjunction of zero, one or more intervals.

        :param intervals: zero, one or more intervals.
        """
        self._intervals = []

        for interval in intervals:
            if isinstance(interval, Interval):
                if not interval.empty:
                    self._intervals.extend(interval._intervals)
            else:
                raise TypeError("Parameters must be Interval instances")

        if len(self._intervals) > 0:
            # Sort intervals by lower bound, closed first.
            self._intervals.sort(key=lambda i: (i.lower, i.left is Bound.OPEN))

            i = 0
            # Try to merge consecutive intervals
            while i < len(self._intervals) - 1:
                current = self._intervals[i]
                successor = self._intervals[i + 1]

                if self.__class__._mergeable(current, successor):
                    if current.lower == successor.lower:
                        lower = current.lower
                        left = (
                            current.left
                            if current.left == Bound.CLOSED
                            else successor.left
                        )

                    else:
                        lower = min(current.lower, successor.lower)
                        left = (
                            current.left if lower == current.lower else successor.left
                        )

                    if current.upper == successor.upper:
                        upper = current.upper
                        right = (
                            current.right
                            if current.right == Bound.CLOSED
                            else successor.right
                        )
                    else:
                        upper = max(current.upper, successor.upper)
                        right = (
                            current.right if upper == current.upper else successor.right
                        )

                    union = Atomic(left, lower, upper, right)
                    self._intervals.pop(i)  # pop current
                    self._intervals.pop(i)  # pop successor
                    self._intervals.insert(i, union)
                else:
                    i = i + 1

    @classmethod
    def from_atomic(cls, left, lower, upper, right):
        """
        Create an Interval instance containing a single atomic interval.

        :param left: either CLOSED or OPEN.
        :param lower: value of the lower bound.
        :param upper: value of the upper bound.
        :param right: either CLOSED or OPEN.
        """
        left = left if lower not in [inf, -inf] else Bound.OPEN
        right = right if upper not in [inf, -inf] else Bound.OPEN

        instance = cls()
        # Check for non-emptiness (otherwise keep instance._intervals = [])
        if lower < upper or (
            lower == upper and left == Bound.CLOSED and right == Bound.CLOSED
        ):
            instance._intervals = [Atomic(left, lower, upper, right)]
        return instance

    @classmethod
    def _mergeable(cls, a, b):
        """
        Tester whether two atomic intervals can be merged (i.e. they overlap or
        are adjacent).

        :param a: an atomic interval.
        :param b: an atomic interval.
        :return: True if mergeable, False otherwise.
        """
        if a.lower < b.lower or (a.lower == b.lower and a.left == Bound.CLOSED):
            first, second = a, b
        else:
            first, second = b, a

        if first.upper == second.lower:
            return first.right == Bound.CLOSED or second.left == Bound.CLOSED

        return first.upper > second.lower

    @property
    def left(self):
        """
        Lowest left boundary is either CLOSED or OPEN.
        """
        if self.empty:
            return Bound.OPEN
        return self._intervals[0].left

    @property
    def lower(self):
        """
        Lowest lower bound value.
        """
        if self.empty:
            return inf
        return self._intervals[0].lower

    @property
    def upper(self):
        """
        Highest upper bound value.
        """
        if self.empty:
            return -inf
        return self._intervals[-1].upper

    @property
    def right(self):
        """
        Highest right boundary is either CLOSED or OPEN.
        """
        if self.empty:
            return Bound.OPEN
        return self._intervals[-1].right

    @property
    def empty(self):
        """
        True if interval is empty, False otherwise.
        """
        return len(self._intervals) == 0

    @property
    def atomic(self):
        """
        True if this interval is atomic, False otherwise.
        An interval is atomic if it is empty or composed of a single interval.
        """
        return len(self._intervals) <= 1

    @property
    def enclosure(self):
        """
        Return the smallest interval composed of a single atomic interval that encloses
        the current interval.

        :return: an Interval instance.
        """
        return self.__class__.from_atomic(self.left, self.lower, self.upper, self.right)

    def replace(
        self, left=None, lower=None, upper=None, right=None, *, ignore_inf=True
    ):
        """
        Create a new interval based on the current one and the provided values.

        If current interval is not atomic, it is extended or restricted such that
        its enclosure satisfies the new bounds. In other words, its new enclosure
        will be equal to self.enclosure.replace(left, lower, upper, right).

        Callable can be passed instead of values. In that case, it is called with the
        current corresponding value except if ignore_inf if set (default) and the
        corresponding bound is an infinity.

        :param left: (a function of) left boundary.
        :param lower: (a function of) value of the lower bound.
        :param upper: (a function of) value of the upper bound.
        :param right: (a function of) right boundary.
        :param ignore_inf: ignore infinities if functions are provided (default
            is True).
        :return: an Interval instance
        """
        enclosure = self.enclosure

        if callable(left):
            left = left(enclosure.left)
        else:
            left = enclosure.left if left is None else left

        if callable(lower):
            if ignore_inf and enclosure.lower in [-inf, inf]:
                lower = enclosure.lower
            else:
                lower = lower(enclosure.lower)
        else:
            lower = enclosure.lower if lower is None else lower

        if callable(upper):
            if ignore_inf and enclosure.upper in [-inf, inf]:
                upper = enclosure.upper
            else:
                upper = upper(enclosure.upper)
        else:
            upper = enclosure.upper if upper is None else upper

        if callable(right):
            right = right(enclosure.right)
        else:
            right = enclosure.right if right is None else right

        if self.atomic:
            return self.__class__.from_atomic(left, lower, upper, right)

        n_interval = self & self.__class__.from_atomic(left, lower, upper, right)

        if n_interval.atomic:
            return n_interval.replace(left, lower, upper, right)
        else:
            lowest = n_interval[0].replace(left=left, lower=lower)
            highest = n_interval[-1].replace(upper=upper, right=right)
            return self.__class__(lowest, *n_interval[1:-1], highest)

    def apply(self, func):
        """
        Apply a function on each of the underlying atomic intervals and return their
        union as a new interval instance

        Given function is expected to return an interval (possibly empty or not
        atomic) or a 4-uple (left, lower, upper, right) whose values correspond to
        the parameters of Interval.from_atomic(left, lower, upper, right).

        This method is merely a shortcut for Interval(*list(map(func, self))).

        :param func: function to apply on each underlying atomic interval.
        :return: an Interval instance.
        """
        intervals = []

        for i in self:
            value = func(i)

            if isinstance(value, Interval):
                intervals.append(value)
            elif isinstance(value, tuple):
                intervals.append(self.__class__.from_atomic(*value))
            else:
                raise TypeError(f"Unsupported return type {type(value)} for {value}")

        return self.__class__(*intervals)

    def adjacent(self, other):
        """
        Test if two intervals are adjacent.

        Two intervals are adjacent if they do not overlap and their union form a
        single atomic interval.

        While this definition corresponds to the usual notion of adjacency for atomic
        intervals, it has stronger requirements for non-atomic ones since it requires
        all underlying atomic intervals to be adjacent (i.e. that one
        interval fills the gaps between the atomic intervals of the other one).

        :param other: an interval.
        :return: True if intervals are adjacent, False otherwise.
        """
        return (self & other).empty and (self | other).atomic

    def overlaps(self, other):
        """
        Test if two intervals overlap (i.e. if their intersection is non-empty).

        :param other: an interval.
        :return: True if intervals overlap, False otherwise.
        """
        if isinstance(other, Interval):
            if self.upper < other.lower or self.lower > other.upper:
                # Early out for clearly non-overlapping intervals
                return False

            i_iter = iter(self)
            o_iter = iter(other)
            i_current = next(i_iter, None)
            o_current = next(o_iter, None)

            while i_current is not None and o_current is not None:
                if i_current < o_current:
                    i_current = next(i_iter, None)
                elif o_current < i_current:
                    o_current = next(o_iter, None)
                else:
                    return True
            return False
        else:
            raise TypeError(f"Unsupported type {type(other)} for {other}")

    def intersection(self, other):
        """
        Return the intersection of two intervals.

        :param other: an interval.
        :return: the intersection of the intervals.
        """
        return self & other

    def union(self, other):
        """
        Return the union of two intervals.

        :param other: an interval.
        :return: the union of the intervals.
        """
        return self | other

    def contains(self, item):
        """
        Test if given item is contained in this interval.
        This method accepts intervals and arbitrary comparable values.

        :param item: an interval or any arbitrary comparable value.
        :return: True if given item is contained, False otherwise.
        """
        return item in self

    def complement(self):
        """
        Return the complement of this interval.

        :return: the complement of this interval.
        """
        return ~self

    def difference(self, other):
        """
        Return the difference of two intervals.

        :param other: an interval.
        :return: the difference of the intervals.
        """
        return self - other

    def __getattr__(self, name):
        if name == "__next__":
            # Hack for a better representation of intervals within pandas
            # See https://github.com/AlexandreDecan/portion/pull/54
            try:
                import inspect

                if inspect.stack()[1].function == "pprint_thing":
                    return
            except Exception:
                pass
        raise AttributeError

    def __len__(self):
        return len(self._intervals)

    def __iter__(self):
        yield from (self.__class__.from_atomic(*i) for i in self._intervals)

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.__class__(
                *[self.__class__.from_atomic(*i) for i in self._intervals[item]]
            )
        else:
            return self.__class__.from_atomic(*self._intervals[item])

    def __and__(self, other):
        if not isinstance(other, Interval):
            return NotImplemented

        if self.upper < other.lower or self.lower > other.upper:
            # Early out for non-overlapping intervals
            return self.__class__()
        elif self.atomic and other.atomic:
            if self.lower == other.lower:
                lower = self.lower
                left = self.left if self.left == Bound.OPEN else other.left
            else:
                lower = max(self.lower, other.lower)
                left = self.left if lower == self.lower else other.left

            if self.upper == other.upper:
                upper = self.upper
                right = self.right if self.right == Bound.OPEN else other.right
            else:
                upper = min(self.upper, other.upper)
                right = self.right if upper == self.upper else other.right

            return self.__class__.from_atomic(left, lower, upper, right)
        else:
            intersections = []

            i_iter = iter(self)
            o_iter = iter(other)
            i_current = next(i_iter, None)
            o_current = next(o_iter, None)

            while i_current is not None and o_current is not None:
                if i_current < o_current:
                    i_current = next(i_iter, None)
                elif o_current < i_current:
                    o_current = next(o_iter, None)
                else:
                    # i_current and o_current have an overlap
                    intersections.append(i_current & o_current)

                    if i_current <= o_current:
                        # o_current can still intersect next i
                        i_current = next(i_iter, None)
                    elif o_current <= i_current:
                        # i_current can still intersect next o
                        o_current = next(o_iter, None)
                    else:
                        raise AssertionError()

            return self.__class__(*intersections)

    def __or__(self, other):
        if isinstance(other, Interval):
            return self.__class__(self, other)
        else:
            return NotImplemented

    def __contains__(self, item):
        if isinstance(item, Interval):
            if item.empty:
                return True
            elif self.upper < item.lower or self.lower > item.upper:
                # Early out for non-overlapping intervals
                return False
            elif self.atomic:
                left = item.lower > self.lower or (
                    item.lower == self.lower
                    and (item.left == self.left or self.left == Bound.CLOSED)
                )
                right = item.upper < self.upper or (
                    item.upper == self.upper
                    and (item.right == self.right or self.right == Bound.CLOSED)
                )
                return left and right
            else:
                selfiter = iter(self)
                current = next(selfiter)

                for other in item:
                    while current < other:
                        try:
                            current = next(selfiter)
                        except StopIteration:
                            return False

                    # here current and other could have an overlap
                    if other not in current:
                        return False
                return True
        else:
            # Item is a value
            if self.upper < item or self.lower > item:
                return False

            for i in self._intervals:
                left = (item >= i.lower) if i.left == Bound.CLOSED else (item > i.lower)
                right = (
                    (item <= i.upper) if i.right == Bound.CLOSED else (item < i.upper)
                )
                if left and right:
                    return True
            return False

    def __invert__(self):
        complements = [
            self.__class__.from_atomic(Bound.OPEN, -inf, self.lower, ~self.left),
            self.__class__.from_atomic(~self.right, self.upper, inf, Bound.OPEN),
        ]

        for i, j in zip(self._intervals[:-1], self._intervals[1:]):
            complements.append(
                self.__class__.from_atomic(~i.right, i.upper, j.lower, ~j.left)
            )

        return self.__class__(*complements)

    def __sub__(self, other):
        if isinstance(other, Interval):
            return self & ~other
        else:
            return NotImplemented

    def __eq__(self, other):
        if isinstance(other, Interval):
            if len(other._intervals) != len(self._intervals):
                return False

            for a, b in zip(self._intervals, other._intervals):
                eq = (
                    a.left == b.left
                    and a.lower == b.lower
                    and a.upper == b.upper
                    and a.right == b.right
                )
                if not eq:
                    return False
            return True
        else:
            return NotImplemented

    def __lt__(self, other):
        if isinstance(other, Interval):
            if self.empty or other.empty:
                return False

            if self.right == Bound.OPEN or other.left == Bound.OPEN:
                return self.upper <= other.lower
            else:
                return self.upper < other.lower
        else:
            warnings.warn(
                "Comparing an interval and a value is deprecated. "
                "Convert value to singleton first.",
                DeprecationWarning,
            )
            return not self.empty and (
                self.upper < other or (self.right == Bound.OPEN and self.upper == other)
            )

    def __gt__(self, other):
        if isinstance(other, Interval):
            if self.empty or other.empty:
                return False

            if self.left == Bound.OPEN or other.right == Bound.OPEN:
                return self.lower >= other.upper
            else:
                return self.lower > other.upper
        else:
            warnings.warn(
                "Comparing an interval and a value is deprecated. "
                "Convert value to singleton first.",
                DeprecationWarning,
            )
            return not self.empty and (
                self.lower > other or (self.left == Bound.OPEN and self.lower == other)
            )

    def __le__(self, other):
        if isinstance(other, Interval):
            if self.empty or other.empty:
                return False

            if self.right == Bound.OPEN or other.right == Bound.CLOSED:
                return self.upper <= other.upper
            else:
                return self.upper < other.upper
        else:
            warnings.warn(
                "Comparing an interval and a value is deprecated. "
                "Convert value to singleton first.",
                DeprecationWarning,
            )
            return not self.empty and self.upper <= other

    def __ge__(self, other):
        if isinstance(other, Interval):
            if self.empty or other.empty:
                return False

            if self.left == Bound.OPEN or other.left == Bound.CLOSED:
                return self.lower >= other.lower
            else:
                return self.lower > other.lower
        else:
            warnings.warn(
                "Comparing an interval and a value is deprecated. "
                "Convert value to singleton first.",
                DeprecationWarning,
            )
            return not self.empty and self.lower >= other

    def __hash__(self):
        return hash((self.lower, self.upper))

    def __repr__(self):
        if self.empty:
            return "()"

        string = []
        for interval in self._intervals:
            if interval.lower == interval.upper:
                string.append("[" + repr(interval.lower) + "]")
            else:
                string.append(
                    ("[" if interval.left == Bound.CLOSED else "(")
                    + repr(interval.lower)
                    + ","
                    + repr(interval.upper)
                    + ("]" if interval.right == Bound.CLOSED else ")")
                )
        return " | ".join(string)


class AbstractDiscreteInterval(Interval):
    """
    An abstract class for discrete interval.

    This class is not expected to be used as-is, and should be subclassed
    first. The only attribute/method that should be overriden is the `_step`
    class variable. This variable defines the step between two consecutive
    values of the discrete domain (e.g., 1 for integers).
    If a meaningfull step cannot be provided (e.g., for characters), the
    _incr and _decr class methods can be overriden. They respectively return
    the next and previous value given the current one.

    This class is still experimental and backward incompatible changes may
    occur even in minor or patch updates of portion.
    """

    _step = None

    @classmethod
    def _incr(cls, value):
        """
        Increment given value.

        :param value: value to increment.
        :return: incremented value.
        """
        return value + cls._step

    @classmethod
    def _decr(cls, value):
        """
        Decrement given value.

        :param value: value to decrement.
        :return: decremented value.
        """
        return value - cls._step

    @classmethod
    def from_atomic(cls, left, lower, upper, right):
        if left == Bound.OPEN and lower not in [-inf, inf]:
            left = Bound.CLOSED
            lower = cls._incr(lower)

        if right == Bound.OPEN and upper not in [-inf, inf]:
            right = Bound.CLOSED
            upper = cls._decr(upper)

        return super().from_atomic(left, lower, upper, right)

    @classmethod
    def _mergeable(cls, a, b):
        if a.upper <= b.upper:
            first, second = a, b
        else:
            first, second = b, a

        if first.right == Bound.CLOSED and first.upper < second.lower:
            first = Atomic(
                first.left,
                first.lower,
                cls._incr(first.upper),
                Bound.OPEN,
            )

        return super()._mergeable(first, second)
