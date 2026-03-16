
import copy
import re
import sys
from itertools import chain, count, groupby

__all__ = 'intspan spanlist intspanlist TheRest ParseError'.split()

_PY2 = sys.version_info[0] == 2
if not _PY2:
    basestring = str


# Define regular expressions for spans and spans that may contain
# star (TheRest) markers
SPANRE = re.compile(r'^\s*(?P<start>-?\d+)\s*(-\s*(?P<stop>-?\d+))?\s*$')
SPANRESTAR = re.compile(
    r'^\s*((?P<star>\*)|(?P<start>-?\d+)\s*(-\s*(?P<stop>-?\d+))?)\s*$')


class ParseError(ValueError):
    """Exception raised if string specification can't be parsed."""
    pass


class Rester(object):
    """
    Singleton to represent "the rest of the values."
    """

    def __repr__(self):
        return 'TheRest'

    def __str__(self):
        return '*'


TheRest = Rester()


def _parse_range(datum):

    """
    Parser for intspan and intspan list.
    """

    def parse_chunk(chunk):
        """
        Parse each comma-separated chunk. Hyphens (-) can indicate ranges,
        or negative numbers. Returns a list of specified values. NB Designed
        to parse correct input correctly. Results of incorrect input are
        undefined.
        """
        m = SPANRESTAR.search(chunk)
        if m:
            if m.group('star'):
                return [TheRest]
            start = int(m.group('start'))
            if not m.group('stop'):
                return [start]
            stop = int(m.group('stop'))
            if start > stop:
                raise ParseError('start value should exceed stop ({0})'.format(chunk))
            return list(range(start, stop + 1))
        else:
            raise ParseError("Can't parse chunk '{0}'".format(chunk))

    if isinstance(datum, basestring):
        result = []
        for part in datum.split(','):
            chunk = part.strip()
            if chunk:
                result.extend(parse_chunk(chunk))
        return result

    return datum if hasattr(datum, '__iter__') else [datum]


def spanlist(spec=None):
    """
    Given a string specification like the ones given to ``intspan``,
    return a list of the included items, in the same item given. Thus,
    ``spanlist("3,1-4")`` yields ``[3, 1, 2, 4]``. Experimental partial
    implementation of ability to have ordered intspans.
    """
    if spec is None or (isinstance(spec, basestring) and spec.strip() == ''):
        return []
    rawitems = _parse_range(spec)
    seen = set()
    items = []
    for i in rawitems:
        if i in seen:
            continue
        items.append(i)
        seen.add(i)
    return items


def _as_range(iterable):
    """
    Return a tuple representing the bounds of the range.
    """
    l = list(iterable)
    return (l[0], l[-1])


def _as_range_str(iterable):
    """
    Return a string representing the range as a string span.
    """
    l = list(iterable)
    if len(l) > 1:
        return '{0}-{1}'.format(l[0], l[-1])
    return '{0}'.format(l[0])


def _noRestDiff(a, b):
    """
    Special difference that, in case difference cannot be computed
    because of ``TypeError`` (indicating that a ``Rester`` object)
    has been found), returns a difference indicating the spanned items
    / group has ended.
    """
    try:
        return a - b
    except TypeError:
        return 2  # anything more than 1 signals "the next thing is in
                  # another span, not this current one"


class intspan(set):  # pylint: disable=invalid-name
    """
    A set of integers, expressed as an ordered sequence of spans.
    Because ``intspan('1-3,14,29,92-97')`` is clearer than
    ``[1, 2, 3, 14, 29, 92, 93, 94, 95, 96, 97]``.
    """

    def __init__(self, initial=None):
        """
        Construct a new ``intspan``.

        :param iterable|str initial: Optional initial list of items.
        :returns: the intspan
        :rtype: intspan
        """
        super(intspan, self).__init__()
        if initial:
            self.update(initial)

    def copy(self):
        """
        Return a new set with the same members.
        """
        return copy.copy(self)

    def update(self, items):
        """
        Add multiple items.

        :param iterable|str items: Items to add. May be an intspan-style string.
        """
        super(intspan, self).update(_parse_range(items))
        return self

    def intersection_update(self, items):
        super(intspan, self).intersection_update(_parse_range(items))
        return self

    def difference_update(self, items):
        super(intspan, self).difference_update(_parse_range(items))
        return self

    def symmetric_difference_update(self, items):
        super(intspan, self).symmetric_difference_update(
            _parse_range(items))
        return self

    def discard(self, items):
        """
        Discard items.

        :param iterable|str items: Items to remove. May be an intspan-style string.
        """
        for item in _parse_range(items):
            super(intspan, self).discard(item)

    def remove(self, items):
        for item in _parse_range(items):
            super(intspan, self).remove(item)

    def add(self, items):
        """
        Add items.

        :param iterable|str items: Items to add. May be an intspan-style string.
        """
        for item in _parse_range(items):
            super(intspan, self).add(item)

    def issubset(self, items):
        return super(intspan, self).issubset(_parse_range(items))

    def issuperset(self, items):
        return super(intspan, self).issuperset(_parse_range(items))

    def union(self, items):
        return intspan(super(intspan, self).union(_parse_range(items)))

    def intersection(self, items):
        return intspan(super(intspan, self).intersection(_parse_range(items)))

    def difference(self, items):
        return intspan(super(intspan, self).difference(_parse_range(items)))

    def symmetric_difference(self, items):
        return intspan(super(intspan, self).symmetric_difference(_parse_range(items)))

    __le__   = issubset
    __ge__   = issuperset
    __or__   = union
    __and__  = intersection
    __sub__  = difference
    __xor__  = symmetric_difference
    __ior__  = update
    __iand__ = intersection_update
    __isub__ = difference_update
    __ixor__ = symmetric_difference_update

    def __eq__(self, items):
        return super(intspan, self).__eq__(_parse_range(items))

    def __lt__(self, items):
        return super(intspan, self).__lt__(_parse_range(items))

    def __gt__(self, items):
        return super(intspan, self).__gt__(_parse_range(items))

    def __iter__(self):
        """
        Iterate in ascending order.
        """
        return iter(sorted(super(intspan, self).__iter__()))

    def pop(self):
        """
        Remove and return an arbitrary element; raises KeyError if empty.

        :returns: Arbitrary member of the set (which is removed)
        :rtype: int
        :raises KeyError: If the set is empty.
        """
        if self:
            min_item = min(self)
            self.discard(min_item)
            return min_item
        else:
            raise KeyError('pop from an empty set')

        # This method added only for PyPy, which otherwise would get the wrong
        # answer (unordered).


    def universe(self, low=None, high=None):
        """
        Return the "universe" or "covering set" of the given intspan--that
        is, all of the integers between its minimum and missing values.
        Optionally allows the bounds of the universe set to be manually specified.

        :param int low: Low bound of universe.
        :param int high: High bound of universe.
        :returns: the universe or covering set
        :rtype: intspan
        """
        if not self and low is None and high is None:
            return intspan()
        low = low if low is not None else min(self)
        high = high if high is not None else max(self)
        universe = self.__class__.from_range(low, high)
        return universe

    def complement(self, low=None, high=None):
        """
        Return the complement of the given intspan--that is, all of the
        'missing' elements between its minimum and missing values.
        Optionally allows the universe set to be manually specified.

        :param int low: Low bound of universe to complement against.
        :param int high: High bound of universe to complement against.
        :returns: the complement set
        :rtype: intspan
        :raises ValueError: if the set is empty (thus the compelement is infinite)
        """
        if not self:
            raise ValueError('cannot represent infinite set')
        return self.universe(low, high) - self

    @classmethod
    def from_range(cls, low, high):
        """
        Construct an intspan from the low value to the high value,
        inclusive. I.e., closed range, not the more typical Python
        half-open range.

        :param int low: Low bound of set to construct.
        :param int high: High bound of set to construct.
        :returns: New intspan low-high.
        :rtype: intspan
        """
        return cls(range(low, high + 1))

    @classmethod
    def from_ranges(cls, ranges):
        """
        Construct an intspan from a sequence of (low, high) value
        sequences (lists or tuples, say). Note that these values are
        inclusive, closed ranges, not the more typical Python
        half-open ranges.

        :param list ranges: List of closed/inclusive ranges, each a tuple.
        :returns: intspan combining the ranges
        :rtype: intspan
        """
        return cls(chain(*(range(r[0], r[1] + 1) for r in ranges)))

    def __repr__(self):
        """
        Return the representation.
        """
        clsname = self.__class__.__name__
        return '{0}({1!r})'.format(clsname, self.__str__())

    def __str__(self):
        """
        Return the stringification.
        """
        items = sorted(self)
        gk = lambda n, c=count(): n - next(c)
        return ','.join(_as_range_str(g) for _, g in groupby(items, key=gk))

    def ranges(self):
        """
        Return a list of the set's contiguous (inclusive) ranges.

        :returns: List of all contained ranges.
        :rtype: list
        """
        items = sorted(self)
        gk = lambda n, c=count(): n - next(c)
        return [_as_range(g) for _, g in groupby(items, key=gk)]

    # see Jeff Mercado's answer to http://codereview.stackexchange.com/questions/5196/grouping-consecutive-numbers-into-ranges-in-python-3-2
    # see also: http://stackoverflow.com/questions/2927213/python-finding-n-consecutive-numbers-in-a-list


# It might be interesting to have a metaclass factory that could create
# spansets of things other than integers. For example, enumerateds defined
# by giving a universe of possible options. Or characters. The Ranger
# package seems to do some of this http://pythonhosted.org/ranger/


class intspanlist(list):  # pylint: disable=invalid-name
    """
    An ordered version of ``intspan``. Is to ``list`` what ``intspan``
    is to ``set``, except that it is somewhat set-like, in that items
    are not intended to be repeated. Works fine as an immutable
    data structure. Still some issues if one mutates an instance. Not
    terrible problems, but the set-like nature where there is only
    one entry for each included integer may be broken.
    """

    def __init__(self, initial=None, universe=None):
        """
        Construct a new ``intspanlist``

        :param iterable|str initial: Optional initial list of items.
        :returns: the intspanlist
        :rtype: intspanlist
        """
        super(intspanlist, self).__init__()
        if initial:
            self.extend(initial)
        if universe is not None:
            try:
                restIndex = self.index(TheRest)
                remaining = sorted(intspan(universe) - set(self))
                self[restIndex + 1:restIndex + 1] = remaining  # splice
                self.pop(restIndex)
            except ValueError:
                pass

    def therest_update(self, universe, inplace=True):
        """
        If the receiving ``intspanlist`` contains a ``TheRest`` marker,
        replace it with the contents of the universe. Generally done
        *in situ*, but if value of ``inplace`` kwarg false, returns
        an edited copy.
        """
        toedit = self if inplace else self.copy()
        try:
            restIndex = toedit.index(TheRest)
            remaining = sorted(intspan(universe) - set(toedit))
            toedit[restIndex + 1:restIndex + 1] = remaining  # splice
            toedit.pop(restIndex)
        except ValueError:
            pass
        return toedit

    def copy(self):
        """
        Return a copy of the intspanlist.
        """
        return copy.copy(self)

    def append(self, item):
        """
        Add to the end of the intspanlist

        :param int item: Item to add
        """
        self.extend(spanlist(item))

    def extend(self, items):
        """
        Add a collection to the end of the intspanlist

        :param iterable items: integers to add
        """
        seen = set(self)
        for newitem in spanlist(items):
            if newitem in seen:
                continue
            super(intspanlist, self).append(newitem)

    def __eq__(self, items):
        return super(intspanlist, self).__eq__(spanlist(items))

    def __lt__(self, items):
        return super(intspanlist, self).__lt__(spanlist(items))

    def __gt__(self, items):
        return super(intspanlist, self).__gt__(spanlist(items))

    def __add__(self, other):
        return intspanlist(list(self) + list(other))

    def __sub__(self, other):
        result = self[:]
        for o in other:
            try:
                result.remove(o)
            except ValueError:
                pass
        return result

    def __iadd__(self, other):
        self.extend(other)
        return self

    def __isub__(self, other):
        for o in other:
            try:
                self.remove(o)
            except ValueError:
                pass
        return self

    def __radd__(self, other):
        return intspanlist(list(other) + list(self))

    def __rsub__(self, other):
        raise NotImplementedError("operation doesn't make sense")

    def complement(self, low=None, high=None):
        """
        Return the complement of the given intspanlist--that is, all of the
        'missing' elements between its minimum and missing values.
        Optionally allows the universe set to be manually specified.

        :param int low: Low bound of universe to complement against.
        :param int high: High bound of universe to complement against.
        :returns: the complement set
        :rtype: intspanlist
        :raises ValueError: if the set is empty (thus the compelement is infinite)
        """
        cls = self.__class__
        if not self:
            raise ValueError('cannot represent infinite set')
        low = low if low is not None else min(self)
        high = high if high is not None else max(self)
        universe = cls.from_range(low, high)
        result = []
        contained = set(self)
        for x in universe:
            if x not in contained:
                result.append(x)
        return cls(result)

    @classmethod
    def from_range(cls, low, high):
        """
        Construct an intspanlist from the low value to the high value,
        inclusive. I.e., closed range, not the more typical Python
        half-open range.

        :param int low: Low bound.
        :param int high: High bound.
        :returns: New intspanlist low-high.
        :rtype: intspanlist
        """
        return cls(range(low, high + 1))

    @classmethod
    def from_ranges(cls, ranges):
        """
        Construct an intspanlist from a sequence of (low, high) value
        sequences (lists or tuples, say). Note that these values are
        inclusive, closed ranges, not the more typical Python
        half-open ranges.


        :param list ranges: List of closed/inclusive ranges, each a tuple.
        :returns: intspanlist combining the ranges
        :rtype: intspanlist
        """
        return cls(chain(*(range(r[0], r[1] + 1) for r in ranges)))

    def __repr__(self):
        """
        Return the representation.
        """
        clsname = self.__class__.__name__
        return '{0}({1!r})'.format(clsname, self.__str__())

    def __str__(self):
        """
        Return the stringification.
        """
        gk = lambda n, c=count(): _noRestDiff(n, next(c))
        return ','.join(_as_range_str(g) for _, g in groupby(self, key=gk))

    def ranges(self):
        """
        Return a list of the set's contiguous (inclusive) ranges.
        """
        gk = lambda n, c=count(): _noRestDiff(n, next(c))
        return [_as_range(g) for _, g in groupby(self, key=gk)]
