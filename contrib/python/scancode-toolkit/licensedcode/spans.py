#
# Copyright (c) 2010 Matt Chaput. All rights reserved.
# Modifications by nexB Copyright (c) nexB Inc. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MATT CHAPUT ``AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MATT CHAPUT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of Matt Chaput.


from collections.abc import Set
from itertools import count
from itertools import groupby

from intbitset import intbitset

"""
Ranges and intervals of integers using bitmaps.
Used as a compact and faster data structure for token and position sets.
"""


class Span(Set):
    """
    Represent ranges of integers (such as tokens positions) as a set of integers.
    A Span is hashable and not meant to be modified once created, like a frozenset.
    It is equivalent to a sparse closed interval.
    Originally derived and heavily modified from Whoosh Span.
    """

    def __init__(self, *args):
        """
        Create a new Span from a start and end ints or an iterable of ints.

        First form:
        Span(start int, end int) : the span is initialized with a range(start, end+1)

        Second form:
        Span(iterable of ints) : the span is initialized with the iterable

        Spans are hashable and immutable.

        For example:
        >>> s = Span(1)
        >>> s.start
        1
        >>> s = Span([1, 2])
        >>> s.start
        1
        >>> s.end
        2
        >>> s
        Span(1, 2)

        >>> s = Span(1, 3)
        >>> s.start
        1
        >>> s.end
        3
        >>> s
        Span(1, 3)

        >>> s = Span([6, 5, 1, 2])
        >>> s.start
        1
        >>> s.end
        6
        >>> s
        Span(1, 2)|Span(5, 6)
        >>> len(s)
        4

        >>> Span([5, 6, 7, 8, 9, 10 ,11, 12]) == Span([5, 6, 7, 8, 9, 10 ,11, 12])
        True
        >>> hash(Span([5, 6, 7, 8, 9, 10 ,11, 12])) == hash(Span([5, 6, 7, 8, 9, 10 ,11, 12]))
        True
        >>> hash(Span([5, 6, 7, 8, 9, 10 ,11, 12])) == hash(Span(5, 12))
        True
        """
        len_args = len(args)

        if len_args == 0:
            self._set = intbitset()

        elif len_args == 1:
            # args0 is a single int or an iterable of ints
            if isinstance(args[0], int):
                self._set = intbitset(args)
            else:
                # some sequence or iterable
                self._set = intbitset(list(args[0]))

        elif len_args == 2:
            # args0 and args1 describe a start and end closed range
            self._set = intbitset(range(args[0], args[1] + 1))

        else:
            # args0 is a single int or args is an iterable of ints
            # args is an iterable of ints
            self._set = intbitset(list(args))

    @classmethod
    def _from_iterable(cls, it):
        return cls(list(it))

    def __len__(self):
        return len(self._set)

    def __iter__(self):
        return iter(self._set)

    def __hash__(self):
        return hash(tuple(self._set))

    def __eq__(self, other):
        return self._set == other._set

    def __and__(self, *others):
        return Span(self._set.intersection(*[o._set for o in others]))

    def __or__(self, *others):
        return Span(self._set.union(*[o._set for o in others]))

    def union(self, *others):
        """
        Return the union of this span with other spans as a new span.
        (i.e. all positions that are in either spans.)
        """
        return self.__or__(*others)

    def difference(self, *others):
        """
        Return the difference of two or more spans as a new span.
        (i.e. all positions that are in this span but not the others.)
        """
        return Span(self._set.difference(*[o._set for o in others]))

    def __repr__(self):
        """
        Return a brief representation of this span by only listing contiguous
        spans and not all items.

        For example:
        >>> Span([1, 2, 3, 4, 5, 7, 8, 9, 10])
        Span(1, 5)|Span(7, 10)
        """
        subspans_repr = []
        for subs in  self.subspans():
            ls = len(subs)
            if not ls:
                subspans_repr.append('Span()')
            elif ls == 1:
                subspans_repr.append('Span(%d)' % subs.start)
            else:
                subspans_repr.append('Span(%d, %d)' % (subs.start, subs.end))
        return '|'.join(subspans_repr)

    def __contains__(self, other):
        """
        Return True if this span contains other span (where other is a Span, an
        int or an ints set).

        For example:
        >>> Span([5, 7]) in Span(5, 7)
        True
        >>> Span([5, 8]) in Span([5, 7])
        False
        >>> 6 in Span([4, 5, 6, 7, 8])
        True
        >>> 2 in Span([4, 5, 6, 7, 8])
        False
        >>> 8 in Span([4, 8])
        True
        >>> 5 in Span([4, 8])
        False
        >>> set([4, 5]) in Span([4, 5, 6, 7, 8])
        True
        >>> set([9]) in Span([4, 8])
        False
        """
        if isinstance(other, Span):
            return self._set.issuperset(other._set)

        if isinstance(other, int):
            return self._set.__contains__(other)

        if isinstance(other, (set, frozenset)):
            return self._set.issuperset(intbitset(other))

        if isinstance(other, intbitset):
            return self._set.issuperset(other)

    @property
    def set(self):
        return self._set

    def issubset(self, other):
        return self._set.issubset(other._set)

    def issuperset(self, other):
        return self._set.issuperset(other._set)

    @property
    def start(self):
        if not self._set:
            raise TypeError('Empty Span has no start.')
        return self._set[0]

    @property
    def end(self):
        if not self._set:
            raise TypeError('Empty Span has no end.')
        return self._set[-1]

    @classmethod
    def sort(cls, spans):
        """
        Return a new sorted sequence of spans given a sequence of spans.
        The primary sort is on start. The secondary sort is on length.
        If two spans have the same start, the longer span will sort first.

        For example:
        >>> spans = [Span([5, 6, 7, 8, 9, 10]), Span([1, 2]), Span([3, 4, 5]), Span([3, 4, 5, 6]), Span([8, 9, 10])]
        >>> Span.sort(spans)
        [Span(1, 2), Span(3, 6), Span(3, 5), Span(5, 10), Span(8, 10)]

        >>> spans = [Span([1, 2]), Span([3, 4, 5]), Span([3, 4, 5, 6]), Span([8, 9, 10])]
        >>> Span.sort(spans)
        [Span(1, 2), Span(3, 6), Span(3, 5), Span(8, 10)]

        >>> spans = [Span([1, 2]), Span([4, 5]), Span([7, 8]), Span([11, 12])]
        >>> Span.sort(spans)
        [Span(1, 2), Span(4, 5), Span(7, 8), Span(11, 12)]

        >>> spans = [Span([1, 2]), Span([7, 8]), Span([5, 6]), Span([12, 13])]
        >>> Span.sort(spans)
        [Span(1, 2), Span(5, 6), Span(7, 8), Span(12, 13)]

        """
        key = lambda s: (s.start, -len(s),)
        return sorted(spans, key=key)

    def magnitude(self):
        """
        Return the maximal length represented by this span start and end. The
        magnitude is the same as the length for a contiguous span. It will be
        greater than the length for a span with non-contiguous int items.
        An empty span has a zero magnitude.

        For example:
        >>> Span([4, 8]).magnitude()
        5
        >>> len(Span([4, 8]))
        2
        >>> len(Span([4, 5, 6, 7, 8]))
        5

        >>> Span([4, 5, 6, 14 , 12, 128]).magnitude()
        125

        >>> Span([4, 5, 6, 7, 8]).magnitude()
        5
        >>> Span([0]).magnitude()
        1
        >>> Span([0]).magnitude()
        1
        """
        if not self._set:
            return 0
        return self.end - self.start + 1

    def density(self):
        """
        Return the density of this span as a ratio of its length to its
        magnitude, a float between 0 and 1. A dense Span has all its integer
        items contiguous and a maximum density of one. A sparse low density span
        has some non-contiguous integer items. An empty span has a zero density.

        For example:
        >>> Span([4, 8]).density()
        0.4
        >>> Span([4, 5, 6, 7, 8]).density()
        1.0
        >>> Span([0]).density()
        1.0
        >>> Span().density()
        0
        """
        if not self._set:
            return 0
        return len(self) / self.magnitude()

    def overlap(self, other):
        """
        Return the count of overlapping items between this span and other span.

        For example:
        >>> Span([1, 2]).overlap(Span([5, 6]))
        0
        >>> Span([5, 6]).overlap(Span([5, 6]))
        2
        >>> Span([4, 5, 6, 7]).overlap(Span([5, 6]))
        2
        >>> Span([4, 5, 6]).overlap(Span([5, 6, 7]))
        2
        >>> Span([4, 5, 6]).overlap(Span([6]))
        1
        >>> Span([4, 5]).overlap(Span([6, 7]))
        0
        """
        return len(self & other)

    def resemblance(self, other):
        """
        Return a resemblance coefficient as a float between 0 and 1.
        0 means the spans are completely different and 1 identical.
        """
        if self._set.isdisjoint(other._set):
            return 0
        if self._set == other._set:
            return 1
        resemblance = self.overlap(other) / len(self | other)
        return resemblance

    def containment(self, other):
        """
        Return a containment coefficient as a float between 0 and 1. This is an
        indication of how much of the other span is contained in this span.
            - 1 means the other span is entirely contained in this span.
            - 0 means that the other span is not contained at all this span.
        """
        if self._set.isdisjoint(other._set):
            return 0
        if self._set == other._set:
            return 1
        containment = self.overlap(other) / len(other)
        return containment

    def surround(self, other):
        """
        Return True if this span surrounds other span.
        This is different from containment. A span can surround another span region
        and have no positions in common with the surrounded.

        For example:
        >>> Span([4, 8]).surround(Span([4, 8]))
        True
        >>> Span([3, 9]).surround(Span([4, 8]))
        True
        >>> Span([5, 8]).surround(Span([4, 8]))
        False
        >>> Span([4, 7]).surround(Span([4, 8]))
        False
        >>> Span([4, 5, 6, 7, 8]).surround(Span([5, 6, 7]))
        True
        """
        return self.start <= other.start and self.end >= other.end

    def is_before(self, other):
        return self.end < other.start

    def is_after(self, other):
        return self.start > other.end

    def touch(self, other):
        """
        Return True if self sequence is contiguous with other span without overlap.

        For example:
        >>> Span([5, 7]).touch(Span([5]))
        False
        >>> Span([5, 7]).touch(Span([5, 8]))
        False
        >>> Span([5, 7]).touch(Span([7, 8]))
        False
        >>> Span([5, 7]).touch(Span([8, 9]))
        True
        >>> Span([8, 9]).touch(Span([5, 7]))
        True
        """
        return self.start == other.end + 1 or self.end == other.start - 1

    def distance_to(self, other):
        """
        Return the absolute positive distance from this span to other span.
        Touching and overlapping spans have a zero distance.

        For example:
        >>> Span([8, 9]).distance_to(Span([5, 7]))
        0
        >>> Span([5, 7]).distance_to(Span([8, 9]))
        0
        >>> Span([5, 6]).distance_to(Span([8, 9]))
        2
        >>> Span([5, 7]).distance_to(Span([5, 7]))
        0
        >>> Span([4, 5, 6]).distance_to(Span([5, 6, 7]))
        0
        >>> Span([5, 7]).distance_to(Span([10, 12]))
        3
        >>> Span([1, 2]).distance_to(Span(range(4, 52)))
        2
        """
        if self.overlap(other) or self.touch(other):
            return 0
        elif self.is_before(other):
            return other.start - self.end
        else:
            return self.start - other.end

    @staticmethod
    def from_ints(ints):
        """
        Return a sequence of Spans from an iterable of ints. A new Span is
        created for each group of monotonously increasing int items.

        >>> Span.from_ints([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
        [Span(1, 12)]
        >>> Span.from_ints([1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12])
        [Span(1, 3), Span(5, 12)]
        >>> Span.from_ints([0, 2, 3, 5, 6, 7, 8, 9, 10, 11, 13])
        [Span(0), Span(2, 3), Span(5, 11), Span(13)]
        """
        ints = sorted(set(ints))
        groups = (group for _, group in groupby(ints, lambda group, c=count(): next(c) - group))
        return [Span(g) for g in groups]

    def subspans(self):
        """
        Return a list of Spans creating one new Span for each set of contiguous
        integer items.

        For example:
        >>> span = Span(5, 6, 7, 8, 9, 10) | Span([1, 2]) | Span(3, 5) | Span(3, 6) | Span([8, 9, 10])
        >>> span.subspans()
        [Span(1, 10)]

        When subspans are not touching they do not merge :
        >>> span = Span([63, 64]) | Span([58, 58])
        >>> span.subspans()
        [Span(58), Span(63, 64)]

        Overlapping subspans are merged as needed:
        >>> span = Span([12, 17, 24]) | Span([15, 16, 17, 35]) | Span(58) | Span(63, 64)
        >>> span.subspans()
        [Span(12), Span(15, 17), Span(24), Span(35), Span(58), Span(63, 64)]
        """
        return Span.from_ints(self)
