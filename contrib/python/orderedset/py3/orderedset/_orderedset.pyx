# cython: embedsignature=True
import sys

if sys.version_info[0] == 2:
    from itertools import izip
    from collections import Set, MutableSet, Iterable
else:
    izip = zip
    from collections.abc import Set, MutableSet, Iterable

from cpython cimport PyDict_Contains, PyIndex_Check


cdef extern from "Python.h":
    int PySlice_GetIndicesEx(slice, ssize_t length, ssize_t *start, ssize_t *stop, ssize_t *step, ssize_t *slicelength) except -1


__all__ = ["OrderedSet"]


cdef class entry:
    cdef object key
    cdef entry prev
    cdef entry next


cdef inline void _add(_OrderedSet oset, object key):
    cdef entry end = oset.end
    cdef dict map = oset.map
    cdef entry next

    if not PyDict_Contains(map, key):
        next = entry()
        next.key, next.prev, next.next = key, end.prev, end
        end.prev.next = end.prev = map[key] = next
        oset.os_used += 1


cdef void _discard(_OrderedSet oset, object key):
    cdef dict map = oset.map
    cdef entry _entry

    if PyDict_Contains(map, key):
        _entry = map.pop(key)
        _entry.prev.next = _entry.next
        _entry.next.prev = _entry.prev
        oset.os_used -= 1


cdef inline object  _isorderedsubset(seq1, seq2):
    if not len(seq1) <= len(seq2):
            return False
    for self_elem, other_elem in izip(seq1, seq2):
        if not self_elem == other_elem:
            return False
    return True


cdef class OrderedSetIterator(object):
    cdef _OrderedSet oset
    cdef entry curr
    cdef ssize_t si_used

    def __cinit__(self, _OrderedSet oset):
        self.oset = oset
        self.curr = oset.end
        self.si_used = oset.os_used

    def __iter__(self):
        return self

    def __next__(self):
        cdef entry item

        if self.si_used != self.oset.os_used:
            # make this state sticky
            self.si_used = -1
            raise RuntimeError('%s changed size during iteration' % type(self.oset).__name__)

        item = self.curr.next
        if item == self.oset.end:
            raise StopIteration()
        self.curr = item
        return item.key


cdef class OrderedSetReverseIterator(object):
    cdef _OrderedSet oset
    cdef entry curr
    cdef ssize_t si_used

    def __cinit__(self, _OrderedSet oset):
        self.oset = oset
        self.curr = oset.end
        self.si_used = oset.os_used

    def __iter__(self):
        return self

    def __next__(self):
        cdef entry item

        if self.si_used != self.oset.os_used:
            # make this state sticky
            self.si_used = -1
            raise RuntimeError('%s changed size during iteration' % type(self.oset).__name__)

        item = self.curr.prev
        if item is self.oset.end:
            raise StopIteration()
        self.curr = item
        return item.key


cdef class _OrderedSet(object):
    cdef dict map
    cdef entry end
    cdef ssize_t os_used

    def __cinit__(self):
        self.map = {}
        self.os_used = 0
        self.end = end = entry()
        end.prev = end.next = end

    def __init__(self, object iterable=None):
        cdef dict map = self.map
        cdef entry end = self.end
        cdef entry next

        if iterable is not None:
            for elem in iterable:
                if not PyDict_Contains(map, elem):
                    next = entry()
                    next.key, next.prev, next.next = elem, end.prev, end
                    end.prev.next = end.prev = map[elem] = next
                    self.os_used += 1

    @classmethod
    def _from_iterable(cls, it):
        return cls(it)

    ##
    # set methods
    ##
    cpdef add(self, elem):
        """Add element `elem` to the set."""
        _add(self, elem)

    cpdef discard(self, elem):
        """Remove element `elem` from the ``OrderedSet`` if it is present."""
        _discard(self, elem)

    cpdef pop(self, last=True):
        """Remove last element. Raises ``KeyError`` if the ``OrderedSet`` is empty."""
        if not self:
            raise KeyError('OrderedSet is empty')
        key = self.end.prev.key if last else self.end.next.key
        _discard(self, key)
        return key

    def remove(self, elem):
        """
        Remove element `elem` from the ``set``.
        Raises :class:`KeyError` if `elem` is not contained in the set.
        """
        if elem not in self:
            raise KeyError(elem)
        _discard(self, elem)

    def clear(self):
        """Remove all elements from the `set`."""
        cdef entry end = self.end
        end.next.prev = end.next = None

        # reinitialize
        self.map = {}
        self.os_used = 0
        self.end = end = entry()
        end.prev = end.next = end

    def copy(self):
        """
        :rtype: OrderedSet
        :return: a new ``OrderedSet`` with a shallow copy of self.
        """
        return self._from_iterable(self)

    def difference(self, other):
        """``OrderedSet - other``

        :rtype: OrderedSet
        :return: a new ``OrderedSet`` with elements in the set that are not in the others.
        """
        return self - other

    def difference_update(self, other):
        """``OrderedSet -= other``

        Update the ``OrderedSet``, removing elements found in others.
        """
        self -= other

    def __sub__(self, other):
        """
        :rtype: OrderedSet
        """
        ostyp = type(self if isinstance(self, OrderedSet) else other)

        if not isinstance(self, Iterable):
            return NotImplemented
        if not isinstance(other, Set):
            if not isinstance(other, Iterable):
                return NotImplemented
            other = ostyp._from_iterable(other)

        return ostyp._from_iterable(value for value in self if value not in other)

    def __isub__(self, other):
        if other is self:
            self.clear()
        else:
            for value in other:
                self.discard(value)
        return self

    def intersection(self, other):
        """``OrderedSet & other``

        :rtype: OrderedSet
        :return: a new ``OrderedSet`` with elements common to the set and all others.
        """
        return self & other

    def intersection_update(self, other):
        """``OrderedSet &= other``

        Update the ``OrderedSet``, keeping only elements found in it and all others.
        """
        self &= other

    def __and__(self, other):
        """
        :rtype: OrderedSet
        """
        ostyp = type(self if isinstance(self, OrderedSet) else other)

        if not isinstance(self, Iterable):
            return NotImplemented
        if not isinstance(other, Set):
            if not isinstance(other, Iterable):
                return NotImplemented
            other = ostyp._from_iterable(other)

        return ostyp._from_iterable(value for value in self if value in other)

    def __iand__(self, it):
        for value in (self - it):
            self.discard(value)
        return self

    def isdisjoint(self, other):
        """
        Return True if the set has no elements in common with other.
        Sets are disjoint if and only if their intersection is the empty set.

        :rtype: bool
        """
        for value in other:
            if value in self:
                return False
        return True

    def issubset(self, other):
        """``OrderedSet <= other``

        :rtype: bool

        Test whether the ``OrderedSet`` is a proper subset of other, that is,
        ``OrderedSet <= other and OrderedSet != other``.
        """
        return self <= other

    def issuperset(self, other):
        """``OrderedSet >= other``

        :rtype: bool

        Test whether every element in other is in the set.
        """
        return other <= self

    def isorderedsubset(self, other):
        return _isorderedsubset(self, other)

    def isorderedsuperset(self, other):
        return _isorderedsubset(other, self)

    def symmetric_difference(self, other):
        """``OrderedSet ^ other``

        :rtype: OrderedSet
        :return: a new ``OrderedSet`` with elements in either the set or other but not both.
        """
        return self ^ other

    def symmetric_difference_update(self, other):
        """``OrderedSet ^= other``

        Update the ``OrderedSet``, keeping only elements found in either set, but not in both.
        """
        self ^= other

    def __xor__(self, other):
        """
        :rtype: OrderedSet
        """
        if not isinstance(self, Iterable):
            return NotImplemented
        if not isinstance(other, Iterable):
            return NotImplemented

        return (self - other) | (other - self)

    def __ixor__(self, other):
        if other is self:
            self.clear()
        else:
            if not isinstance(other, Set):
                other = self._from_iterable(other)
            for value in other:
                if value in self:
                    self.discard(value)
                else:
                    self.add(value)
        return self

    def union(self, other):
        """``OrderedSet | other``

        :rtype: OrderedSet
        :return: a new ``OrderedSet`` with elements from the set and all others.
        """
        return self | other

    def update(self, other):
        """``OrderedSet |= other``

        Update the ``OrderedSet``, adding elements from all others.
        """
        self |= other

    def __or__(self, other):
        """
        :rtype: OrderedSet
        """
        ostyp = type(self if isinstance(self, OrderedSet) else other)

        if not isinstance(self, Iterable):
            return NotImplemented
        if not isinstance(other, Iterable):
            return NotImplemented
        chain = (e for s in (self, other) for e in s)
        return ostyp._from_iterable(chain)

    def __ior__(self, other):
        for elem in other:
            _add(self, elem)
        return self

    ##
    # list methods
    ##
    def index(self, elem):
        """Return the index of `elem`. Rases :class:`ValueError` if not in the OrderedSet."""
        if elem not in self:
            raise ValueError("%s is not in %s" % (elem, type(self).__name__))
        cdef entry curr = self.end.next
        cdef ssize_t index = 0
        while curr.key != elem:
            curr = curr.next
            index += 1
        return index

    cdef _getslice(self, slice item):
        cdef ssize_t start, stop, step, slicelength, place, i
        cdef entry curr
        cdef _OrderedSet result
        PySlice_GetIndicesEx(item, len(self), &start, &stop, &step, &slicelength)

        result = type(self)()
        place = start
        curr = self.end

        if slicelength <= 0:
            pass
        elif step > 0:
            # normal forward slice
            i = 0
            while slicelength > 0:
                while i <= place:
                    curr = curr.next
                    i += 1
                _add(result, curr.key)
                place += step
                slicelength -= 1
        else:
            # we're going backwards
            i = len(self)
            while slicelength > 0:
                while i > place:
                    curr = curr.prev
                    i -= 1
                _add(result, curr.key)
                place += step
                slicelength -= 1
        return result

    cdef _getindex(self, ssize_t index):
        cdef ssize_t _len = len(self)
        if index >= _len or (index < 0 and abs(index) > _len):
            raise IndexError("list index out of range")

        cdef entry curr
        if index >= 0:
            curr = self.end.next
            while index:
                curr = curr.next
                index -= 1
        else:
            index = abs(index) - 1
            curr = self.end.prev
            while index:
                curr = curr.prev
                index -= 1
        return curr.key

    def __getitem__(self, item):
        """Return the `elem` at `index`. Raises :class:`IndexError` if `index` is out of range."""
        if isinstance(item, slice):
            return self._getslice(item)
        if not PyIndex_Check(item):
            raise TypeError("%s indices must be integers, not %s" % (type(self).__name__, type(item)))
        return self._getindex(item)

    ##
    # sequence methods
    ##
    def __len__(self):
        return len(self.map)

    def __contains__(self, elem):
        return elem in self.map

    def __iter__(self):
        return OrderedSetIterator(self)

    def __reversed__(self):
        return OrderedSetReverseIterator(self)

    def __reduce__(self):
        items = list(self)
        inst_dict = vars(self).copy()
        return self.__class__, (items, ), inst_dict


class OrderedSet(_OrderedSet, MutableSet):
    """
    An ``OrderedSet`` object is an ordered collection of distinct hashable objects.

    It works like the :class:`set` type, but remembers insertion order.

    It also supports :meth:`__getitem__` and :meth:`index`, like the
    :class:`list` type.
    """
    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, (_OrderedSet, list)):
            return len(self) == len(other) and list(self) == list(other)
        elif isinstance(other, Set):
            return set(self) == set(other)
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, Set):
            return len(self) <= len(other) and set(self) <= set(other)
        elif isinstance(other, list):
            return len(self) <= len(other) and list(self) <= list(other)
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, Set):
            return len(self) < len(other) and set(self) < set(other)
        elif isinstance(other, list):
            return len(self) < len(other) and list(self) < list(other)
        return NotImplemented

    def __ge__(self, other):
        ret = self < other
        if ret is NotImplemented:
            return ret
        return not ret

    def __gt__(self, other):
        ret = self <= other
        if ret is NotImplemented:
            return ret
        return not ret
