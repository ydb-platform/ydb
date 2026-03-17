# This file is part of Invenio.
# Copyright (C) 2007, 2008, 2009, 2010, 2011, 2013, 2014, 2015, 2016 CERN.
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

# cython: infer_types=True
# cython: language_level=2

"""
Defines an intbitset data object to hold unordered sets of unsigned
integers with ultra fast set operations, implemented via bit vectors
and Python C extension to optimize speed and memory usage.

Emulates the Python built-in set class interface with some additional
specific methods such as its own fast dump and load marshalling
functions.  Uses real bits to optimize memory usage, so may have
issues with endianness if you transport serialized bitsets between
various machine architectures.

Please note that no bigger than __maxelem__ elements can be added to
an intbitset and, if CFG_INTBITSET_ENABLE_SANITY_CHECKS is disabled,
you will receive unpredictable results.

Note to developers: If you make modification to this file you
have to manually regenerate intbitset.c by running:
  $ cython intbitset.pyx
and then commit generated intbitset.c.
"""

#cython: infer_types=True

import sys
import zlib
from array import array
from cpython.buffer cimport PyBUF_SIMPLE, Py_buffer, PyObject_GetBuffer, PyBuffer_Release
from cpython.ref cimport PyObject

CFG_INTBITSET_ENABLE_SANITY_CHECKS = False
from intbitset_helper import _
from intbitset_version import __version__

__all__ = ['intbitset', '__version__']

cdef extern from *:
    ## See: <http://wiki.cython.org/FAQ/#HowdoIuse.27const.27.3F>
    ## In order to avoid warnings with PyObject_AsReadBuffer
    ctypedef void* const_void_ptr "const void*"

cdef extern from "intbitset.h":
    ctypedef int Py_ssize_t
    object PyBytes_FromStringAndSize(char *s, Py_ssize_t len)
    object PyString_FromStringAndSize(char *s, Py_ssize_t len)

cdef extern from "intbitset.h":
    ctypedef unsigned long long int word_t
    #ctypedef unsigned char bool_t
    ctypedef struct IntBitSet:
        int size
        int allocated
        word_t trailing_bits
        int tot
        word_t *bitset
    int wordbytesize
    int wordbitsize
    int maxelem
    IntBitSet *intBitSetCreate(int size, bint trailing_bits)
    IntBitSet *intBitSetCreateFromBuffer(void *buf, int bufsize)
    IntBitSet *intBitSetResetFromBuffer(IntBitSet *bitset, void *buf, int bufsize)
    IntBitSet *intBitSetReset(IntBitSet *bitset)
    void intBitSetDestroy(IntBitSet *bitset)
    IntBitSet *intBitSetClone(IntBitSet *bitset)
    int intBitSetGetSize(IntBitSet *bitset)
    int intBitSetGetAllocated(IntBitSet *bitset)
    int intBitSetGetTot(IntBitSet * bitset)
    bint intBitSetIsInElem(IntBitSet *bitset, unsigned int elem)
    void intBitSetAddElem(IntBitSet *bitset, unsigned int elem)
    void intBitSetDelElem(IntBitSet *bitset, unsigned int elem)
    bint intBitSetEmpty(IntBitSet *bitset)
    IntBitSet *intBitSetUnion(IntBitSet *x, IntBitSet *y)
    IntBitSet *intBitSetIntersection(IntBitSet *x, IntBitSet *y)
    IntBitSet *intBitSetSub(IntBitSet *x, IntBitSet *y)
    IntBitSet *intBitSetXor(IntBitSet *x, IntBitSet *y)
    IntBitSet *intBitSetIUnion(IntBitSet *dst, IntBitSet *src)
    IntBitSet *intBitSetIIntersection(IntBitSet *dst, IntBitSet *src)
    IntBitSet *intBitSetISub(IntBitSet *x, IntBitSet *y)
    IntBitSet *intBitSetIXor(IntBitSet *x, IntBitSet *y)
    bint intBitSetIsDisjoint(IntBitSet *x, IntBitSet *y)
    int intBitSetGetNext(IntBitSet *x, int last)
    int intBitSetGetLast(IntBitSet *x)
    unsigned char intBitSetCmp(IntBitSet *x, IntBitSet *y)

__maxelem__ = maxelem

cdef class intbitset:
    """
    Defines an intbitset data object to hold unordered sets of
    unsigned integers with ultra fast set operations, implemented via
    bit vectors and Python C extension to optimize speed and memory
    usage.

    Emulates the Python built-in set class interface with some
    additional specific methods such as its own fast dump and load
    marshalling functions.  Uses real bits to optimize memory usage,
    so may have issues with endianness if you transport serialized
    bitsets between various machine architectures.

    The constructor accept the following parameters:
        rhs=0, 
        int preallocate=-1, 
        int trailing_bits=0,
        bint sanity_checks=CFG_INTBITSET_ENABLE_SANITY_CHECKS,
        int no_allocate=0:

    where rhs can be:

    * ``int/long`` for creating allocating empty intbitset that will hold at
      least rhs elements, before being resized

    * ``intbitset`` for cloning

    * ``bytes`` for retrieving an intbitset that was dumped into a byte string

    * ``array`` for retrieving an intbitset that was dumped into a string stored
      in an array

    * sequence made of integers for copying all the elements from the sequence. 
      If minsize is specified than it is initially allocated enough space to
      hold up to minsize integers, otherwise the biggest element of the sequence
      will be used.

    * sequence made of tuples: then the first element of each tuple is
      considered as an integer (as in the sequence made of integers).

    The other arguments are:

    * ``preallocate`` is a suggested initial upper bound on the numbers that
      will be stored, by looking at rhs a sequence of number.

    * ``trailing_bits`` is 1, then the set will contain "all" the positive integers

    * ``no_allocate`` and ``sanity_checks`` are used internally and should never be set.
    """
    cdef IntBitSet *bitset
    cdef bint sanity_checks

    def __cinit__(
        self not None, 
        rhs=0, 
        int preallocate=-1, 
        int trailing_bits=0, 
        bint sanity_checks=CFG_INTBITSET_ENABLE_SANITY_CHECKS, 
        int no_allocate=0,
    ):
        cdef Py_ssize_t size = 0
        cdef const_void_ptr buf = NULL
        cdef int elem
        cdef int last
        cdef int remelem
        cdef bint tuple_of_tuples
        cdef Py_buffer view

        self.sanity_checks = sanity_checks
        #print >> sys.stderr, "intbitset.__cinit__ is called"
        msg = "Error"
        self.bitset = NULL
        try:
            if no_allocate:
                return
            if type(rhs) in (int, long):
                if rhs < 0:
                    raise ValueError("rhs can't be negative")
                self.bitset = intBitSetCreate(rhs, trailing_bits)
            elif type(rhs) is intbitset:
                self.bitset = intBitSetClone((<intbitset>rhs).bitset)
            elif type(rhs) in (bytes, array):
                try:
                    if type(rhs) is array:
                        rhs = rhs.tobytes()
                    tmp = zlib.decompress(rhs)

                    if PyObject_GetBuffer(tmp, &view, PyBUF_SIMPLE) != 0:
                        raise ValueError("Unable to get buffer")
                    
                    try:
                        buf = <const_void_ptr>view.buf
                        size = view.len

                        if (size % wordbytesize):
                            ## Wrong size!
                            raise Exception()

                        self.bitset = intBitSetCreateFromBuffer(buf, size)
                    finally:
                        PyBuffer_Release(&view)
                        
                except Exception as e:
                    raise ValueError("rhs is corrupted: %s" % str(e))
            elif hasattr(rhs, '__iter__'):
                tuple_of_tuples = (
                    rhs
                    and hasattr(rhs, '__getitem__')
                    and hasattr(rhs[0], '__getitem__')
                )
                try:
                    if preallocate < 0:
                        if rhs and (not hasattr(rhs, '__getitem__') or type(rhs[0]) is int):
                            try:
                                preallocate = max(rhs)
                            except ValueError:
                                preallocate = 0
                        else:
                            preallocate = 0
                    if self.sanity_checks:
                        if not (0 <= preallocate < maxelem):
                            raise OverflowError("Can't store integers bigger than %s" % maxelem)
                    self.bitset = intBitSetCreate(preallocate, trailing_bits)
                    if trailing_bits:
                        last = 0
                        if self.sanity_checks:
                            if tuple_of_tuples:
                                for tmp_tuple in rhs:
                                    elem = tmp_tuple[0]
                                    if elem < 0:
                                        raise ValueError("Negative numbers, not allowed")
                                    elif elem > maxelem:
                                        raise OverflowError("Elements must be <= %s" % maxelem)
                                    for remelem from last <= remelem < elem:
                                        intBitSetDelElem(self.bitset, remelem)
                                    last = elem + 1
                            else:
                                for elem in rhs:
                                    if elem < 0:
                                        raise ValueError("Negative numbers, not allowed")
                                    elif elem > maxelem:
                                        raise OverflowError("Elements must be <= %s" % maxelem)
                                    for remelem from last <= remelem < elem:
                                        intBitSetDelElem(self.bitset, remelem)
                                    last = elem + 1
                        else:
                            if tuple_of_tuples:
                                for tmp_tuple in rhs:
                                    elem = tmp_tuple[0]
                                    for remelem from last <= remelem < elem:
                                        intBitSetDelElem(self.bitset, remelem)
                                    last = elem + 1
                            else:
                                for elem in rhs:
                                    for remelem from last <= remelem < elem:
                                        intBitSetDelElem(self.bitset, remelem)
                                    last = elem + 1

                    else:
                        if self.sanity_checks:
                            if tuple_of_tuples:
                                for tmp_tuple in rhs:
                                    elem = tmp_tuple[0]
                                    if elem < 0:
                                        raise ValueError("Negative numbers, not allowed")
                                    elif elem > maxelem:
                                        raise OverflowError("Elements must be <= %s" % maxelem)
                                    intBitSetAddElem(self.bitset, elem)
                            else:
                                for elem in rhs:
                                    if elem < 0:
                                        raise ValueError("Negative numbers, not allowed")
                                    elif elem > maxelem:
                                        raise OverflowError("Elements must be <= %s" % maxelem)
                                    intBitSetAddElem(self.bitset, elem)
                        else:
                            if tuple_of_tuples:
                                for tmp_tuple in rhs:
                                    elem = tmp_tuple[0]
                                    intBitSetAddElem(self.bitset, elem)
                            else:
                                for elem in rhs:
                                    intBitSetAddElem(self.bitset, elem)
                except Exception as e:
                    raise ValueError("retrieving integers from rhs is impossible: %s" % str(e))
            else:
                raise TypeError("rhs is of unknown type %s" % type(rhs))
        except:
            intBitSetDestroy(self.bitset)
            raise

    def __dealloc__(self not None):
        #print >> sys.stderr, "intbitset.__dealloc__ is called"
        intBitSetDestroy(self.bitset)

    def __contains__(self not None, int elem):
        if self.sanity_checks:
            if elem < 0:
                raise ValueError("Negative numbers, not allowed")
            elif elem > maxelem:
                raise OverflowError("Element must be <= %s" % maxelem)
        return intBitSetIsInElem(self.bitset, elem) != 0

    def __cmp__(self not None, intbitset rhs not None):
        raise TypeError("cannot compare intbitset using cmp()")

    def __richcmp__(self not None, rhs, int op):
        if not isinstance(self, intbitset) or not isinstance(rhs, intbitset):
            return False
        cdef short unsigned int tmp
        tmp = intBitSetCmp((<intbitset>self).bitset, (<intbitset>rhs).bitset)
        if op == 0: # <
            return tmp == 1
        if op == 1: # <=
            return tmp <= 1
        if op == 2: # ==
            return tmp == 0
        if op == 3: # !=
            return tmp > 0
        if op == 4: # >
            return tmp == 2
        if op == 5: # >=
            return tmp in (0, 2)

    def __len__(self not None):
        return intBitSetGetTot(self.bitset)

    def __hash__(self not None):
        return hash(
            PyBytes_FromStringAndSize(
                <char *>self.bitset.bitset, 
                wordbytesize * (intBitSetGetTot(self.bitset) / wordbitsize + 1)
            )
        )

    def __nonzero__(self not None):
        return not intBitSetEmpty(self.bitset)

    def __deepcopy__(self not None, memo):
        return intbitset(self)

    def __delitem__(self not None, int elem):
        if self.sanity_checks:
            if elem < 0:
                raise ValueError("Negative numbers, not allowed")
            elif elem > maxelem:
                raise OverflowError("Element must be <= %s" % maxelem)
        intBitSetDelElem(self.bitset, elem)

    def __iadd__(self not None, rhs):
        cdef int elem
        if isinstance(rhs, (int, long)):
            if self.sanity_checks:
                if rhs < 0:
                    raise ValueError("Negative numbers, not allowed")
                elif rhs > maxelem:
                    raise OverflowError("rhs must be <= %s" % maxelem)
            intBitSetAddElem(self.bitset, rhs)
        elif isinstance(rhs, intbitset):
            intBitSetIUnion(self.bitset, (<intbitset> rhs).bitset)
        else:
            if self.sanity_checks:
                for elem in rhs:
                    if elem < 0:
                        raise ValueError("Negative numbers, not allowed")
                    elif elem > maxelem:
                        raise OverflowError("Elements must be <= %s" % maxelem)
                    intBitSetAddElem(self.bitset, elem)
            else:
                for elem in rhs:
                    if elem < 0:
                        raise ValueError("Negative numbers, not allowed")
                    elif elem > maxelem:
                        raise OverflowError("Elements must be <= %s" % maxelem)
                    intBitSetAddElem(self.bitset, elem)
        return self

    def __isub__(self not None, rhs not None):
        """Remove all elements of another set from this set."""
        cdef int elem
        if isinstance(rhs, (int, long)):
            if self.sanity_checks:
                if rhs < 0:
                    raise ValueError("Negative numbers, not allowed")
                elif rhs > maxelem:
                    raise OverflowError("rhs must be <= %s" % maxelem)
            intBitSetDelElem(self.bitset, rhs)
        elif isinstance(rhs, intbitset):
            intBitSetISub(self.bitset, (<intbitset> rhs).bitset)
        else:
            if self.sanity_checks:
                for elem in rhs:
                    if elem < 0:
                        raise ValueError("Negative numbers, not allowed")
                    elif elem > maxelem:
                        raise OverflowError("Elements must be <= %s" % maxelem)
                    intBitSetDelElem(self.bitset, elem)
            else:
                for elem in rhs:
                    intBitSetDelElem(self.bitset, elem)
        return self

    def __sub__(self not None, intbitset rhs not None):
        """Return the difference of two intbitsets as a new set.
        (i.e. all elements that are in this intbitset but not the other.)
        """
        cdef intbitset ret = intbitset(no_allocate=1)
        (<intbitset>ret).bitset = intBitSetSub((<intbitset> self).bitset, rhs.bitset)
        return ret

    def __and__(self not None, intbitset rhs not None):
        """Return the intersection of two intbitsets as a new set.
        (i.e. all elements that are in both intbitsets.)
        """
        cdef intbitset ret = intbitset(no_allocate=1)
        (<intbitset>ret).bitset = intBitSetIntersection((<intbitset> self).bitset, rhs.bitset)
        return ret

    def __iand__(self not None, intbitset rhs not None):
        """Update a intbitset with the intersection of itself and another."""
        intBitSetIIntersection(self.bitset, rhs.bitset)
        return self

    def __or__(self not None, intbitset rhs not None):
        """Return the union of two intbitsets as a new set.
        (i.e. all elements that are in either intbitsets.)
        """
        cdef intbitset ret = intbitset(no_allocate=1)
        (<intbitset>ret).bitset = intBitSetUnion((<intbitset> self).bitset, rhs.bitset)
        return ret

    def __ior__(self not None, intbitset rhs not None):
        """Update a intbitset with the union of itself and another."""
        intBitSetIUnion(self.bitset, rhs.bitset)
        return self

    def __xor__(self not None, intbitset rhs not None):
        """Return the symmetric difference of two sets as a new set.
        (i.e. all elements that are in exactly one of the sets.)
        """
        cdef intbitset ret = intbitset(no_allocate=1)
        (<intbitset>ret).bitset = intBitSetXor((<intbitset> self).bitset, rhs.bitset)
        return ret

    def __ixor__(self not None, intbitset rhs not None):
        """Update an intbitset with the symmetric difference of itself and another.
        """
        intBitSetIXor(self.bitset, rhs.bitset)
        return self

    def __repr__(self not None):
        finite_list = self.extract_finite_list()
        if self.bitset.trailing_bits:
            return "intbitset(%s, trailing_bits=True)" % repr(finite_list)
        else:
            return "intbitset(%s)" % repr(finite_list)

    def __str__(self not None):
        cdef int tot
        tot = intBitSetGetTot(self.bitset)
        if tot < 0:
            return "intbitset([...], trailing_bits=True)"
        elif tot > 10:
            begin_list = self[0:5]
            end_list = self[tot - 5:tot]
            ret = "intbitset(["
            for n in begin_list:
                ret += '%i, ' % n
            ret += "..., "
            for n in end_list:
                ret += '%i, ' % n
            ret = ret[:-2]
            ret += '])'
            return ret
        else:
            return self.__repr__()

    def __getitem__(self not None, object key):
        cdef Py_ssize_t i
        cdef int elem = -1
        cdef int start
        cdef int end
        cdef int step
        if hasattr(key, 'indices'):
            ## This is a slice object!
            if self.bitset.trailing_bits and (key.start < 0 or key.stop < 0):
                raise IndexError("negative indexes are not allowed on infinite intbitset")
            retset = intbitset()
            start, end, step = key.indices(intBitSetGetTot(self.bitset))
            if step < 0:
                raise ValueError("negative steps are not yet supported")
            for i in range(start):
                elem = intBitSetGetNext(self.bitset, elem)
                if elem < 0:
                    return retset
            for i in range(end - start):
                elem = intBitSetGetNext(self.bitset, elem)
                if elem < 0:
                    return retset
                if i % step == 0:
                    retset.add(elem)
            return retset
        else:
            end = key
            if end < 0:
                if self.bitset.trailing_bits:
                    raise IndexError("negative indexes are not allowed on infinite intbitset")
                end += intBitSetGetTot(self.bitset)
                if end < 0:
                    raise IndexError("intbitset index out of range")
            if end >= intBitSetGetTot(self.bitset):
                raise IndexError("intbitset index out of range")
            for i in range(end + 1):
                elem = intBitSetGetNext(self.bitset, elem)
            return elem

    # pickle interface
    def __reduce__(self not None):
        return _, (self.fastdump(),)

    __safe_for_unpickling__ = True

    # Iterator interface
    def __iter__(self not None):
        if self.bitset.trailing_bits:
            raise OverflowError("It's impossible to iterate over an infinite set.")
        return intbitset_iterator(self)

    # Customized interface
    cpdef add(intbitset self, int elem):
        """Add an element to a set.
        This has no effect if the element is already present."""
        if self.sanity_checks:
            if elem < 0:
                raise ValueError("Negative numbers, not allowed")
            elif elem > maxelem:
                raise OverflowError("Element must be <= %s" % maxelem)
        intBitSetAddElem(self.bitset, elem)

    cpdef clear(intbitset self):
        intBitSetReset(self.bitset)


    cpdef discard(intbitset self, int elem):
        """Remove an element from a intbitset if it is a member.
        If the element is not a member, do nothing."""
        if self.sanity_checks:
            if elem < 0:
                raise ValueError("Negative numbers, not allowed")
            elif elem > maxelem:
                raise OverflowError("Element must be <= %s" % maxelem)
        intBitSetDelElem(self.bitset, elem)

    symmetric_difference = __xor__
    symmetric_difference_update = __ixor__

    cpdef issubset(intbitset self, rhs):
        """Report whether another set contains this set."""
        return self.__le__(rhs)

    cpdef issuperset(intbitset self, rhs):
        """Report whether this set contains another set."""
        return self.__ge__(rhs)

    # Dumping & Loading
    cpdef fastdump(intbitset self):
        """Return a compressed string representation suitable to be saved
        somewhere."""
        cdef Py_ssize_t size
        size = intBitSetGetSize((<intbitset> self).bitset)
        tmp = PyBytes_FromStringAndSize(<char *>self.bitset.bitset, ( size + 1) * wordbytesize)
        return zlib.compress(tmp)

    cpdef fastload(intbitset self, strdump):
        """Load a compressed string representation produced by a previous call
        to the fastdump method into the current intbitset. The previous content
        will be replaced."""
        cdef Py_ssize_t size
        cdef const_void_ptr buf
        cdef Py_buffer view

        buf = NULL
        size = 0
        try:
            if type(strdump) is array:
                strdump = strdump.tostring()
            # tmp needed to not be garbage collected
            tmp = zlib.decompress(strdump)

            if PyObject_GetBuffer(tmp, &view, PyBUF_SIMPLE) != 0:
                raise ValueError("Unable to get buffer")

            try:
                buf = <const_void_ptr>view.buf
                size = view.len

                if (size % wordbytesize):
                    ## Wrong size!
                    raise Exception()

                intBitSetResetFromBuffer((<intbitset> self).bitset, buf, size)
            finally:
                PyBuffer_Release(&view)

        except:
            raise ValueError("strdump is corrupted")

    cpdef copy(intbitset self):
        """Return a shallow copy of a set."""
        return intbitset(self)

    cpdef pop(intbitset self):
        """Remove and return an arbitrary set element.

        Note: intbitset implementation of .pop() differs from the native ``set``
            implementation by guaranteeing returning always the largest element.
        """
        cdef int ret
        ret = intBitSetGetLast(self.bitset)
        if ret < 0:
            raise KeyError("pop from an empty or infinite intbitset")
        intBitSetDelElem(self.bitset, ret)
        return ret

    cpdef remove(intbitset self, int elem):
        """Remove an element from a set; it must be a member.
        If the element is not a member, raise a KeyError.
        """
        if self.sanity_checks:
            if elem < 0:
                raise ValueError("Negative numbers, not allowed")
            elif elem > maxelem:
                raise OverflowError("Elements must be <= %s" % maxelem)
        if intBitSetIsInElem(self.bitset, elem):
            intBitSetDelElem(self.bitset, elem)
        else:
            raise KeyError(elem)

    cpdef strbits(intbitset self):
        """Return a string of 0s and 1s representing the content in memory
        of the intbitset.
        """
        cdef int i
        cdef int last
        if (<intbitset> self).bitset.trailing_bits:
            raise OverflowError("It's impossible to print an infinite set.")
        last = 0
        ret = []
        for i in self:
            ret.append('0'*(i-last)+'1')
            last = i+1
        return ''.join(ret)

    def update(self not None, *args):
        """Update the intbitset, adding elements from all others."""
        cdef intbitset iarg
        for arg in args:
            iarg = arg if hasattr(arg, "bitset") else intbitset(arg)
            intBitSetIUnion(self.bitset, iarg.bitset)

    union_update = update

    def intersection_update(self not None, *args):
        """Update the intbitset, keeping only elements found in it and all others."""
        cdef intbitset iarg
        for arg in args:
            iarg = arg if hasattr(arg, "bitset") else intbitset(arg)
            intBitSetIIntersection(self.bitset, iarg.bitset)

    def difference_update(self not None, *args):
        """Update the intbitset, removing elements found in others."""
        cdef intbitset iarg
        for arg in args:
            iarg = arg if hasattr(arg, "bitset") else intbitset(arg)
            intBitSetISub(self.bitset, iarg.bitset)

    def union(self not None, *args):
        """Return a new intbitset with elements from the intbitset and all others."""
        cdef intbitset ret = intbitset(self)
        cdef intbitset iarg
        for arg in args:
            iarg = arg if hasattr(arg, "bitset") else intbitset(arg)
            intBitSetIUnion(ret.bitset, iarg.bitset)
        return ret

    def intersection(self not None, *args):
        """Return a new intbitset with elements common to the intbitset and all others."""
        cdef intbitset ret = intbitset(self)
        cdef intbitset iarg
        for arg in args:
            iarg = arg if hasattr(arg, "bitset") else intbitset(arg)
            intBitSetIIntersection(ret.bitset, iarg.bitset)
        return ret

    def difference(self not None, *args):
        """Return a new intbitset with elements from the intbitset that are not in the others."""
        cdef intbitset ret = intbitset(self)
        cdef intbitset iarg
        for arg in args:
            iarg = arg if hasattr(arg, "bitset") else intbitset(arg)
            intBitSetISub(ret.bitset, iarg.bitset)
        return ret

    def isdisjoint(self not None, intbitset rhs not None):
        """Return True if two intbitsets have a null intersection."""
        return intBitSetIsDisjoint(self.bitset, rhs.bitset)

    cpdef update_with_signs(intbitset self, rhs):
        """Given a dictionary rhs whose keys are integers, remove all the integers
        whose value are less than 0 and add every integer whose value is 0 or more"""
        cdef int value
        try:
            if self.sanity_checks:
                for value, sign in rhs.iteritems():
                    if value < 0:
                        raise ValueError("Negative numbers, not allowed")
                    elif value > maxelem:
                        raise OverflowError("Elements must <= %s" % maxelem)
                    if sign < 0:
                        intBitSetDelElem(self.bitset, value)
                    else:
                        intBitSetAddElem(self.bitset, value)
            else:
                for value, sign in rhs.iteritems():
                    if sign < 0:
                        intBitSetDelElem(self.bitset, value)
                    else:
                        intBitSetAddElem(self.bitset, value)
        except AttributeError:
            raise TypeError("rhs should be a valid dictionary with integers keys and integer values")

    cpdef get_size(intbitset self):
        return intBitSetGetSize(self.bitset)

    cpdef get_allocated(intbitset self):
        return intBitSetGetAllocated(self.bitset)

    cpdef is_infinite(intbitset self):
        """Return True if the intbitset is infinite. (i.e. trailing_bits=True
        was used in the constructor.)"""
        return self.bitset.trailing_bits != 0

    cpdef extract_finite_list(intbitset self, int up_to=-1):
        """Return a finite list of elements sufficient to be passed to intbitset
        constructor toghether with the proper value of trailing_bits in order
        to reproduce this intbitset. At least up_to integer are looked for when
        they are inside the intbitset but not necessarily needed to build the
        intbitset"""
        cdef int true_up_to
        cdef int last
        if self.sanity_checks and up_to > maxelem:
            raise OverflowError("up_to must be <= %s" % maxelem)
        ret = []
        true_up_to = max(up_to, (intBitSetGetSize(self.bitset)) * wordbitsize)
        last = -1
        while last < true_up_to:
            last = intBitSetGetNext(self.bitset, last)
            if last == -2:
                break
            ret.append(last)
        return ret

    cpdef get_wordbitsize(intbitset self):
        return wordbitsize

    cpdef get_wordbytsize(intbitset self):
        return wordbytesize

    cpdef tolist(intbitset self):
        """Legacy method to retrieve a list of all the elements inside an
        intbitset.
        """
        if self.bitset.trailing_bits:
            raise OverflowError("It's impossible to retrieve a list of an infinite set")
        return self.extract_finite_list()

    cdef object __weakref__

cdef class intbitset_iterator:
    cdef int last
    cdef IntBitSet *bitset
    cdef bint sanity_checks

    def __cinit__(intbitset_iterator self, intbitset bitset not None):
        #print >> sys.stderr, "intbitset_iterator.__cinit__ is called"
        self.last = -1
        ## A copy should be performed, in case the original bitset disappears
        ## as in "for x in intbitset([1,2,3])"!
        self.bitset = intBitSetClone(bitset.bitset)
        self.sanity_checks = CFG_INTBITSET_ENABLE_SANITY_CHECKS

    def __dealloc__(intbitset_iterator self):
        #print >> sys.stderr, "intbitset_iterator.__dealloc__ is called"
        intBitSetDestroy(self.bitset)

    def __next__(intbitset_iterator self):
        if self.last == -2:
            raise StopIteration()
        self.last = intBitSetGetNext(self.bitset, self.last)
        if self.sanity_checks and (self.bitset.allocated < self.bitset.size):
            raise MemoryError(
                "intbitset corrupted: allocated: %s, size: %s"
                % (self.bitset.allocated, self.bitset.size)
            )
        if self.last < 0:
            self.last = -2
            raise StopIteration()
        return self.last

    def __iter__(intbitset_iterator self):
        return self

    cdef object __weakref__
