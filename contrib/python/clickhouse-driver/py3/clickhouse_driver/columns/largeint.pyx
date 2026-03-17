from cpython cimport Py_INCREF
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM

from .. import writer

cdef object MAX_UINT64 = writer.MAX_UINT64
cdef object MAX_INT64 = writer.MAX_INT64


def int128_from_quads(self, quad_items, unsigned long long n_items):
    cdef unsigned int factor = 2
    items = PyTuple_New(n_items)

    cdef unsigned long long i, ix
    cdef object item

    for i in range(n_items):
        ix = factor * i

        if quad_items[ix + 1] > MAX_INT64:
            item = (
                -((MAX_UINT64 - quad_items[ix + 1]) << 64)
                - (MAX_UINT64 - quad_items[ix])
                - 1
            )

        else:
            item = (quad_items[ix + 1] << 64) + quad_items[ix]

        Py_INCREF(item)
        PyTuple_SET_ITEM(items, i, item)

    return items


def int128_to_quads(self, items, unsigned long long n_items):
    cdef unsigned int factor = 2
    quad_items = PyTuple_New(n_items * factor)

    cdef unsigned long long i, ix
    cdef object x, item

    for i in range(n_items):
        ix = factor * i

        x = items[i]
        if x < 0:
            x = -x - 1

            item = MAX_UINT64 - x & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix, item)

            item = MAX_UINT64 - (x >> 64) & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix + 1, item)

        else:
            item = x & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix, item)

            item = (x >> 64) & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix + 1, item)

    return quad_items


def uint128_from_quads(self, quad_items, unsigned long long n_items):
    cdef unsigned int factor = 2
    items = PyTuple_New(n_items)

    cdef unsigned long long i, ix
    cdef object item

    for i in range(n_items):
        ix = factor * i
        item = (quad_items[ix + 1] << 64) + quad_items[ix]

        Py_INCREF(item)
        PyTuple_SET_ITEM(items, i, item)

    return items


def uint128_to_quads(self, items, unsigned long long n_items):
    cdef unsigned int factor = 2
    quad_items = PyTuple_New(n_items * factor)

    cdef unsigned long long i, ix
    cdef object x, item

    for i in range(n_items):
        ix = factor * i

        x = items[i]
        item = x & MAX_UINT64
        Py_INCREF(item)
        PyTuple_SET_ITEM(quad_items, ix, item)

        item = (x >> 64) & MAX_UINT64
        Py_INCREF(item)
        PyTuple_SET_ITEM(quad_items, ix + 1, item)

    return quad_items

# 256 bits


def int256_from_quads(self, quad_items, unsigned long long n_items):
    cdef unsigned int factor = 4
    items = PyTuple_New(n_items)

    cdef unsigned long long i, ix
    cdef object item

    for i in range(n_items):
        ix = factor * i

        if quad_items[ix + 3] > MAX_INT64:
            item = (
                -((MAX_UINT64 - quad_items[ix + 3]) << 192)
                -((MAX_UINT64 - quad_items[ix + 2]) << 128)
                -((MAX_UINT64 - quad_items[ix + 1]) << 64)
                - (MAX_UINT64 - quad_items[ix])
                - 1
            )

        else:
            item = (
                (quad_items[ix + 3] << 192) +
                (quad_items[ix + 2] << 128) +
                (quad_items[ix + 1] << 64) +
                quad_items[ix]
            )

        Py_INCREF(item)
        PyTuple_SET_ITEM(items, i, item)

    return items


def int256_to_quads(self, items, unsigned long long n_items):
    cdef unsigned int factor = 4
    quad_items = PyTuple_New(n_items * factor)

    cdef unsigned long long i, ix
    cdef object x, item

    for i in range(n_items):
        ix = factor * i

        x = items[i]
        if x < 0:
            x = -x - 1

            item = MAX_UINT64 - x & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix, item)

            item = MAX_UINT64 - (x >> 64) & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix + 1, item)

            item = MAX_UINT64 - (x >> 128) & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix + 2, item)

            item = MAX_UINT64 - (x >> 192) & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix + 3, item)

        else:
            item = x & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix, item)

            item = (x >> 64) & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix + 1, item)

            item = (x >> 128) & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix + 2, item)

            item = (x >> 192) & MAX_UINT64
            Py_INCREF(item)
            PyTuple_SET_ITEM(quad_items, ix + 3, item)

    return quad_items


def uint256_from_quads(self, quad_items, unsigned long long n_items):
    cdef unsigned int factor = 4
    items = PyTuple_New(n_items)

    cdef unsigned long long i, ix
    cdef object item

    for i in range(n_items):
        ix = factor * i
        item = (
            (quad_items[ix + 3] << 192) +
            (quad_items[ix + 2] << 128) +
            (quad_items[ix + 1] << 64) +
            quad_items[ix]
        )

        Py_INCREF(item)
        PyTuple_SET_ITEM(items, i, item)

    return items


def uint256_to_quads(self, items, unsigned long long n_items):
    cdef unsigned int factor = 4
    quad_items = PyTuple_New(n_items * factor)

    cdef unsigned long long i, ix
    cdef object x, item

    for i in range(n_items):
        ix = factor * i

        x = items[i]
        item = x & MAX_UINT64
        Py_INCREF(item)
        PyTuple_SET_ITEM(quad_items, ix, item)

        item = (x >> 64) & MAX_UINT64
        Py_INCREF(item)
        PyTuple_SET_ITEM(quad_items, ix + 1, item)

        item = (x >> 128) & MAX_UINT64
        Py_INCREF(item)
        PyTuple_SET_ITEM(quad_items, ix + 2, item)

        item = (x >> 192) & MAX_UINT64
        Py_INCREF(item)
        PyTuple_SET_ITEM(quad_items, ix + 3, item)

    return quad_items
