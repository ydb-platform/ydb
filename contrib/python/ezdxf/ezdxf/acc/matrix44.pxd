# cython: language_level=3
# Copyright (c) 2020-2023, Manfred Moitzi
# License: MIT License
from .vector cimport Vec3

cdef class Matrix44:
    cdef double m[16]
    cdef Vec3 get_ux(self: Matrix44)
    cdef Vec3 get_uy(self: Matrix44)
    cdef Vec3 get_uz(self: Matrix44)

cdef inline swap(double *a, double *b):
    cdef double tmp = a[0]
    a[0] = b[0]
    b[0] = tmp