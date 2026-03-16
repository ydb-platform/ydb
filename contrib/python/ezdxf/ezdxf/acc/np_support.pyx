# cython: language_level=3
# Copyright (c) 2023-2024, Manfred Moitzi
# License: MIT License
from typing_extensions import TypeAlias
import numpy as np
import numpy.typing as npt

import cython
from .vector cimport isclose

cdef extern from "constants.h":
    const double ABS_TOL
    const double REL_TOL


NDArray: TypeAlias = npt.NDArray[np.float64]


def has_clockwise_orientation(vertices: np.ndarray) -> bool:
    """ Returns True if 2D `vertices` have clockwise orientation. Ignores
    z-axis of all vertices.

    Args:
        vertices: numpy array

    Raises:
        ValueError: less than 3 vertices

    """
    if len(vertices) < 3:
        raise ValueError('At least 3 vertices required.')

    return _has_clockwise_orientation(vertices, vertices.shape[0])


@cython.boundscheck(False)
@cython.wraparound(False)
cdef bint _has_clockwise_orientation(double [:, ::1] vertices, Py_ssize_t size):
    cdef Py_ssize_t index
    cdef Py_ssize_t start
    cdef Py_ssize_t last = size - 1
    cdef double s = 0.0
    cdef double p1x = vertices[0][0]
    cdef double p1y = vertices[0][1]
    cdef double p2x = vertices[last][0]
    cdef double p2y = vertices[last][1]

    # Using the same tolerance as the Python implementation:
    cdef bint x_is_close = isclose(p1x, p2x, REL_TOL, ABS_TOL)
    cdef bint y_is_close = isclose(p1y, p2y, REL_TOL, ABS_TOL)

    if x_is_close and y_is_close:
        p1x = vertices[0][0]
        p1y = vertices[0][1]
        start = 1
    else:
        p1x = vertices[last][0]
        p1y = vertices[last][1]
        start = 0

    for index in range(start, size):
        p2x = vertices[index][0]
        p2y = vertices[index][1]
        s += (p2x - p1x) * (p2y + p1y)
        p1x = p2x
        p1y = p2y
    return s > 0.0


def lu_decompose(A: NDArray, m1: int, m2: int) -> tuple[NDArray, NDArray, NDArray]:
    upper: np.ndarray = np.array(A, dtype=np.float64)
    n: int = upper.shape[0]
    lower: np.ndarray = np.zeros((n, m1), dtype=np.float64)

    # Is <np.int_> better to match <int> on all platforms?
    index: np.ndarray = np.zeros((n,), dtype=np.int32)  
    _lu_decompose_cext(upper, lower, index, n, m1, m2)
    return upper, lower, index


@cython.boundscheck(False)
@cython.wraparound(False)
cdef _lu_decompose_cext(
    double [:, ::1] upper, 
    double [:, ::1] lower, 
    int [::1] index, 
    int n, 
    int m1, 
    int m2
):
    cdef int mm = m1 + m2 + 1
    cdef int l = m1
    cdef int i, j, k
    cdef double dum

    for i in range(m1):
        for j in range(m1 - i, mm):
            upper[i][j - l] = upper[i][j]
        l -= 1
        for j in range(mm - l - 1, mm):
            upper[i][j] = 0.0

    l = m1
    for k in range(n):
        dum = upper[k][0]
        i = k
        if l < n:
            l += 1
        for j in range(k + 1, l):
            if abs(upper[j][0]) > abs(dum):
                dum = upper[j][0]
                i = j
        index[k] = i + 1
        if i != k:
            for j in range(mm):
                upper[k][j], upper[i][j] = upper[i][j], upper[k][j]

        for i in range(k + 1, l):
            dum = upper[i][0] / upper[k][0]
            lower[k][i - k - 1] = dum
            for j in range(1, mm):
                upper[i][j - 1] = upper[i][j] - dum * upper[k][j]
            upper[i][mm - 1] = 0.0


def solve_vector_banded_matrix(
    x: NDArray,
    upper: NDArray,
    lower: NDArray,
    index: NDArray,
    m1: int,
    m2: int,
) -> NDArray:
    n: int = upper.shape[0]
    x = np.array(x)  # copy x because array x gets modified
    if x.shape[0] != n:
        raise ValueError(
            "Item count of vector <x> has to match the row count of matrix <upper>."
        )
    _solve_vector_banded_matrix_cext(x, upper, lower, index, n, m1, m2)
    return x


@cython.boundscheck(False)
@cython.wraparound(False)
cdef _solve_vector_banded_matrix_cext(
    double [::1] x,
    double [:, ::1] au,
    double [:, ::1] al,
    int [::1] index,
    int n,
    int m1,
    int m2,
):
    cdef int mm = m1 + m2 + 1
    cdef int l = m1
    cdef int i, j, k
    cdef double dum

    for k in range(n):
        j = index[k] - 1
        if j != k:
            x[k], x[j] = x[j], x[k]
        if l < n:
            l += 1
        for j in range(k + 1, l):
            x[j] -= al[k][j - k - 1] * x[k]

    l = 1
    for i in range(n - 1, -1, -1):
        dum = x[i]
        for k in range(1, l):
            dum -= au[i][k] * x[k + i]
        x[i] = dum / au[i][0]
        if l < mm:
            l += 1
    return x
