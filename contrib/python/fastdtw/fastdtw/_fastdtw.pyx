#!python
#cython: boundscheck=False, cdivision=True, wraparound=False

from __future__ import absolute_import, division

import numbers

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.math cimport pow, fabs
from libcpp.vector cimport vector

import numpy as np

try:
  from libc.math cimport INFINITY
except:
  from numpy.math cimport INFINITY


cdef struct LowHigh:
    int low, high

cdef struct WindowElement:
    int x_idx, y_idx, cost_idx_left, cost_idx_up, cost_idx_corner

cdef struct CostElement:
    double cost
    int prev_idx

cdef struct PathElement:
    int x_idx, y_idx


def fastdtw(x, y, int radius=1, dist=None):
    ''' return the approximate distance between 2 time series with O(N)
        time and memory complexity

        Parameters
        ----------
        x : array_like
            input array 1
        y : array_like
            input array 2
        radius : int
            size of neighborhood when expanding the path. A higher value will
            increase the accuracy of the calculation but also increase time
            and memory consumption. A radius equal to the size of x and y will
            yield an exact dynamic time warping calculation.
        dist : function or int
            The method for calculating the distance between x[i] and y[j]. If
            dist is an int of value p > 0, then the p-norm will be used. If
            dist is a function then dist(x[i], y[j]) will be used. If dist is
            None then abs(x[i] - y[j]) will be used.

        Returns
        -------
        distance : float
            the approximate distance between the 2 time series
        path : list
            list of indexes for the inputs x and y

        Examples
        --------
        >>> import numpy as np
        >>> import fastdtw
        >>> x = np.array([1, 2, 3, 4, 5], dtype='float')
        >>> y = np.array([2, 3, 4], dtype='float')
        >>> fastdtw.fastdtw(x, y)
        (2.0, [(0, 0), (1, 0), (2, 1), (3, 2), (4, 2)])

        Optimization Considerations
        ---------------------------
        This function runs fastest if the following conditions are satisfied:
            1) x and y are either 1 or 2d numpy arrays whose dtype is a
               subtype of np.float
            2) The dist input is a positive integer or None
    '''
    x, y = __prep_inputs(x, y, dist)

    # passing by reference to recursive functions apparently doesn't work
    # so we are passing pointers
    cdef PathElement *path = (
        <PathElement *>PyMem_Malloc((len(x) + len(y) - 1) *
                                    sizeof(PathElement)))
    cdef int path_len = 0, i
    cost = __fastdtw(x, y, radius, dist, path, path_len)

    path_lst = []
    if path != NULL:
        path_lst = [(path[i].x_idx, path[i].y_idx) for i in range(path_len)]
        PyMem_Free(path)

    return cost, path_lst


cdef double __fastdtw(x, y, int radius, dist,
                      PathElement *path, int &path_len) except? -1:
    cdef int min_time_size
    cdef double cost

    min_time_size = radius + 2

    if len(x) < min_time_size or len(y) < min_time_size:
        cost, path_lst = dtw(x, y, dist=dist)
        (&path_len)[0] = len(path_lst)
        for i in range((&path_len)[0]):
            path[i].x_idx = path_lst[i][0]
            path[i].y_idx = path_lst[i][1]

        return cost

    x_shrinked = __reduce_by_half(x)
    y_shrinked = __reduce_by_half(y)
    distance = __fastdtw(x_shrinked, y_shrinked, radius, dist, path, path_len)

    cdef vector[WindowElement] window
    __expand_window(path, path_len, x, y, radius, dist, window)
    return __dtw(x, y, window, dist, path, path_len)


def dtw(x, y, dist=None):
    ''' return the distance between 2 time series without approximation

        Parameters
        ----------
        x : array_like
            input array 1
        y : array_like
            input array 2
        dist : function or int
            The method for calculating the distance between x[i] and y[j]. If
            dist is an int of value p > 0, then the p-norm will be used. If
            dist is a function then dist(x[i], y[j]) will be used. If dist is
            None then abs(x[i] - y[j]) will be used.

        Returns
        -------
        distance : float
            the approximate distance between the 2 time series
        path : list
            list of indexes for the inputs x and y

        Examples
        --------
        >>> import numpy as np
        >>> import fastdtw
        >>> x = np.array([1, 2, 3, 4, 5], dtype='float')
        >>> y = np.array([2, 3, 4], dtype='float')
        >>> fastdtw.dtw(x, y)
        (2.0, [(0, 0), (1, 0), (2, 1), (3, 2), (4, 2)])

        Optimization Considerations
        ---------------------------
        This function runs fastest if the following conditions are satisfied:
            1) x and y are either 1 or 2d numpy arrays whose dtype is a
               subtype of np.float
            2) The dist input is a positive integer or None
    '''

    cdef int len_x, len_y, x_idx, y_idx, idx
    cdef double cost
    x, y = __prep_inputs(x, y, dist)
    len_x, len_y = len(x), len(y)
    idx = 0
    cdef WindowElement we
    cdef vector[WindowElement] window
    window.resize(len_x * len_y)

    for x_idx in range(len_x):
        for y_idx in range(len_y):
            we.x_idx = x_idx
            we.y_idx = y_idx

            if x_idx == y_idx == 0:
                we.cost_idx_left = -1
                we.cost_idx_up = -1
                we.cost_idx_corner = 0
            else:
                we.cost_idx_left = -1 \
                    if y_idx == 0 and x_idx > 0 \
                    else idx
                we.cost_idx_up = -1 \
                    if x_idx == 0 and y_idx > 0 \
                    else idx - len_y + 1
                we.cost_idx_corner = -1 \
                    if we.cost_idx_left == -1 or we.cost_idx_up == -1 \
                    else we.cost_idx_up - 1

                if we.cost_idx_left == 0:
                    we.cost_idx_left = -1
                if we.cost_idx_up == 0:
                    we.cost_idx_up = 0

            window[idx] = we
            idx += 1

    cdef PathElement *path = (
        <PathElement *>PyMem_Malloc(
            (len(x) + len(y) - 1) * sizeof(PathElement)))

    cdef int path_len = 0
    cost = __dtw(x, y, window, dist, path, path_len)

    path_lst = []
    if path != NULL:
        path_lst = [(path[i].x_idx, path[i].y_idx)
                    for i in range(path_len)]
        PyMem_Free(path)

    return cost, path_lst


cdef inline double __difference(double a, double b):
    return fabs(a - b)


def __norm(p):
    return lambda a, b: np.linalg.norm(a - b, p)


def __prep_inputs(x, y, dist):
    x = np.asanyarray(x, dtype='float')
    y = np.asanyarray(y, dtype='float')

    if x.ndim == y.ndim > 1 and x.shape[1] != y.shape[1]:
        raise ValueError('second dimension of x and y must be the same')
    if isinstance(dist, numbers.Number) and dist <= 0:
        raise ValueError('dist cannot be a negative integer')

    return x, y


cdef double __dtw(x, y, vector[WindowElement] &window, dist,
                  PathElement *path, int &path_len) except? -1:
    ''' calculate the distance between 2 time series where the path between 2
        time series can only be in window.
    '''
    cdef int window_len = window.size()
    cdef WindowElement we
    cdef int idx, i, cost_len
    cdef double d_left, d_up, d_corner, dt, diff, sm

    # initializing to avoid compiler warnings although if these variables are
    # used they will always be set below
    cdef int use_1d = 0, use_2d = 0, width = 0

    cdef double[:] x_arr1d, y_arr1d
    cdef double[:, :] x_arr2d, y_arr2d

    cdef tempx, tempy

    # define the dist function
    # if it is numpy array we can get an order of magnitude improvement
    # by calculating the p-norm ourselves.
    cdef double pnorm = -1.0 if not isinstance(dist, numbers.Number) else dist
    if ((dist is None or pnorm > 0) and
        (isinstance(x, np.ndarray) and isinstance(y, np.ndarray) and
         np.issubdtype(x.dtype, np.floating) and
         np.issubdtype(y.dtype, np.floating))):

        if x.ndim == 1:
            if dist is None:
                pnorm = 1
            use_1d = 1
            x_arr1d = x
            y_arr1d = y
        elif x.ndim == 2:
            if dist is None:
                pnorm = 1
            use_2d = 1
            x_arr2d = x
            y_arr2d = y
            width = x.shape[1]

    if not use_1d and not use_2d:
        if dist is None:
            dist = __difference
        elif pnorm > 0:
            dist = __norm(pnorm)

    # loop over the window. Note from __expand_window that if we loop over its
    # indices we will in effect be looping over each row
    # Note further that the cost vector has the same indices as window with an
    # offset of 1.
    cost_len = window_len + 1
    cdef vector[CostElement] cost
    cost.resize(cost_len)
    cost[0].cost = 0
    cost[0].prev_idx = -1

    for idx in range(window_len):

        we = window[idx]
        if use_1d:
            dt = abs(x_arr1d[we.x_idx] - y_arr1d[we.y_idx])
        elif use_2d:
            sm = 0
            for i in range(width):
                diff = abs(x_arr2d[we.x_idx, i] - y_arr2d[we.y_idx, i])
                sm += pow(diff, pnorm)
            dt = pow(sm, 1 / pnorm)
        else:
            dt = dist(x[we.x_idx], y[we.y_idx])

        d_left = cost[we.cost_idx_left].cost \
            if we.cost_idx_left != -1 else INFINITY
        d_up = cost[we.cost_idx_up].cost \
            if we.cost_idx_up != -1 else INFINITY
        d_corner = cost[we.cost_idx_corner].cost \
            if we.cost_idx_corner != -1 else INFINITY

        if d_up < d_left and d_up < d_corner:
            cost[idx + 1].cost = d_up + dt
            cost[idx + 1].prev_idx = we.cost_idx_up
        elif d_left < d_corner:
            cost[idx + 1].cost = d_left + dt
            cost[idx + 1].prev_idx = we.cost_idx_left
        else:
            cost[idx + 1].cost = d_corner + dt
            cost[idx + 1].prev_idx = we.cost_idx_corner

    # recreate the path
    idx = cost_len - 1
    (&path_len)[0] = 0
    while idx != 0:
        we = window[idx - 1]
        path[path_len].x_idx = we.x_idx
        path[path_len].y_idx = we.y_idx
        (&path_len)[0] += 1
        idx = cost[idx].prev_idx

    # reverse path
    for i in range(path_len / 2):   # using c division to round down
        temp = path[path_len - 1 - i]
        path[path_len - 1 - i] = path[i]
        path[i] = temp

    return cost[cost_len - 1].cost


cdef __reduce_by_half(x):
    ''' return x whose size is floor(len(x) / 2) with linear interpolation
        between values
    '''
    cdef int i, mx
    try:
        mx = x.shape[0] - x.shape[0] % 2
        return (x[:mx:2] + x[1:mx:2]) / 2
    except AttributeError:
        return [(x[i] + x[1+i]) / 2 for i in range(0, len(x) - len(x) % 2, 2)]


cdef __expand_window(PathElement *path, int path_len,
                     x, y, int radius, dist,
                     vector[WindowElement] &window):
    ''' calculate a window around a path where the expansion is of length
        radius in all directions (including diagonals).

        It is helpful to think of the window as a 2d grid. The calculation
        proceeds as follows:
            1) Given a path, get the minimum and maximum y index for each x
               index (this is ybounds1)
            2) Expand ybounds1 by radius (this is ybounds2)
            3) Double the size of ybounds2 to account for later expansion of x
               and y (this is ybounds 3)
            4) write ybounds3 to a window.
    '''

    cdef int max_x_idx, max_y_idx, prv_y_idx, cur_x_idx, x_idx, y_idx, idx
    cdef int low_y_idx, hgh_y_idx
    cdef WindowElement we

    cdef int len_x, len_y
    len_x, len_y = len(x), len(y)

    # step 1
    max_x_idx = path[path_len - 1].x_idx
    max_y_idx = path[path_len - 1].y_idx

    prv_y_idx = 0
    cur_x_idx = -1

    cdef vector[LowHigh] ybounds1
    ybounds1.resize(max_x_idx + 1)

    for idx in range(path_len):
        x_idx = path[idx].x_idx
        y_idx = path[idx].y_idx

        if x_idx != cur_x_idx:
            cur_x_idx = x_idx
            ybounds1[x_idx].low = y_idx

            if cur_x_idx > 0:
                ybounds1[x_idx - 1].high = prv_y_idx

        prv_y_idx = y_idx

    ybounds1[max_x_idx].high = prv_y_idx

    # step 2
    cdef int len_ybounds2 = max_x_idx + radius + 1
    cdef vector[LowHigh] ybounds2
    ybounds2.resize(len_ybounds2)

    for x_idx in range(max_x_idx + 1):
        temp_min_x_idx = max(0, x_idx - radius)
        ybounds2[x_idx].low = \
            max(0, ybounds1[temp_min_x_idx].low - radius)

        temp_max_x_idx = min(max_x_idx, x_idx + radius)
        ybounds2[x_idx].high = \
            min(max_y_idx + radius, ybounds1[temp_max_x_idx].high + radius)

    for x_idx in range(max_x_idx + 1, max_x_idx + radius + 1):
        ybounds2[x_idx].low = ybounds2[x_idx - 1].low
        ybounds2[x_idx].high = ybounds2[x_idx - 1].high

    # step 3
    cdef int len_ybounds3 = min(len_x, 2 * len_ybounds2)
    cdef vector[LowHigh] ybounds3
    ybounds3.resize(len_ybounds3)

    cdef int window_size = 0

    for x_idx in range(max_x_idx + radius + 1):
        if 2 * x_idx < len_x:
            ybounds3[2 * x_idx].low = 2 * ybounds2[x_idx].low
            ybounds3[2 * x_idx].high = \
                min(len_y - 1, 2 * ybounds2[x_idx].high + 1)

            window_size += \
                ybounds3[2 * x_idx].high - ybounds3[2 * x_idx].low + 1

        if 2 * x_idx + 1 < len_x:
            ybounds3[2 * x_idx + 1].low = 2 * ybounds2[x_idx].low
            ybounds3[2 * x_idx + 1].high = \
                min(len_y - 1, 2 * ybounds2[x_idx].high + 1)

            window_size += \
                ybounds3[2 * x_idx + 1].high - ybounds3[2 * x_idx + 1].low + 1

    # step 4
    window.resize(window_size)

    idx = 0
    for x_idx in range(len_ybounds3):

        low_y_idx = ybounds3[x_idx].low
        hgh_y_idx = ybounds3[x_idx].high

        for y_idx in range(low_y_idx, hgh_y_idx + 1):

            we.x_idx = x_idx
            we.y_idx = y_idx

            if x_idx == 0 and y_idx == 0:
                we.cost_idx_up = -1
                we.cost_idx_left = -1
                we.cost_idx_corner = 0
                window[idx] = we
                idx += 1
                continue

            if y_idx == low_y_idx:
                we.cost_idx_left = -1
            else:
                we.cost_idx_left = idx

            if x_idx == 0 or y_idx > ybounds3[x_idx - 1].high:
                we.cost_idx_up = -1
            else:
                we.cost_idx_up = \
                    (idx -
                     (min(len_y - 1, ybounds3[x_idx - 1].high) -
                      ybounds3[x_idx].low + 1)) + 1

                if we.cost_idx_up == 0:
                    we.cost_idx_up = -1

            if (x_idx == 0 or
                    y_idx < ybounds3[x_idx - 1].low + 1 or
                    y_idx > ybounds3[x_idx - 1].high):

                we.cost_idx_corner = -1
            else:
                we.cost_idx_corner = we.cost_idx_up - 1

            window[idx] = we
            idx += 1
