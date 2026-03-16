"""
interval.py
--------------

Deal with 1D intervals which are defined by:
  [start position, end position]
"""

import numpy as np

from .typed import ArrayLike, NDArray, float64


def intersection(a: ArrayLike, b: NDArray[float64]) -> NDArray[float64]:
    """
    Given pairs of ranges merge them in to
    one range if they overlap.

    Parameters
    --------------
    a : (2, ) or (n, 2)
      Start and end of a 1D interval
    b : (2, ) float
      Start and end of a 1D interval

    Returns
    --------------
    inter : (2, ) or (2, 2) float
      The unioned range from the two inputs,
      if not np.ptp(`inter, axis=1)` will be zero.
    """
    a = np.array(a, dtype=np.float64)
    b = np.array(b, dtype=np.float64)

    # convert to vectorized form
    is_1D = a.shape == (2,)
    a = a.reshape((-1, 2))
    b = b.reshape((-1, 2))

    # make sure they're min-max
    a.sort(axis=1)
    b.sort(axis=1)
    a_low, a_high = a.T
    b_low, b_high = b.T

    # do the checks
    check = np.logical_not(np.logical_or(b_low >= a_high, a_low >= b_high))
    overlap = np.zeros(a.shape, dtype=np.float64)
    overlap[check] = np.column_stack(
        (
            np.array([a_low[check], b_low[check]]).max(axis=0),
            np.array([a_high[check], b_high[check]]).min(axis=0),
        )
    )

    if is_1D:
        return overlap[0]

    return overlap


def union(intervals: ArrayLike, sort: bool = True) -> NDArray[float64]:
    """
    For array of multiple intervals union them all into
    the subset of intervals.

    For example:
    `intervals = [[1,2], [2,3]] -> [[1, 3]]`
    `intervals = [[1,2], [2.5,3]] -> [[1, 2], [2.5, 3]]`


    Parameters
    ------------
    intervals : (n, 2)
      Pairs of `(min, max)` values.
    sort
      If the array is already ordered into (min, max) pairs
      and then pairs sorted by minimum value you can skip the
      sorting in this function.

    Returns
    ----------
    unioned : (m, 2)
      New intervals where `m <= n`
    """
    if len(intervals) == 0:
        return np.zeros(0)

    # if the intervals have not been pre-sorted we should apply our sorting logic
    # you would only skip this if you are subsetting a larger list elsewhere.
    if sort:
        # copy inputs and make sure they are (min, max) pairs
        intervals = np.sort(intervals, axis=1)
        # order them by lowest starting point
        intervals = intervals[intervals[:, 0].argsort()]

    # we know we will have at least one interval
    unions = [intervals[0].tolist()]

    for begin, end in intervals[1:]:
        if unions[-1][1] >= begin:
            unions[-1][1] = max(unions[-1][1], end)
        else:
            unions.append([begin, end])

    return np.array(unions)
