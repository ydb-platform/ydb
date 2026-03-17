# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Utilities that are useful in conjunction with cartopy.

"""
import numpy as np
import numpy.ma as ma


def add_cyclic_point(data, coord=None, axis=-1):
    """
    Add a cyclic point to an array and optionally a corresponding
    coordinate.

    Parameters
    ----------
    data
        An n-dimensional array of data to add a cyclic point to.
    coord : optional
        A 1-dimensional array which specifies the coordinate values for
        the dimension the cyclic point is to be added to. The coordinate
        values must be regularly spaced. Defaults to None.
    axis : optional
        Specifies the axis of the data array to add the cyclic point to.
        Defaults to the right-most axis.

    Returns
    -------
    cyclic_data
        The data array with a cyclic point added.
    cyclic_coord
        The coordinate with a cyclic point, only returned if the coord
        keyword was supplied.

    Examples
    --------
    Adding a cyclic point to a data array, where the cyclic dimension is
    the right-most dimension.

    >>> import numpy as np
    >>> data = np.ones([5, 6]) * np.arange(6)
    >>> cyclic_data = add_cyclic_point(data)
    >>> print(cyclic_data)  # doctest: +NORMALIZE_WHITESPACE
    [[0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]]

    Adding a cyclic point to a data array and an associated coordinate

    >>> lons = np.arange(0, 360, 60)
    >>> cyclic_data, cyclic_lons = add_cyclic_point(data, coord=lons)
    >>> print(cyclic_data)  # doctest: +NORMALIZE_WHITESPACE
    [[0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]]
    >>> print(cyclic_lons)
    [  0  60 120 180 240 300 360]

    """
    if coord is not None:
        if coord.ndim != 1:
            raise ValueError('The coordinate must be 1-dimensional.')
        if len(coord) != data.shape[axis]:
            raise ValueError(f'The length of the coordinate does not match '
                             f'the size of the corresponding dimension of '
                             f'the data array: len(coord) = {len(coord)}, '
                             f'data.shape[{axis}] = {data.shape[axis]}.')
        delta_coord = np.diff(coord)
        if not np.allclose(delta_coord, delta_coord[0]):
            raise ValueError('The coordinate must be equally spaced.')
        new_coord = ma.concatenate((coord, coord[-1:] + delta_coord[0]))
    slicer = [slice(None)] * data.ndim
    try:
        slicer[axis] = slice(0, 1)
    except IndexError:
        raise ValueError('The specified axis does not correspond to an '
                         'array dimension.')
    new_data = ma.concatenate((data, data[tuple(slicer)]), axis=axis)
    if coord is None:
        return_value = new_data
    else:
        return_value = new_data, new_coord
    return return_value


def _add_cyclic_data(data, axis=-1):
    """
    Add a cyclic point to a data array.

    Parameters
    ----------
    data : ndarray
        An n-dimensional array of data to add a cyclic point to.
    axis : int, optional
        Specifies the axis of the data array to add the cyclic point to.
        Defaults to the right-most axis.

    Returns
    -------
    The data array with a cyclic point added.

    """
    slicer = [slice(None)] * data.ndim
    try:
        slicer[axis] = slice(0, 1)
    except IndexError:
        raise ValueError(
            'The specified axis does not correspond to an array dimension.')
    npc = np.ma if np.ma.is_masked(data) else np
    return npc.concatenate((data, data[tuple(slicer)]), axis=axis)


def _add_cyclic_x(x, axis=-1, cyclic=360):
    """
    Add a cyclic point to a x/longitude coordinate array.

    Parameters
    ----------
    x : ndarray
        An array which specifies the x-coordinate values for
        the dimension the cyclic point is to be added to.
    axis : int, optional
        Specifies the axis of the x-coordinate array to add the cyclic point
        to. Defaults to the right-most axis.
    cyclic : float, optional
        Width of periodic domain (default: 360)

    Returns
    -------
    The coordinate array ``x`` with a cyclic point added.

    """
    npc = np.ma if np.ma.is_masked(x) else np
    # get cyclic x-coordinates
    # cx is the code from basemap (addcyclic)
    # https://github.com/matplotlib/basemap/blob/master/lib/mpl_toolkits/basemap/__init__.py
    cx = (np.take(x, [0], axis=axis) +
          cyclic * np.sign(np.diff(np.take(x, [0, -1], axis=axis),
                                   axis=axis)))
    # basemap ensures that the values do not exceed cyclic
    # (next code line). We do not do this to deal with rotated grids that
    # might have values not exactly 0.
    #     cx = npc.where(cx <= cyclic, cx, np.mod(cx, cyclic))
    return npc.concatenate((x, cx), axis=axis)


def has_cyclic(x, axis=-1, cyclic=360, precision=1e-4):
    """
    Check if x/longitude coordinates already have a cyclic point.

    Checks all differences between the first and last
    x-coordinates along ``axis`` to be less than ``precision``.

    Parameters
    ----------
    x : ndarray
        An array with the x-coordinate values to be checked for cyclic points.
    axis : int, optional
        Specifies the axis of the ``x`` array to be checked.
        Defaults to the right-most axis.
    cyclic : float, optional
        Width of periodic domain (default: 360).
    precision : float, optional
        Maximal difference between first and last x-coordinate to detect
        cyclic point (default: 1e-4).

    Returns
    -------
    True if a cyclic point was detected along the given axis,
    False otherwise.

    Examples
    --------
    Check for cyclic x-coordinate in one dimension.
    >>> import numpy as np
    >>> lons = np.arange(0, 360, 60)
    >>> clons = np.arange(0, 361, 60)
    >>> print(has_cyclic(lons))
    False
    >>> print(has_cyclic(clons))
    True

    Check for cyclic x-coordinate in two dimensions.
    >>> lats = np.arange(-90, 90, 30)
    >>> lon2d, lat2d = np.meshgrid(lons, lats)
    >>> clon2d, clat2d = np.meshgrid(clons, lats)
    >>> print(has_cyclic(lon2d))
    False
    >>> print(has_cyclic(clon2d))
    True

    """
    npc = np.ma if np.ma.is_masked(x) else np
    # transform to 0-cyclic, assuming e.g. -180 to 180 if any < 0
    x1 = np.mod(npc.where(x < 0, x + cyclic, x), cyclic)
    dd = np.diff(np.take(x1, [0, -1], axis=axis), axis=axis)
    if npc.all(np.abs(dd) < precision):
        return True
    else:
        return False


def add_cyclic(data, x=None, y=None, axis=-1,
               cyclic=360, precision=1e-4):
    """
    Add a cyclic point to an array and optionally corresponding
    x/longitude and y/latitude coordinates.

    Checks all differences between the first and last
    x-coordinates along ``axis`` to be less than ``precision``.

    Parameters
    ----------
    data : ndarray
        An n-dimensional array of data to add a cyclic point to.
    x : ndarray, optional
        An n-dimensional array which specifies the x-coordinate values
        for the dimension the cyclic point is to be added to, i.e. normally the
        longitudes. Defaults to None.

        If ``x`` is given then *add_cyclic* checks if a cyclic point is
        already present by checking all differences between the first and last
        coordinates to be less than ``precision``.
        No point is added if a cyclic point was detected.

        If ``x`` is 1- or 2-dimensional, ``x.shape[-1]`` must equal
        ``data.shape[axis]``, otherwise ``x.shape[axis]`` must equal
        ``data.shape[axis]``.
    y : ndarray, optional
        An n-dimensional array with the values of the y-coordinate, i.e.
        normally the latitudes.
        The cyclic point simply copies the last column. Defaults to None.

        No cyclic point is added if ``y`` is 1-dimensional.
        If ``y`` is 2-dimensional, ``y.shape[-1]`` must equal
        ``data.shape[axis]``, otherwise ``y.shape[axis]`` must equal
        ``data.shape[axis]``.
    axis : int, optional
        Specifies the axis of the arrays to add the cyclic point to,
        i.e. axis with changing x-coordinates. Defaults to the right-most axis.
    cyclic : int or float, optional
        Width of periodic domain (default: 360).
    precision : float, optional
        Maximal difference between first and last x-coordinate to detect
        cyclic point (default: 1e-4).

    Returns
    -------
    cyclic_data
        The data array with a cyclic point added.
    cyclic_x
        The x-coordinate with a cyclic point, only returned if the ``x``
        keyword was supplied.
    cyclic_y
        The y-coordinate with the last column of the cyclic axis duplicated,
        only returned if ``x`` was 2- or n-dimensional and the ``y``
        keyword was supplied.

    Examples
    --------
    Adding a cyclic point to a data array, where the cyclic dimension is
    the right-most dimension.

    >>> import numpy as np
    >>> data = np.ones([5, 6]) * np.arange(6)
    >>> cyclic_data = add_cyclic(data)
    >>> print(cyclic_data)  # doctest: +NORMALIZE_WHITESPACE
    [[0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]]

    Adding a cyclic point to a data array and an associated x-coordinate.

    >>> lons = np.arange(0, 360, 60)
    >>> cyclic_data, cyclic_lons = add_cyclic(data, x=lons)
    >>> print(cyclic_data)  # doctest: +NORMALIZE_WHITESPACE
    [[0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]]
    >>> print(cyclic_lons)
    [  0  60 120 180 240 300 360]

    Adding a cyclic point to a data array and an associated 2-dimensional
    x-coordinate.

    >>> lons = np.arange(0, 360, 60)
    >>> lats = np.arange(-90, 90, 180/5)
    >>> lon2d, lat2d = np.meshgrid(lons, lats)
    >>> cyclic_data, cyclic_lon2d = add_cyclic(data, x=lon2d)
    >>> print(cyclic_data)  # doctest: +NORMALIZE_WHITESPACE
    [[0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]]
    >>> print(cyclic_lon2d)
    [[  0  60 120 180 240 300 360]
     [  0  60 120 180 240 300 360]
     [  0  60 120 180 240 300 360]
     [  0  60 120 180 240 300 360]
     [  0  60 120 180 240 300 360]]

    Adding a cyclic point to a data array and the associated 2-dimensional
    x- and y-coordinates.

    >>> lons = np.arange(0, 360, 60)
    >>> lats = np.arange(-90, 90, 180/5)
    >>> lon2d, lat2d = np.meshgrid(lons, lats)
    >>> cyclic_data, cyclic_lon2d, cyclic_lat2d = add_cyclic(
    ...     data, x=lon2d, y=lat2d)
    >>> print(cyclic_data)  # doctest: +NORMALIZE_WHITESPACE
    [[0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]
     [0. 1. 2. 3. 4. 5. 0.]]
    >>> print(cyclic_lon2d)
    [[  0  60 120 180 240 300 360]
     [  0  60 120 180 240 300 360]
     [  0  60 120 180 240 300 360]
     [  0  60 120 180 240 300 360]
     [  0  60 120 180 240 300 360]]
    >>> print(cyclic_lat2d)
    [[-90. -90. -90. -90. -90. -90. -90.]
     [-54. -54. -54. -54. -54. -54. -54.]
     [-18. -18. -18. -18. -18. -18. -18.]
     [ 18.  18.  18.  18.  18.  18.  18.]
     [ 54.  54.  54.  54.  54.  54.  54.]]

    Not adding a cyclic point if cyclic point detected in x.

    >>> lons = np.arange(0, 361, 72)
    >>> lats = np.arange(-90, 90, 180/5)
    >>> lon2d, lat2d = np.meshgrid(lons, lats)
    >>> cyclic_data, cyclic_lon2d, cyclic_lat2d = add_cyclic(
    ...     data, x=lon2d, y=lat2d)
    >>> print(cyclic_data)  # doctest: +NORMALIZE_WHITESPACE
    [[0. 1. 2. 3. 4. 5.]
     [0. 1. 2. 3. 4. 5.]
     [0. 1. 2. 3. 4. 5.]
     [0. 1. 2. 3. 4. 5.]
     [0. 1. 2. 3. 4. 5.]]
    >>> print(cyclic_lon2d)
    [[  0  72 144 216 288 360]
     [  0  72 144 216 288 360]
     [  0  72 144 216 288 360]
     [  0  72 144 216 288 360]
     [  0  72 144 216 288 360]]
    >>> print(cyclic_lat2d)
    [[-90. -90. -90. -90. -90. -90.]
     [-54. -54. -54. -54. -54. -54.]
     [-18. -18. -18. -18. -18. -18.]
     [ 18.  18.  18.  18.  18.  18.]
     [ 54.  54.  54.  54.  54.  54.]]

    """
    if x is None:
        return _add_cyclic_data(data, axis=axis)
    # if x was given
    if x.ndim > 2:
        xaxis = axis
    else:
        xaxis = -1
    if x.shape[xaxis] != data.shape[axis]:
        estr = (f'x.shape[{xaxis}] does not match the size of the'
                f' corresponding dimension of the data array:'
                f' x.shape[{xaxis}] = {x.shape[xaxis]},'
                f' data.shape[{axis}] = {data.shape[axis]}.')
        raise ValueError(estr)
    if has_cyclic(x, axis=xaxis, cyclic=cyclic, precision=precision):
        if y is None:
            return data, x
        # if y was given
        return data, x, y
    # if not has_cyclic, add cyclic points to data and x
    out_data = _add_cyclic_data(data, axis=axis)
    out_x = _add_cyclic_x(x, axis=xaxis, cyclic=cyclic)
    if y is None:
        return out_data, out_x
    # if y was given
    if y.ndim == 1:
        return out_data, out_x, y
    if y.ndim > 2:
        yaxis = axis
    else:
        yaxis = -1
    if y.shape[yaxis] != data.shape[axis]:
        estr = (f'y.shape[{yaxis}] does not match the size of the'
                f' corresponding dimension of the data array:'
                f' y.shape[{yaxis}] = {y.shape[yaxis]},'
                f' data.shape[{axis}] = {data.shape[axis]}.')
        raise ValueError(estr)
    out_y = _add_cyclic_data(y, axis=yaxis)
    return out_data, out_x, out_y
