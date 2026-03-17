# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Generic functionality to support Cartopy vector transforms.

"""

import numpy as np


try:
    from scipy.interpolate import griddata
except ImportError as e:
    raise ImportError("Regridding vectors requires scipy.") from e


def _interpolate_to_grid(nx, ny, x, y, *scalars, **kwargs):
    """
    Interpolate two vector components and zero or more scalar fields,
    which can be irregular, to a regular grid.

    Parameters
    ----------
    nx
        Number of points at which to interpolate in x direction.
    ny
        Number of points at which to interpolate in y direction.
    x
        Array of source points in x direction.
    y
        Array of source points in y direction.

    Other Parameters
    ----------------
    scalars
        Zero or more scalar fields to regrid along with the vector
        components.
    target_extent
        The extent in the target CRS that the grid should occupy, in the
        form ``(x-lower, x-upper, y-lower, y-upper)``. Defaults to cover
        the full extent of the vector field.

    """
    target_extent = kwargs.get('target_extent', None)
    if target_extent is None:
        target_extent = (x.min(), x.max(), y.min(), y.max())
    x0, x1, y0, y1 = target_extent
    xr = x1 - x0
    yr = y1 - y0
    points = np.column_stack([(x.ravel() - x0) / xr, (y.ravel() - y0) / yr])
    x_grid, y_grid = np.meshgrid(np.linspace(0, 1, nx),
                                 np.linspace(0, 1, ny))
    s_grid_tuple = tuple()
    for s in scalars:
        s_grid_tuple += (griddata(points, s.ravel(), (x_grid, y_grid),
                                  method='linear'),)
    return (x_grid * xr + x0, y_grid * yr + y0) + s_grid_tuple


def vector_scalar_to_grid(src_crs, target_proj, regrid_shape, x, y, u, v,
                          *scalars, **kwargs):
    """
    Transform and interpolate a vector field to a regular grid in the
    target projection.

    Parameters
    ----------
    src_crs
        The :class:`~cartopy.crs.CRS` that represents the coordinate
        system the vectors are defined in.
    target_proj
        The :class:`~cartopy.crs.Projection` that represents the
        projection the vectors are to be transformed to.
    regrid_shape
        The regular grid dimensions. If a single integer then the grid
        will have that number of points in the x and y directions. A
        2-tuple of integers specify the size of the regular grid in the
        x and y directions respectively.
    x, y
        The x and y coordinates, in the source CRS coordinates,
        where the vector components are located.
    u, v
        The grid eastward and grid northward components of the
        vector field respectively. Their shapes must match.

    Other Parameters
    ----------------
    scalars
        Zero or more scalar fields to regrid along with the vector
        components. Each scalar field must have the same shape as the
        vector components.
    target_extent
        The extent in the target CRS that the grid should occupy, in the
        form ``(x-lower, x-upper, y-lower, y-upper)``. Defaults to cover
        the full extent of the vector field.

    Returns
    -------
    x_grid, y_grid
        The x and y coordinates of the regular grid points as
        2-dimensional arrays.
    u_grid, v_grid
        The eastward and northward components of the vector field on
        the regular grid.
    scalars_grid
        The scalar fields on the regular grid. The number of returned
        scalar fields is the same as the number that were passed in.

    """
    x = np.asanyarray(x)
    y = np.asanyarray(y)
    u = np.asanyarray(u)
    v = np.asanyarray(v)

    if u.shape != v.shape:
        raise ValueError('u and v must be the same shape')
    if x.shape != u.shape:
        x, y = np.meshgrid(x, y)
        if not (x.shape == y.shape == u.shape):
            raise ValueError('x and y coordinates are not compatible '
                             'with the shape of the vector components')
    if scalars:
        np_like_scalars = ()
        for s in scalars:
            s = np.asanyarray(s)
            np_like_scalars = np_like_scalars + (s,)
            if s.shape != u.shape:
                raise ValueError('scalar fields must have the same '
                                 'shape as the vector components')
        scalars = np_like_scalars
    try:
        nx, ny = regrid_shape
    except TypeError:
        nx = ny = regrid_shape
    if target_proj == src_crs:
        # Just immediately regrid, interpolate and return
        return _interpolate_to_grid(nx, ny, x, y, u, v, *scalars, **kwargs)

    # We need to transform the vectors from the source to target frame
    # Convert coordinates to the target projection.
    proj_xyz = target_proj.transform_points(src_crs, x, y)
    targetx, targety = proj_xyz[..., 0], proj_xyz[..., 1]

    # Create the grid in the target frame
    gridx, gridy = _interpolate_to_grid(nx, ny, targetx, targety, **kwargs)

    # Bring the x/y target grid coordinates back into the source frame
    src_xyz = src_crs.transform_points(target_proj, gridx, gridy)
    # Mask the invalid points that were outside the domain
    src_xyz = np.ma.array(src_xyz, mask=~np.isfinite(src_xyz))
    sourcex, sourcey = src_xyz[..., 0], src_xyz[..., 1]

    # Now interpolate in the source frame
    x0, x1 = sourcex.min(), sourcex.max()
    y0, y1 = sourcey.min(), sourcey.max()
    xr = x1 - x0
    yr = y1 - y0
    # We also need to transform the original source points to the source
    # projection to account for original points outside the wrapped domain
    xyz = src_crs.transform_points(src_crs, x, y)
    x, y = xyz[..., 0], xyz[..., 1]
    points = np.column_stack([(x.ravel() - x0) / xr,
                              (y.ravel() - y0) / yr])
    newx = (sourcex - x0) / xr
    newy = (sourcey - y0) / yr
    s_grid_tuple = tuple()
    for s in (u, v) + scalars:
        s_grid_tuple += (griddata(points, s.ravel(), (newx, newy),
                                  method='linear'),)

    u, v = s_grid_tuple[0], s_grid_tuple[1]
    # Finally, transform the vectors (in the source frame) to the target CRS.
    u, v = target_proj.transform_vectors(src_crs, sourcex, sourcey, u, v)

    return (gridx, gridy, u, v) + s_grid_tuple[2:]
