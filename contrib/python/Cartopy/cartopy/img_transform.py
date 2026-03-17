# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Generic functionality to support Cartopy image transformations.

"""

import numpy as np


try:
    from pykdtree.kdtree import KDTree as _kdtreeClass
    _is_pykdtree = True
except ImportError:
    try:
        from scipy.spatial import cKDTree as _kdtreeClass
    except ImportError as e:
        raise ImportError("Using image transforms requires either "
                          "pykdtree or scipy.") from e
    _is_pykdtree = False

import cartopy.crs as ccrs


def mesh_projection(projection, nx, ny,
                    x_extents=(None, None),
                    y_extents=(None, None)):
    """
    Return sample points in the given projection which span the entire
    projection range evenly.

    The range of the x-direction and y-direction sample points will be
    within the bounds of the projection or specified extents.

    Parameters
    ----------
    projection
        A :class:`~cartopy.crs.Projection` instance.
    nx: int
        The number of sample points in the projection x-direction.
    ny: int
        The number of sample points in the projection y-direction.
    x_extents: optional
        The (lower, upper) x-direction extent of the projection.
        Defaults to the :attr:`~cartopy.crs.Projection.x_limits`.
    y_extents: optional
        The (lower, upper) y-direction extent of the projection.
        Defaults to the :attr:`~cartopy.crs.Projection.y_limits`.

    Returns
    -------
    A tuple of three items.
        The x-direction sample points
        :class:`numpy.ndarray` of shape (nx, ny), y-direction
        sample points :class:`numpy.ndarray` of shape (nx, ny),
        and the extent of the projection range as
        ``(x-lower, x-upper, y-lower, y-upper)``.

    """

    def extent(specified, default, index):
        if specified[index] is not None:
            return specified[index]
        else:
            return default[index]

    # Establish the x-direction and y-direction extents.
    x_lower = extent(x_extents, projection.x_limits, 0)
    x_upper = extent(x_extents, projection.x_limits, 1)
    y_lower = extent(y_extents, projection.y_limits, 0)
    y_upper = extent(y_extents, projection.y_limits, 1)

    # Calculate evenly spaced sample points spanning the
    # extent - excluding endpoint.
    x, xstep = np.linspace(x_lower, x_upper, nx, retstep=True,
                           endpoint=False)
    y, ystep = np.linspace(y_lower, y_upper, ny, retstep=True,
                           endpoint=False)

    # Deal with single point corner case and the difference
    # between np.linspace v1.9 and v1.10+ retstep nan result.
    if nx == 1 and np.isnan(xstep):
        xstep = x_upper - x_lower

    if ny == 1 and np.isnan(ystep):
        ystep = y_upper - y_lower

    # Offset the sample points to be within the extent range.
    x += 0.5 * xstep
    y += 0.5 * ystep

    # Generate the x-direction and y-direction meshgrids.
    x, y = np.meshgrid(x, y)
    return x, y, [x_lower, x_upper, y_lower, y_upper]


def warp_img(fname, target_proj, source_proj=None, target_res=(400, 200)):
    """
    Regrid the image file from the source projection to the target projection.

    Parameters
    ----------
    fname
        Image filename to be loaded and warped.
    target_proj
        The target :class:`~cartopy.crs.Projection` instance for the image.
    source_proj: optional
        The source :class:`~cartopy.crs.Projection` instance of the image.
        Defaults to a :class:`~cartopy.crs.PlateCarree` projection.
    target_res: optional
        The (nx, ny) resolution of the target projection. Where nx defaults to
        400 sample points, and ny defaults to 200 sample points.

    """

    if source_proj is None:
        source_proj = ccrs.PlateCarree()

    raise NotImplementedError('Not yet implemented.')


def warp_array(array, target_proj, source_proj=None, target_res=(400, 200),
               source_extent=None, target_extent=None,
               mask_extrapolated=False):
    """
    Regrid the data array from the source projection to the target projection.

    Also see, :func:`~cartopy.img_transform.regrid`.

    Parameters
    ----------
    array
        The :class:`numpy.ndarray` of data to be regridded to the target
        projection.
    target_proj
        The target :class:`~cartopy.crs.Projection` instance for the data.
    source_proj: optional
        The source :class:`~cartopy.crs.Projection' instance of the data.
        Defaults to a :class:`~cartopy.crs.PlateCarree` projection.
    target_res: optional
        The (nx, ny) resolution of the target projection. Where nx defaults to
        400 sample points, and ny defaults to 200 sample points.
    source_extent: optional
        The (x-lower, x-upper, y-lower, y-upper) extent in native
        source projection coordinates.
    target_extent: optional
        The (x-lower, x-upper, y-lower, y-upper) extent in native
        target projection coordinates.
    mask_extrapolated: optional
        Assume that the source coordinate is rectilinear and so mask the
        resulting target grid values which lie outside the source grid
        domain.

    Returns
    -------
    array, extent
        A tuple of the regridded :class:`numpy.ndarray` in the target
        projection and the (x-lower, x-upper, y-lower, y-upper) target
        projection extent.

    """

    # source_extent is in source coordinates.
    if source_extent is None:
        source_extent = [None] * 4
    # target_extent is in target coordinates.
    if target_extent is None:
        target_extent = [None] * 4

    source_x_extents = source_extent[:2]
    source_y_extents = source_extent[2:]

    target_x_extents = target_extent[:2]
    target_y_extents = target_extent[2:]

    if source_proj is None:
        source_proj = ccrs.PlateCarree()

    ny, nx = array.shape[:2]
    source_native_xy = mesh_projection(source_proj, nx, ny,
                                       x_extents=source_x_extents,
                                       y_extents=source_y_extents)

    # XXX Take into account the extents of the original to determine
    # target_extents?
    target_native_x, target_native_y, extent = mesh_projection(
        target_proj, target_res[0], target_res[1],
        x_extents=target_x_extents, y_extents=target_y_extents)

    array = regrid(array, source_native_xy[0], source_native_xy[1],
                   source_proj, target_proj,
                   target_native_x, target_native_y,
                   mask_extrapolated)
    return array, extent


def _determine_bounds(x_coords, y_coords, source_cs):
    # Returns bounds corresponding to one or two rectangles depending on
    # transformation between ranges.
    bounds = dict(x=[])
    half_px = abs(np.diff(x_coords[:2])).max() / 2.

    if (((hasattr(source_cs, 'is_geodetic') and
            source_cs.is_geodetic()) or
            isinstance(source_cs, ccrs.PlateCarree)) and x_coords.max() > 180):
        if x_coords.min() < 180:
            bounds['x'].append([x_coords.min() - half_px, 180])
            bounds['x'].append([-180, x_coords.max() - 360 + half_px])
        else:
            bounds['x'].append([x_coords.min() - 180 - half_px,
                                x_coords.max() - 180 + half_px])
    else:
        bounds['x'].append([x_coords.min() - half_px,
                            x_coords.max() + half_px])

    # y_coords are the centers, so adjust a half-pixel out in y too
    half_px = abs(np.diff(y_coords, axis=0)).max() / 2.
    bounds['y'] = [y_coords.min() - half_px, y_coords.max() + half_px]
    return bounds


def regrid(array, source_x_coords, source_y_coords, source_proj, target_proj,
           target_x_points, target_y_points, mask_extrapolated=False):
    """
    Regrid the data array from the source projection to the target projection.

    Parameters
    ----------
    array
        The :class:`numpy.ndarray` of data to be regridded to the
        target projection.
    source_x_coords
        A 2-dimensional source projection :class:`numpy.ndarray` of
        x-direction sample points.
    source_y_coords
        A 2-dimensional source projection :class:`numpy.ndarray` of
        y-direction sample points.
    source_proj
        The source :class:`~cartopy.crs.Projection` instance.
    target_proj
        The target :class:`~cartopy.crs.Projection` instance.
    target_x_points
        A 2-dimensional target projection :class:`numpy.ndarray` of
        x-direction sample points.
    target_y_points
        A 2-dimensional target projection :class:`numpy.ndarray` of
        y-direction sample points.
    mask_extrapolated: optional
        Assume that the source coordinate is rectilinear and so mask the
        resulting target grid values which lie outside the source grid domain.
        Defaults to False.

    Returns
    -------
    new_array
        The data array regridded in the target projection.

    """

    # Stack our original xyz array, this will also wrap coords when necessary
    xyz = source_proj.transform_points(source_proj,
                                       source_x_coords.flatten(),
                                       source_y_coords.flatten())
    # Transform the target points into the source projection
    target_xyz = source_proj.transform_points(target_proj,
                                              target_x_points.flatten(),
                                              target_y_points.flatten())

    # Find mask of valid points before querying kdtree: scipy >= 1.11 errors
    # when querying nan points, might as well use for pykdtree too.
    indices = np.zeros(target_xyz.shape[0], dtype=int)
    finite_xyz = np.all(np.isfinite(target_xyz), axis=-1)

    if _is_pykdtree:
        kdtree = _kdtreeClass(xyz)
        # Use sqr_dists=True because we don't care about distances,
        # and it saves a sqrt.
        _, indices[finite_xyz] = kdtree.query(target_xyz[finite_xyz, :],
                                              k=1,
                                              sqr_dists=True)
    else:
        # Versions of scipy >= v0.16 added the balanced_tree argument,
        # which caused the KDTree to hang with this input.
        kdtree = _kdtreeClass(xyz, balanced_tree=False)
        _, indices[finite_xyz] = kdtree.query(target_xyz[finite_xyz, :], k=1)

    mask = ~finite_xyz | (indices >= len(xyz))
    indices[mask] = 0

    desired_ny, desired_nx = target_x_points.shape

    # Squash the first two dims of the source array into one
    temp_array = array.reshape((-1,) + array.shape[2:])
    if np.any(mask):
        new_array = np.ma.array(temp_array[indices])
        new_array[mask] = np.ma.masked
    else:
        new_array = temp_array[indices]
    new_array.shape = (desired_ny, desired_nx) + (array.shape[2:])

    # Do double transform to clip points that do not map back and forth
    # to the same point to within a fixed fractional offset.
    # NOTE: This only needs to be done for (pseudo-)cylindrical projections,
    # or any others which have the concept of wrapping
    back_to_target_xyz = target_proj.transform_points(source_proj,
                                                      target_xyz[:, 0],
                                                      target_xyz[:, 1])
    back_to_target_x = back_to_target_xyz[:, 0].reshape(desired_ny,
                                                        desired_nx)
    back_to_target_y = back_to_target_xyz[:, 1].reshape(desired_ny,
                                                        desired_nx)
    FRACTIONAL_OFFSET_THRESHOLD = 0.1  # data has moved by 10% of the map

    x_extent = np.abs(target_proj.x_limits[1] - target_proj.x_limits[0])
    y_extent = np.abs(target_proj.y_limits[1] - target_proj.y_limits[0])

    non_self_inverse_points = (((np.abs(target_x_points - back_to_target_x) /
                                 x_extent) > FRACTIONAL_OFFSET_THRESHOLD) |
                               ((np.abs(target_y_points - back_to_target_y) /
                                 y_extent) > FRACTIONAL_OFFSET_THRESHOLD))
    if np.any(non_self_inverse_points):
        if not np.ma.isMaskedArray(new_array):
            new_array = np.ma.array(new_array, mask=False)

        new_array[non_self_inverse_points] = np.ma.masked

    # Transform the target points to the source projection and mask any points
    # that fall outside the original source domain.
    if mask_extrapolated:
        target_in_source_x = target_xyz[:, 0].reshape(desired_ny,
                                                      desired_nx)
        target_in_source_y = target_xyz[:, 1].reshape(desired_ny,
                                                      desired_nx)

        bounds = _determine_bounds(source_x_coords, source_y_coords,
                                   source_proj)

        outside_source_domain = ((target_in_source_y >= bounds['y'][1]) |
                                 (target_in_source_y <= bounds['y'][0]))

        tmp_inside = np.zeros_like(outside_source_domain)
        for bound_x in bounds['x']:
            tmp_inside = tmp_inside | ((target_in_source_x <= bound_x[1]) &
                                       (target_in_source_x >= bound_x[0]))
        outside_source_domain = outside_source_domain | ~tmp_inside

        if np.any(outside_source_domain):
            if not np.ma.isMaskedArray(new_array):
                new_array = np.ma.array(new_array, mask=False)
            new_array[outside_source_domain] = np.ma.masked

    return new_array
