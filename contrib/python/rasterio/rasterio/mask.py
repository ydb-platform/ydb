"""Mask the area outside of the input shapes with no data."""

import logging
import warnings

import numpy as np

from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window


logger = logging.getLogger(__name__)


def raster_geometry_mask(dataset, shapes, all_touched=False, invert=False,
                         crop=False, pad=False, pad_width=0.5):
    """Create a mask from shapes, transform, and optional window within original
    raster.

    By default, mask is intended for use as a numpy mask, where pixels that
    overlap shapes are False.

    If shapes do not overlap the raster and crop=True, a ValueError is
    raised.  Otherwise, a warning is raised, and a completely True mask
    is returned (if invert is False).

    Parameters
    ----------
    dataset : a dataset object opened in 'r' mode
        Raster for which the mask will be created.
    shapes : iterable object
        The values must be a GeoJSON-like dict or an object that implements
        the Python geo interface protocol (such as a Shapely Polygon).
    all_touched : bool (opt)
        Include a pixel in the mask if it touches any of the shapes.
        If False (default), include a pixel only if its center is within one of
        the shapes, or if it is selected by Bresenham's line algorithm.
    invert : bool (opt)
        Determines whether to mask pixels outside or inside the shapes.
        The default (False) is to mask pixels outside shapes.
        When invert is used with crop, the area outside the cropping window is
        considered selected and should be processed accordingly by the user.
    crop : bool (opt)
        Crop the dataset to the extent of the shapes. Useful for
        processing rasters where shapes cover only a relatively small area.
        Defaults to False.
    pad : bool (opt)
        If True, the features will be padded in each direction by
        one half of a pixel prior to cropping dataset. Defaults to False.
    pad_width : float (opt)
        If pad is set (to maintain back-compatibility), then this will be the
        pixel-size width of the padding around the mask.

    Returns
    -------
    tuple

        Three elements:

            mask : np.ndarray of type 'bool'
                Mask suitable for use in a MaskedArray where valid pixels are
                marked `False` and invalid pixels are marked `True`.

            out_transform : affine.Affine()
                Information for mapping pixel coordinates in `masked` to another
                coordinate system.

            window: rasterio.windows.Window instance
                Window within original raster covered by shapes.  None if crop
                is False.
    """
    if crop and pad:
        pad_x = pad_width
        pad_y = pad_width
    else:
        pad_x = 0
        pad_y = 0

    try:
        window = geometry_window(dataset, shapes, pad_x=pad_x, pad_y=pad_y)

    except WindowError:
        # If shapes do not overlap raster, raise Exception or UserWarning
        # depending on value of crop
        if crop:
            raise ValueError('Input shapes do not overlap raster.')
        else:
            warnings.warn('shapes are outside bounds of raster. '
                          'Are they in different coordinate reference systems?')

        # Return an entirely True mask (if invert is False)
        mask = np.ones(shape=dataset.shape[-2:], dtype=bool)
        if invert:
            mask = ~mask
        return mask, dataset.transform, None

    if crop:
        transform = dataset.window_transform(window)
        out_shape = (int(window.height), int(window.width))

    else:
        window = None
        transform = dataset.transform
        out_shape = (int(dataset.height), int(dataset.width))

    mask = geometry_mask(shapes, transform=transform, invert=invert,
                         out_shape=out_shape, all_touched=all_touched)

    return mask, transform, window


def mask(dataset, shapes, all_touched=False, invert=False, nodata=None,
         filled=True, crop=False, pad=False, pad_width=0.5, indexes=None):
    """Creates a masked or filled array using input shapes.
    Pixels are masked or set to nodata outside the input shapes, unless
    `invert` is `True`.

    If shapes do not overlap the raster and crop=True, a ValueError is
    raised.  Otherwise, a warning is raised.

    Parameters
    ----------
    dataset : a dataset object opened in 'r' mode
        Raster to which the mask will be applied.
    shapes : iterable object
        The values must be a GeoJSON-like dict or an object that implements
        the Python geo interface protocol (such as a Shapely Polygon).
    all_touched : bool (opt)
        Include a pixel in the mask if it touches any of the shapes.
        If False (default), include a pixel only if its center is within one of
        the shapes, or if it is selected by Bresenham's line algorithm.
    invert : bool (opt)
        If False (default) pixels outside shapes will be masked.  If True,
        pixels inside shape will be masked.
    nodata : int or float (opt)
        Value representing nodata within each raster band. If not set,
        defaults to the nodata value for the input raster. If there is no
        set nodata value for the raster, it defaults to 0.
    filled : bool (opt)
        If True, the pixels outside the features will be set to nodata.
        If False, the output array will contain the original pixel data,
        and only the mask will be based on shapes.  Defaults to True.
    crop : bool (opt)
        Whether to crop the raster to the extent of the shapes. Defaults to
        False.
    pad : bool (opt)
        If True, the features will be padded in each direction by
        one half of a pixel prior to cropping raster. Defaults to False.
    indexes : list of ints or a single int (opt)
        If `indexes` is a list, the result is a 3D array, but is
        a 2D array if it is a band index number.

    Returns
    -------
    tuple

        Two elements:

            masked : numpy.ndarray or numpy.ma.MaskedArray
                Data contained in the raster after applying the mask. If
                `filled` is `True` and `invert` is `False`, the return will be
                an array where pixels outside shapes are set to the nodata value
                (or nodata inside shapes if `invert` is `True`).

                If `filled` is `False`, the return will be a MaskedArray in
                which pixels outside shapes are `True` (or `False` if `invert`
                is `True`).

            out_transform : affine.Affine()
                Information for mapping pixel coordinates in `masked` to another
                coordinate system.
    """

    if nodata is None:
        if dataset.nodata is not None:
            nodata = dataset.nodata
        else:
            nodata = 0

    shape_mask, transform, window = raster_geometry_mask(
        dataset, shapes, all_touched=all_touched, invert=invert, crop=crop,
        pad=pad, pad_width=pad_width)

    if indexes is None:
        out_shape = (dataset.count, ) + shape_mask.shape
    elif isinstance(indexes, int):
        out_shape = shape_mask.shape
    else:
        out_shape = (len(indexes), ) + shape_mask.shape

    out_image = dataset.read(
        window=window, out_shape=out_shape, masked=True, indexes=indexes)

    out_image.mask = out_image.mask | shape_mask

    if filled:
        out_image = out_image.filled(nodata)

    return out_image, transform
