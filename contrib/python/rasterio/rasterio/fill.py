"""Fill holes in raster dataset by interpolation from the edges."""

from numpy.ma import MaskedArray

from rasterio._fill import _fillnodata
from rasterio.env import ensure_env
from rasterio import dtypes


@ensure_env
def fillnodata(
    image, mask=None, max_search_distance=100.0, smoothing_iterations=0, **filloptions
):
    """Fill holes in raster data by interpolation

    This algorithm will interpolate values for all designated nodata
    pixels (marked by zeros in `mask`). For each pixel a four direction
    conic search is done to find values to interpolate from (using
    inverse distance weighting). Once all values are interpolated, zero
    or more smoothing iterations (3x3 average filters on interpolated
    pixels) are applied to smooth out artifacts.

    This algorithm is generally suitable for interpolating missing
    regions of fairly continuously varying rasters (such as elevation
    models for instance). It is also suitable for filling small holes
    and cracks in more irregularly varying images (like aerial photos).
    It is generally not so great for interpolating a raster from sparse
    point data.

    Parameters
    ----------
    image : numpy.ndarray
        The source image with holes to be filled. If a MaskedArray, the
        inverse of its mask will define the pixels to be filled --
        unless the ``mask`` argument is not None (see below).`
    mask : numpy.ndarray, optional
        A mask band indicating which pixels to interpolate. Pixels to
        interpolate into are indicated by the value 0. Values
        > 0 indicate areas to use during interpolation. Must be same
        shape as image. This array always takes precedence over the
        image's mask (see above). If None, the inverse of the image's
        mask will be used if available.
    max_search_distance : float, optional
        The maximum number of pixels to search in all directions to
        find values to interpolate from. The default is 100.
    smoothing_iterations : integer, optional
        The number of 3x3 smoothing filter passes to run. The default is
        0.
    filloptions :
        Keyword arguments providing finer control over filling. See
        https://gdal.org/en/stable/api/gdal_alg.html. Lowercase option
        names and numerical values are allowed. For example:
        nodata=0 is a valid keyword argument.

    Returns
    -------
    numpy.ndarray :
        The filled raster array.
    """
    if mask is None and isinstance(image, MaskedArray):
        mask = ~image.mask
    if not dtypes.is_ndarray(mask):
        raise ValueError("An mask array is required")

    if isinstance(image, MaskedArray):
        image = image.data
    if not dtypes.is_ndarray(image):
        raise ValueError("An image array is required")

    max_search_distance = float(max_search_distance)
    smoothing_iterations = int(smoothing_iterations)
    return _fillnodata(image, mask, max_search_distance, smoothing_iterations, **filloptions)
