"""Coordinate and geometry warping and reprojection"""

from warnings import warn

from fiona._transform import _transform, _transform_geom
from fiona.compat import DICT_TYPES
from fiona.errors import FionaDeprecationWarning
from fiona.model import decode_object, Geometry


def transform(src_crs, dst_crs, xs, ys):
    """Transform coordinates from one reference system to another.

    Parameters
    ----------
    src_crs: str or dict
        A string like 'EPSG:4326' or a dict of proj4 parameters like
        {'proj': 'lcc', 'lat_0': 18.0, 'lat_1': 18.0, 'lon_0': -77.0}
        representing the coordinate reference system on the "source"
        or "from" side of the transformation.
    dst_crs: str or dict
        A string or dict representing the coordinate reference system
        on the "destination" or "to" side of the transformation.
    xs: sequence of float
        A list or tuple of x coordinate values. Must have the same
        length as the ``ys`` parameter.
    ys: sequence of float
        A list or tuple of y coordinate values. Must have the same
        length as the ``xs`` parameter.

    Returns
    -------
    xp, yp: list of float
        A pair of transformed coordinate sequences. The elements of
        ``xp`` and ``yp`` correspond exactly to the elements of the
        ``xs`` and ``ys`` input parameters.

    Examples
    --------

    >>> transform('EPSG:4326', 'EPSG:26953', [-105.0], [40.0])
    ([957097.0952383667], [378940.8419189212])

    """
    # Function is implemented in the _transform C extension module.
    return _transform(src_crs, dst_crs, xs, ys)


def transform_geom(
    src_crs,
    dst_crs,
    geom,
    antimeridian_cutting=False,
    antimeridian_offset=10.0,
    precision=-1,
):
    """Transform a geometry obj from one reference system to another.

    Parameters
    ----------
    src_crs: str or dict
        A string like 'EPSG:4326' or a dict of proj4 parameters like
        {'proj': 'lcc', 'lat_0': 18.0, 'lat_1': 18.0, 'lon_0': -77.0}
        representing the coordinate reference system on the "source"
        or "from" side of the transformation.
    dst_crs: str or dict
        A string or dict representing the coordinate reference system
        on the "destination" or "to" side of the transformation.
    geom: obj
        A GeoJSON-like geometry object with 'type' and 'coordinates'
        members or an iterable of GeoJSON-like geometry objects.
    antimeridian_cutting: bool, optional
        ``True`` to cut output geometries in two at the antimeridian,
        the default is ``False`.
    antimeridian_offset: float, optional
        A distance in decimal degrees from the antimeridian, outside of
        which geometries will not be cut.
    precision: int, optional
        Round geometry coordinates to this number of decimal places.
        This parameter is deprecated and will be removed in 2.0.

    Returns
    -------
    obj
        A new GeoJSON-like geometry (or a list of GeoJSON-like geometries
        if an iterable was given as input) with transformed coordinates. Note
        that if the output is at the antimeridian, it may be cut and
        of a different geometry ``type`` than the input, e.g., a
        polygon input may result in multi-polygon output.

    Examples
    --------

    >>> transform_geom(
    ...     'EPSG:4326', 'EPSG:26953',
    ...     {'type': 'Point', 'coordinates': [-105.0, 40.0]})
    {'type': 'Point', 'coordinates': (957097.0952383667, 378940.8419189212)}

    """
    if precision >= 0:
        warn(
            "The precision keyword argument is deprecated and will be removed in 2.0",
            FionaDeprecationWarning,
        )

    # Function is implemented in the _transform C extension module.
    if isinstance(geom, (Geometry,) + DICT_TYPES):
        return _transform_geom(
            src_crs,
            dst_crs,
            decode_object(geom),
            antimeridian_cutting,
            antimeridian_offset,
            precision,
        )
    else:
        return _transform_geom(
            src_crs,
            dst_crs,
            (decode_object(g) for g in geom),
            antimeridian_cutting,
            antimeridian_offset,
            precision,
        )
