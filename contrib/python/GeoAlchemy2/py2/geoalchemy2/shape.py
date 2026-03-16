"""
This module provides utility functions for integrating with Shapely.

.. note::

    As GeoAlchemy 2 itself has no dependency on `Shapely`, applications using
    functions of this module have to ensure that `Shapely` is available.
"""
from pkg_resources import parse_version

import shapely.wkb
import shapely.wkt

from .elements import WKBElement, WKTElement
from .compat import buffer, bytes, str

if parse_version(shapely.__version__) < parse_version("1.7"):
    ######################################################################
    # Backport function from Shapely 1.7
    from shapely.geos import WKBWriter, lgeos
    from shapely.geometry.base import geom_factory

    def dumps(ob, hex=False, srid=None, **kw):
        """Dump a WKB representation of a geometry to a byte string, or a
        hex-encoded string if ``hex=True``.

        Parameters
        ----------
        ob : geometry
            The geometry to export to well-known binary (WKB) representation
        hex : bool
            If true, export the WKB as a hexidecimal string. The default is to
            return a binary string/bytes object.
        srid : int
            Spatial reference system ID to include in the output. The default
            value means no SRID is included.
        **kw : kwargs
            See available keyword output settings in ``shapely.geos.WKBWriter``.
        """
        if srid is not None:
            # clone the object and set the SRID before dumping
            geom = lgeos.GEOSGeom_clone(ob._geom)
            lgeos.GEOSSetSRID(geom, srid)
            ob = geom_factory(geom)
            kw["include_srid"] = True
        writer = WKBWriter(lgeos, **kw)
        if hex:
            return writer.write_hex(ob)
        else:
            return writer.write(ob)
    ######################################################################
else:
    from shapely.wkb import dumps  # noqa


def to_shape(element):
    """
    Function to convert a :class:`geoalchemy2.types.SpatialElement`
    to a Shapely geometry.

    Example::

        lake = Session.query(Lake).get(1)
        polygon = to_shape(lake.geom)
    """
    assert isinstance(element, (WKBElement, WKTElement))
    if isinstance(element, WKBElement):
        data, hex = (element.data, True) if isinstance(element.data, str) else \
                    (bytes(element.data), False)
        return shapely.wkb.loads(data, hex=hex)
    elif isinstance(element, WKTElement):
        if element.extended:
            return shapely.wkt.loads(element.data.split(';', 1)[1])
        else:
            return shapely.wkt.loads(element.data)


def from_shape(shape, srid=-1, extended=False):
    """
    Function to convert a Shapely geometry to a
    :class:`geoalchemy2.types.WKBElement`.

    Additional arguments:

    ``srid``

        An integer representing the spatial reference system. E.g. 4326.
        Default value is -1, which means no/unknown reference system.

    ``extended``

        A boolean to switch between WKB and EWKB.
        Default value is False.

    Example::

        from shapely.geometry import Point
        wkb_element = from_shape(Point(5, 45), srid=4326)
        ewkb_element = from_shape(Point(5, 45), srid=4326, extended=True)
    """
    return WKBElement(
        buffer(dumps(shape, srid=srid if extended else None)),
        srid=srid,
        extended=extended)
