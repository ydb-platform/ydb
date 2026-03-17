"""This module defines specific functions for SQLite dialect."""

import re
import warnings

from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape


def format_geom_type(wkt, default_srid=None):
    """Format the Geometry type for SQLite."""
    match = re.match(WKTElement.SPLIT_WKT_PATTERN, wkt)
    if match is None:
        warnings.warn(
            "The given WKT could not be parsed by GeoAlchemy2, this could lead to undefined "
            f"behavior with Z, M or ZM geometries or with incorrect SRID. The WKT string is: {wkt}"
        )
        return wkt
    _, srid, geom_type, coords = match.groups()
    geom_type = geom_type.replace(" ", "")
    if geom_type.endswith("ZM"):
        geom_type = geom_type[:-2]
    elif geom_type.endswith("Z"):
        geom_type = geom_type[:-1]
    if srid is None and default_srid is not None:
        srid = f"SRID={default_srid}"
    if srid is not None:
        return "%s;%s%s" % (srid, geom_type, coords)
    else:
        return "%s%s" % (geom_type, coords)


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, WKTElement):
        return format_geom_type(
            bindvalue.data,
            default_srid=bindvalue.srid if bindvalue.srid >= 0 else spatial_type.srid,
        )
    elif isinstance(bindvalue, WKBElement):
        # With SpatiaLite we use Shapely to convert the WKBElement to an EWKT string
        shape = to_shape(bindvalue)
        # shapely.wkb.loads returns geom_type with a 'Z', for example, 'LINESTRING Z'
        # which is a limitation with SpatiaLite. Hence, a temporary fix.
        res = format_geom_type(
            shape.wkt, default_srid=bindvalue.srid if bindvalue.srid >= 0 else spatial_type.srid
        )
        return res
    elif isinstance(bindvalue, RasterElement):
        return "%s" % (bindvalue.data)
    elif isinstance(bindvalue, str):
        return format_geom_type(bindvalue, default_srid=spatial_type.srid)
    else:
        return bindvalue
