"""This module defines specific functions for Postgresql dialect."""

from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, WKTElement):
        if bindvalue.extended:
            return "%s" % (bindvalue.data)
        else:
            return "SRID=%d;%s" % (bindvalue.srid, bindvalue.data)
    elif isinstance(bindvalue, WKBElement):
        if not bindvalue.extended:
            # When the WKBElement includes a WKB value rather
            # than a EWKB value we use Shapely to convert the WKBElement to an
            # EWKT string
            shape = to_shape(bindvalue)
            return "SRID=%d;%s" % (bindvalue.srid, shape.wkt)
        else:
            # PostGIS ST_GeomFromEWKT works with EWKT strings as well
            # as EWKB hex strings
            return bindvalue.desc
    elif isinstance(bindvalue, RasterElement):
        return "%s" % (bindvalue.data)
    else:
        return bindvalue
