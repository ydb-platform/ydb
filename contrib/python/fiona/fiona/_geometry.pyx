# Coordinate and geometry transformations.

include "gdal.pxi"

import logging

from fiona.errors import UnsupportedGeometryTypeError
from fiona.model import decode_object, GEOMETRY_TYPES, Geometry, OGRGeometryType
from fiona._err cimport exc_wrap_int


class NullHandler(logging.Handler):
    def emit(self, record):
        pass

log = logging.getLogger(__name__)
log.addHandler(NullHandler())


# mapping of GeoJSON type names to OGR integer geometry types
GEOJSON2OGR_GEOMETRY_TYPES = dict((v, k) for k, v in GEOMETRY_TYPES.items())

cdef set LINEAR_GEOM_TYPES = {
    OGRGeometryType.CircularString.value,
    OGRGeometryType.CompoundCurve.value,
    OGRGeometryType.CurvePolygon.value,
    OGRGeometryType.MultiCurve.value,
    OGRGeometryType.MultiSurface.value,
    # OGRGeometryType.Curve.value,  # Abstract type
    # OGRGeometryType.Surface.value,  # Abstract type
}

cdef set PS_TIN_Tri_TYPES = {
    OGRGeometryType.PolyhedralSurface.value,
    OGRGeometryType.TIN.value,
    OGRGeometryType.Triangle.value
}


cdef int ogr_get_geometry_type(void *geometry):
    # OGR_G_GetGeometryType with NULL geometry support
    if geometry == NULL:
        return 0 # unknown
    return OGR_G_GetGeometryType(geometry)


cdef unsigned int geometry_type_code(name) except? 9999:
    """Map OGC geometry type names to integer codes."""
    offset = 0
    if name.endswith('ZM'):
        offset = 3000
    elif name.endswith('M'):
        offset = 2000
    elif name.endswith('Z'):
        offset = 1000

    normalized_name = name.rstrip('ZM')
    if normalized_name not in GEOJSON2OGR_GEOMETRY_TYPES:
        raise UnsupportedGeometryTypeError(name)

    return offset + <unsigned int>GEOJSON2OGR_GEOMETRY_TYPES[normalized_name]


cdef object normalize_geometry_type_code(unsigned int code):
    """Normalize M geometry type codes."""
    # Normalize 'M' types to 2D types.
    if 2000 <= code < 3000:
        code = code % 1000
    elif code == 3000:
        code = 0
    # Normalize 'ZM' types to 3D types.
    elif 3000 < code < 4000:
        code = (code % 1000) | 0x80000000

    # Normalize to a linear type.
    code = OGR_GT_GetLinear(<OGRwkbGeometryType>code)

    if code not in GEOMETRY_TYPES:
        raise UnsupportedGeometryTypeError(code)

    return code


cdef inline unsigned int base_geometry_type_code(unsigned int code):
    """ Returns base geometry code without Z, M and ZM types """
    # Remove 2.5D flag.
    code = code & (~0x80000000)

    # Normalize Z, M, and ZM types. Fiona 1.x does not support M
    # and doesn't treat OGC 'Z' variants as special types of their
    # own.
    return code % 1000


# Geometry related functions and classes follow.
cdef void * _createOgrGeomFromWKB(object wkb) except NULL:
    """Make an OGR geometry from a WKB string"""
    wkbtype = bytearray(wkb)[1]
    cdef unsigned char *buffer = wkb
    cdef void *cogr_geometry = OGR_G_CreateGeometry(<OGRwkbGeometryType>wkbtype)
    if cogr_geometry is not NULL:
        exc_wrap_int(OGR_G_ImportFromWkb(cogr_geometry, buffer, len(wkb)))
    return cogr_geometry


cdef _deleteOgrGeom(void *cogr_geometry):
    """Delete an OGR geometry"""
    if cogr_geometry is not NULL:
        OGR_G_DestroyGeometry(cogr_geometry)
    cogr_geometry = NULL


cdef class GeomBuilder:
    """Builds Fiona (GeoJSON) geometries from an OGR geometry handle.
    """

    # Note: The geometry passed to OGR_G_ForceToPolygon and
    # OGR_G_ForceToMultiPolygon must be removed from the container /
    # feature beforehand and the returned geometry needs to be cleaned up
    # afterwards.
    # OGR_G_GetLinearGeometry returns a copy of the geometry that needs
    # to be cleaned up afterwards.

    cdef list _buildCoords(self, void *geom):
        # Build a coordinate sequence
        cdef int i
        if geom == NULL:
            raise ValueError("Null geom")
        npoints = OGR_G_GetPointCount(geom)
        coords = []
        for i in range(npoints):
            values = [OGR_G_GetX(geom, i), OGR_G_GetY(geom, i)]
            if self.ndims > 2:
                values.append(OGR_G_GetZ(geom, i))
            coords.append(tuple(values))
        return coords

    cdef dict  _buildPoint(self, void *geom):
        return {'type': 'Point', 'coordinates': self._buildCoords(geom)[0]}

    cdef dict _buildLineString(self, void *geom):
        return {'type': 'LineString', 'coordinates': self._buildCoords(geom)}

    cdef dict _buildLinearRing(self, void *geom):
        return {'type': 'LinearRing', 'coordinates': self._buildCoords(geom)}

    cdef list _buildParts(self, void *geom):
        cdef int j
        cdef int code
        cdef int count
        cdef void *part
        if geom == NULL:
            raise ValueError("Null geom")
        parts = []
        j = 0
        count = OGR_G_GetGeometryCount(geom)

        while j < count:
            part = OGR_G_GetGeometryRef(geom, j)
            code = base_geometry_type_code(OGR_G_GetGeometryType(part))
            if code in PS_TIN_Tri_TYPES:
                OGR_G_RemoveGeometry(geom, j, False)
                # Removing a geometry will cause the geometry count to drop by one,
                # and all “higher” geometries will shuffle down one in index.
                count -= 1
                parts.append(GeomBuilder().build(part))
            else:
                parts.append(GeomBuilder().build(part))
                j += 1
        return parts

    cdef dict _buildPolygon(self, void *geom):
        coordinates = [p['coordinates'] for p in self._buildParts(geom)]
        return {'type': 'Polygon', 'coordinates': coordinates}

    cdef dict _buildMultiPoint(self, void *geom):
        coordinates = [p['coordinates'] for p in self._buildParts(geom)]
        return {'type': 'MultiPoint', 'coordinates': coordinates}

    cdef dict _buildMultiLineString(self, void *geom):
        coordinates = [p['coordinates'] for p in self._buildParts(geom)]
        return {'type': 'MultiLineString', 'coordinates': coordinates}

    cdef dict _buildMultiPolygon(self, void *geom):
        coordinates = [p['coordinates'] for p in self._buildParts(geom)]
        return {'type': 'MultiPolygon', 'coordinates': coordinates}

    cdef dict _buildGeometryCollection(self, void *geom):
        parts = self._buildParts(geom)
        return {'type': 'GeometryCollection', 'geometries': parts}


    cdef object build_from_feature(self, void *feature):
        # Build Geometry from *OGRFeatureH
        cdef void *cogr_geometry = NULL
        cdef int code

        cogr_geometry = OGR_F_GetGeometryRef(feature)
        code = base_geometry_type_code(ogr_get_geometry_type(cogr_geometry))

        # We need to take ownership of the geometry before we can call 
        # OGR_G_ForceToPolygon or OGR_G_ForceToMultiPolygon
        if code in PS_TIN_Tri_TYPES:
            cogr_geometry = OGR_F_StealGeometry(feature)
        return self.build(cogr_geometry)

    cdef object build(self, void *geom):
        # Build Geometry from *OGRGeometryH

        cdef void *geometry_to_dealloc = NULL

        if geom == NULL:
            return None

        code = base_geometry_type_code(ogr_get_geometry_type(geom))

        # We convert special geometries (Curves, TIN, Triangle, ...)
        # to GeoJSON compatible geometries (LineStrings, Polygons, MultiPolygon, ...)
        if code in LINEAR_GEOM_TYPES:
            geometry_to_dealloc = OGR_G_GetLinearGeometry(geom, 0.0, NULL)
            code = base_geometry_type_code(ogr_get_geometry_type(geometry_to_dealloc))
            geom = geometry_to_dealloc
        elif code in PS_TIN_Tri_TYPES:
            if code == OGRGeometryType.Triangle.value:
                geometry_to_dealloc = OGR_G_ForceToPolygon(geom)
            else:
                geometry_to_dealloc = OGR_G_ForceToMultiPolygon(geom)
            code = base_geometry_type_code(OGR_G_GetGeometryType(geometry_to_dealloc))
            geom = geometry_to_dealloc
        self.ndims = OGR_G_GetCoordinateDimension(geom)

        if code not in GEOMETRY_TYPES:
            raise UnsupportedGeometryTypeError(code)

        geomtypename = GEOMETRY_TYPES[code]
        if geomtypename == "Point":
            built = self._buildPoint(geom)
        elif geomtypename == "LineString":
            built = self._buildLineString(geom)
        elif geomtypename == "LinearRing":
            built = self._buildLinearRing(geom)
        elif geomtypename == "Polygon":
            built = self._buildPolygon(geom)
        elif geomtypename == "MultiPoint":
            built = self._buildMultiPoint(geom)
        elif geomtypename == "MultiLineString":
            built = self._buildMultiLineString(geom)
        elif geomtypename == "MultiPolygon":
            built = self._buildMultiPolygon(geom)
        elif geomtypename == "GeometryCollection":
            built = self._buildGeometryCollection(geom)
        else:
            raise UnsupportedGeometryTypeError(code)

        # Cleanup geometries we have ownership over
        if geometry_to_dealloc is not NULL:
           OGR_G_DestroyGeometry(geometry_to_dealloc)

        return Geometry.from_dict(built)

    cpdef build_wkb(self, object wkb):
        # Build geometry from wkb
        cdef object data = wkb
        cdef void *cogr_geometry = _createOgrGeomFromWKB(data)
        result = self.build(cogr_geometry)
        _deleteOgrGeom(cogr_geometry)
        return result


cdef class OGRGeomBuilder:
    """Builds OGR geometries from Fiona geometries.
    """
    cdef void * _createOgrGeometry(self, int geom_type) except NULL:
        cdef void *cogr_geometry = OGR_G_CreateGeometry(<OGRwkbGeometryType>geom_type)
        if cogr_geometry == NULL:
            raise Exception("Could not create OGR Geometry of type: %i" % geom_type)
        return cogr_geometry

    cdef _addPointToGeometry(self, void *cogr_geometry, object coordinate):
        if len(coordinate) == 2:
            x, y = coordinate
            OGR_G_AddPoint_2D(cogr_geometry, x, y)
        else:
            x, y, z = coordinate[:3]
            OGR_G_AddPoint(cogr_geometry, x, y, z)

    cdef void * _buildPoint(self, object coordinates) except NULL:
        cdef void *cogr_geometry = self._createOgrGeometry(GEOJSON2OGR_GEOMETRY_TYPES['Point'])
        self._addPointToGeometry(cogr_geometry, coordinates)
        return cogr_geometry

    cdef void * _buildLineString(self, object coordinates) except NULL:
        cdef void *cogr_geometry = self._createOgrGeometry(GEOJSON2OGR_GEOMETRY_TYPES['LineString'])
        for coordinate in coordinates:
            self._addPointToGeometry(cogr_geometry, coordinate)
        return cogr_geometry

    cdef void * _buildLinearRing(self, object coordinates) except NULL:
        cdef void *cogr_geometry = self._createOgrGeometry(GEOJSON2OGR_GEOMETRY_TYPES['LinearRing'])
        for coordinate in coordinates:
            self._addPointToGeometry(cogr_geometry, coordinate)
        OGR_G_CloseRings(cogr_geometry)
        return cogr_geometry

    cdef void * _buildPolygon(self, object coordinates) except NULL:
        cdef void *cogr_ring
        cdef void *cogr_geometry = self._createOgrGeometry(GEOJSON2OGR_GEOMETRY_TYPES['Polygon'])
        for ring in coordinates:
            cogr_ring = self._buildLinearRing(ring)
            exc_wrap_int(OGR_G_AddGeometryDirectly(cogr_geometry, cogr_ring))
        return cogr_geometry

    cdef void * _buildMultiPoint(self, object coordinates) except NULL:
        cdef void *cogr_part
        cdef void *cogr_geometry = self._createOgrGeometry(GEOJSON2OGR_GEOMETRY_TYPES['MultiPoint'])
        for coordinate in coordinates:
            cogr_part = self._buildPoint(coordinate)
            exc_wrap_int(OGR_G_AddGeometryDirectly(cogr_geometry, cogr_part))
        return cogr_geometry

    cdef void * _buildMultiLineString(self, object coordinates) except NULL:
        cdef void *cogr_part
        cdef void *cogr_geometry = self._createOgrGeometry(GEOJSON2OGR_GEOMETRY_TYPES['MultiLineString'])
        for line in coordinates:
            cogr_part = self._buildLineString(line)
            exc_wrap_int(OGR_G_AddGeometryDirectly(cogr_geometry, cogr_part))
        return cogr_geometry

    cdef void * _buildMultiPolygon(self, object coordinates) except NULL:
        cdef void *cogr_part
        cdef void *cogr_geometry = self._createOgrGeometry(GEOJSON2OGR_GEOMETRY_TYPES['MultiPolygon'])
        for part in coordinates:
            cogr_part = self._buildPolygon(part)
            exc_wrap_int(OGR_G_AddGeometryDirectly(cogr_geometry, cogr_part))
        return cogr_geometry

    cdef void * _buildGeometryCollection(self, object geometries) except NULL:
        cdef void *cogr_part
        cdef void *cogr_geometry = self._createOgrGeometry(GEOJSON2OGR_GEOMETRY_TYPES['GeometryCollection'])
        for part in geometries:
            cogr_part = OGRGeomBuilder().build(part)
            exc_wrap_int(OGR_G_AddGeometryDirectly(cogr_geometry, cogr_part))
        return cogr_geometry

    cdef void * build(self, object geometry) except NULL:
        cdef object typename = geometry.type
        cdef object coordinates = geometry.coordinates
        cdef object geometries = geometry.geometries

        if typename == 'Point':
            return self._buildPoint(coordinates)
        elif typename == 'LineString':
            return self._buildLineString(coordinates)
        elif typename == 'LinearRing':
            return self._buildLinearRing(coordinates)
        elif typename == 'Polygon':
            return self._buildPolygon(coordinates)
        elif typename == 'MultiPoint':
            return self._buildMultiPoint(coordinates)
        elif typename == 'MultiLineString':
            return self._buildMultiLineString(coordinates)
        elif typename == 'MultiPolygon':
            return self._buildMultiPolygon(coordinates)
        elif typename == 'GeometryCollection':
            return self._buildGeometryCollection(geometries)
        else:
            raise UnsupportedGeometryTypeError("Unsupported geometry type %s" % typename)


def geometryRT(geom):
    # For testing purposes only, leaks the JSON data
    geometry = decode_object(geom)
    cdef void *cogr_geometry = OGRGeomBuilder().build(geometry)
    result = GeomBuilder().build(cogr_geometry)
    _deleteOgrGeom(cogr_geometry)
    return result
