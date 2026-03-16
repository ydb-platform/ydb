include "gdal.pxi"


cdef class GeomBuilder:

    cdef OGRGeometryH geom
    cdef object code
    cdef object geomtypename
    cdef object ndims
    cdef _buildCoords(self, OGRGeometryH geom)
    cpdef _buildPoint(self)
    cpdef _buildLineString(self)
    cpdef _buildLinearRing(self)
    cdef _buildParts(self, OGRGeometryH geom)
    cpdef _buildPolygon(self)
    cpdef _buildMultiPoint(self)
    cpdef _buildMultiLineString(self)
    cpdef _buildMultiPolygon(self)
    cpdef _buildGeometryCollection(self)
    cdef build(self, OGRGeometryH geom)


cdef class OGRGeomBuilder:

    cdef OGRGeometryH _createOgrGeometry(self, int geom_type) except NULL
    cdef _addPointToGeometry(self, OGRGeometryH geom, object coordinate)
    cdef OGRGeometryH _buildPoint(self, object coordinates) except NULL
    cdef OGRGeometryH _buildLineString(self, object coordinates) except NULL
    cdef OGRGeometryH _buildLinearRing(self, object coordinates) except NULL
    cdef OGRGeometryH _buildPolygon(self, object coordinates) except NULL
    cdef OGRGeometryH _buildMultiPoint(self, object coordinates) except NULL
    cdef OGRGeometryH _buildMultiLineString(self, object coordinates) except NULL
    cdef OGRGeometryH _buildMultiPolygon(self, object coordinates) except NULL
    cdef OGRGeometryH _buildGeometryCollection(self, object geoms) except NULL
    cdef OGRGeometryH build(self, object geom) except NULL


cdef class ShapeIterator:

    cdef OGRLayerH layer
    cdef int fieldtype
