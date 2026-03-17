# Geometry API functions.

ctypedef int OGRErr


cdef extern from "ogr_core.h":
    ctypedef enum OGRwkbGeometryType:
        wkbUnknown
        wkbPoint
        wkbLineString
        wkbPolygon
        wkbMultiPoint
        wkbMultiLineString
        wkbMultiPolygon
        wkbGeometryCollection
        wkbCircularString
        wkbCompoundCurve
        wkbCurvePolygon
        wkbMultiCurve
        wkbMultiSurface
        wkbCurve
        wkbSurface
        wkbPolyhedralSurface
        wkbTIN
        wkbTriangle
        wkbNone
        wkbLinearRing
        wkbCircularStringZ
        wkbCompoundCurveZ
        wkbCurvePolygonZ
        wkbMultiCurveZ
        wkbMultiSurfaceZ
        wkbCurveZ
        wkbSurfaceZ
        wkbPolyhedralSurfaceZ
        wkbTINZ
        wkbTriangleZ
        wkbPointM
        wkbLineStringM
        wkbPolygonM
        wkbMultiPointM
        wkbMultiLineStringM
        wkbMultiPolygonM
        wkbGeometryCollectionM
        wkbCircularStringM
        wkbCompoundCurveM
        wkbCurvePolygonM
        wkbMultiCurveM
        wkbMultiSurfaceM
        wkbCurveM
        wkbSurfaceM
        wkbPolyhedralSurfaceM
        wkbTINM
        wkbTriangleM
        wkbPointZM
        wkbLineStringZM
        wkbPolygonZM
        wkbMultiPointZM
        wkbMultiLineStringZM
        wkbMultiPolygonZM
        wkbGeometryCollectionZM
        wkbCircularStringZM
        wkbCompoundCurveZM
        wkbCurvePolygonZM
        wkbMultiCurveZM
        wkbMultiSurfaceZM
        wkbCurveZM
        wkbSurfaceZM
        wkbPolyhedralSurfaceZM
        wkbTINZM
        wkbTriangleZM
        wkbPoint25D
        wkbLineString25D
        wkbPolygon25D
        wkbMultiPoint25D
        wkbMultiLineString25D
        wkbMultiPolygon25D
        wkbGeometryCollection25D


ctypedef struct OGREnvelope:
    double MinX
    double MaxX
    double MinY
    double MaxY


cdef extern from "ogr_api.h":
    OGRErr  OGR_G_AddGeometryDirectly (void *geometry, void *part)
    void    OGR_G_AddPoint (void *geometry, double x, double y, double z)
    void    OGR_G_AddPoint_2D (void *geometry, double x, double y)
    void    OGR_G_CloseRings (void *geometry)
    void *  OGR_G_CreateGeometry (OGRwkbGeometryType wkbtypecode)
    void    OGR_G_DestroyGeometry (void *geometry)
    unsigned char * OGR_G_ExportToJson (void *geometry)
    void    OGR_G_ExportToWkb (void *geometry, int endianness, char *buffer)
    int     OGR_G_GetCoordinateDimension (void *geometry)
    int     OGR_G_GetGeometryCount (void *geometry)
    unsigned char *  OGR_G_GetGeometryName (void *geometry)
    int     OGR_G_GetGeometryType (void *geometry)
    void *  OGR_G_GetGeometryRef (void *geometry, int n)
    int     OGR_G_GetPointCount (void *geometry)
    double  OGR_G_GetX (void *geometry, int n)
    double  OGR_G_GetY (void *geometry, int n)
    double  OGR_G_GetZ (void *geometry, int n)
    OGRErr  OGR_G_ImportFromWkb (void *geometry, unsigned char *bytes, int nbytes)
    int     OGR_G_WkbSize (void *geometry)


cdef class GeomBuilder:
    cdef object ndims
    cdef list _buildCoords(self, void *geom)
    cdef dict _buildPoint(self, void *geom)
    cdef dict _buildLineString(self, void *geom)
    cdef dict _buildLinearRing(self, void *geom)
    cdef list _buildParts(self, void *geom)
    cdef dict _buildPolygon(self, void *geom)
    cdef dict _buildMultiPoint(self, void *geom)
    cdef dict _buildMultiLineString(self, void *geom)
    cdef dict _buildMultiPolygon(self, void *geom)
    cdef dict _buildGeometryCollection(self, void *geom)
    cdef object build_from_feature(self, void *feature)
    cdef object build(self, void *geom)
    cpdef build_wkb(self, object wkb)


cdef class OGRGeomBuilder:
    cdef void * _createOgrGeometry(self, int geom_type) except NULL
    cdef _addPointToGeometry(self, void *cogr_geometry, object coordinate)
    cdef void * _buildPoint(self, object coordinates) except NULL
    cdef void * _buildLineString(self, object coordinates) except NULL
    cdef void * _buildLinearRing(self, object coordinates) except NULL
    cdef void * _buildPolygon(self, object coordinates) except NULL
    cdef void * _buildMultiPoint(self, object coordinates) except NULL
    cdef void * _buildMultiLineString(self, object coordinates) except NULL
    cdef void * _buildMultiPolygon(self, object coordinates) except NULL
    cdef void * _buildGeometryCollection(self, object coordinates) except NULL
    cdef void * build(self, object geom) except NULL


cdef unsigned int geometry_type_code(object name) except? 9999
cdef object normalize_geometry_type_code(unsigned int code)
cdef unsigned int base_geometry_type_code(unsigned int code)

