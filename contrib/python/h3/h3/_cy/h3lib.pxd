from libc cimport stdint
from cpython cimport bool
from libc.stdint cimport int64_t

ctypedef stdint.uint64_t H3int
ctypedef basestring H3str

cdef extern from "h3api.h":
    cdef int H3_VERSION_MAJOR
    cdef int H3_VERSION_MINOR
    cdef int H3_VERSION_PATCH

    ctypedef stdint.uint64_t H3Index

    ctypedef struct GeoCoord:
        double lat  # in radians
        double lng "lon"  # in radians

    ctypedef struct GeoBoundary:
        int num_verts "numVerts"
        GeoCoord verts[10]  # MAX_CELL_BNDRY_VERTS

    ctypedef struct Geofence:
        int numVerts
        GeoCoord *verts

    ctypedef struct GeoPolygon:
        Geofence geofence
        int numHoles
        Geofence *holes

    ctypedef struct GeoMultiPolygon:
        int numPolygons
        GeoPolygon *polygons

    ctypedef struct LinkedGeoCoord:
        GeoCoord data "vertex"
        LinkedGeoCoord *next

    # renaming these for clarity
    ctypedef struct LinkedGeoLoop:
        LinkedGeoCoord *data "first"
        LinkedGeoCoord *_data_last "last"  # not needed in Cython bindings
        LinkedGeoLoop *next

    ctypedef struct LinkedGeoPolygon:
        LinkedGeoLoop *data "first"
        LinkedGeoLoop *_data_last "last"  # not needed in Cython bindings
        LinkedGeoPolygon *next

    ctypedef struct CoordIJ:
        int i
        int j

    H3Index geoToH3(const GeoCoord *g, int res) nogil

    void h3ToGeo(H3Index h3, GeoCoord *g) nogil

    void h3ToGeoBoundary(H3Index h3, GeoBoundary *gp)

    int maxKringSize(int k)

    int hexRange(H3Index origin, int k, H3Index *out)

    int hexRangeDistances(H3Index origin, int k, H3Index *out, int *distances)

    int h3Distance(H3Index origin, H3Index h3)

    int hexRanges(H3Index *h3Set, int length, int k, H3Index *out)

    void kRing(H3Index origin, int k, H3Index *out)

    void kRingDistances(H3Index origin, int k, H3Index *out, int *distances)

    int hexRing(H3Index origin, int k, H3Index *out)

    int maxPolyfillSize(const GeoPolygon *geoPolygon, int res)

    void polyfill(const GeoPolygon *geoPolygon, int res, H3Index *out)

    void h3SetToLinkedGeo(const H3Index *h3Set, const int numHexes, LinkedGeoPolygon *out)

    void destroyLinkedPolygon(LinkedGeoPolygon *polygon)

    double degsToRads(double degrees) nogil

    double radsToDegs(double radians) nogil

    stdint.int64_t numHexagons(int res)

    int h3GetResolution(H3Index h) nogil

    int h3GetBaseCell(H3Index h)

    H3Index stringToH3(const char *str)

    void h3ToString(H3Index h, char *str, size_t sz)

    int h3IsValid(H3Index h)

    H3Index h3ToParent(H3Index h, int parentRes) nogil

    int maxH3ToChildrenSize(H3Index h, int childRes)

    void h3ToChildren(H3Index h, int childRes, H3Index *children)

    int compact(const H3Index *h3Set, H3Index *compactedSet, const int numHexes)

    int maxUncompactSize(const H3Index *compactedSet, const int numHexes, const int res)

    int uncompact(const H3Index *compactedSet, const int numHexes, H3Index *h3Set, const int maxHexes, const int res)

    int h3IsResClassIII(H3Index h)

    int h3IsPentagon(H3Index h)

    int pentagonIndexCount()

    void getPentagonIndexes(int res, H3Index *out)

    int res0IndexCount()

    void getRes0Indexes(H3Index *out)

    H3Index h3ToCenterChild(H3Index h, int res)

    int h3IndexesAreNeighbors(H3Index origin, H3Index destination)

    H3Index getH3UnidirectionalEdge(H3Index origin, H3Index destination)

    int h3UnidirectionalEdgeIsValid(H3Index edge)

    H3Index getOriginH3IndexFromUnidirectionalEdge(H3Index edge)

    H3Index getDestinationH3IndexFromUnidirectionalEdge(H3Index edge)

    void getH3IndexesFromUnidirectionalEdge(H3Index edge, H3Index *originDestination)

    void getH3UnidirectionalEdgesFromHexagon(H3Index origin, H3Index *edges)

    void getH3UnidirectionalEdgeBoundary(H3Index edge, GeoBoundary *gb)

    int h3LineSize(H3Index start, H3Index end)
    int h3Line(H3Index start, H3Index end, H3Index *out)

    int maxFaceCount(H3Index h3)
    void h3GetFaces(H3Index h3, int *out)

    int experimentalH3ToLocalIj(H3Index origin, H3Index h3, CoordIJ *out)
    int experimentalLocalIjToH3(H3Index origin, const CoordIJ *ij, H3Index *out)

    double hexAreaKm2(int res) nogil
    double hexAreaM2(int res) nogil

    double cellAreaRads2(H3Index h) nogil
    double cellAreaKm2(H3Index h) nogil
    double cellAreaM2(H3Index h) nogil

    double edgeLengthKm(int res) nogil
    double edgeLengthM(int res) nogil

    double exactEdgeLengthRads(H3Index edge) nogil
    double exactEdgeLengthKm(H3Index edge) nogil
    double exactEdgeLengthM(H3Index edge) nogil

    double pointDistRads(const GeoCoord *a, const GeoCoord *b) nogil
    double pointDistKm(const GeoCoord *a, const GeoCoord *b) nogil
    double pointDistM(const GeoCoord *a, const GeoCoord *b) nogil
