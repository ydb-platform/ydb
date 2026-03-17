#pragma once

#include <contrib/libs/h3/h3lib/include/h3api.h>

// TODO(dakovalkov): eliminate it.
#define lng lon

using LatLng = GeoCoord;
using CellBoundary = GeoBoundary;
using H3Error = uint32_t;

enum H3ErrorCodes
{
    E_SUCCESS = 0,
    E_FAILED = 1,
};

#define H3_NULL 0

inline H3Error latLngToCell(const LatLng *g, int res, H3Index *out)
{
    *out = geoToH3(g, res);
    return *out == H3_NULL ? E_FAILED : E_SUCCESS;
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error getHexagonEdgeLengthAvgM(int res, double *out)
// But CH uses a random commit from H3 master branch which is incompatible with both 3.x and 4.x.
inline double getHexagonEdgeLengthAvgM(int res)
{
    return edgeLengthM(res);
}

inline int getBaseCellNumber(H3Index h)
{
    return h3GetBaseCell(h);
}

inline int getResolution(H3Index h)
{
    return h3GetResolution(h);
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error getHexagonAreaAvgM2(int res, double *out)
inline double getHexagonAreaAvgM2(int res)
{
    return hexAreaM2(res);
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error areNeighborCells(H3Index origin, H3Index destination, int *out)
inline int areNeighborCells(H3Index origin, H3Index destination)
{
    return h3IndexesAreNeighbors(origin, destination);
}

inline int isValidCell(H3Index h)
{
    return h3IsValid(h);
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error cellToChildrenSize(H3Index cell, int childRes, int64_t *out)
inline int64_t cellToChildrenSize(H3Index cell, int childRes)
{
    return maxH3ToChildrenSize(cell, childRes);
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error cellToChildren(H3Index cell, int childRes, H3Index *children)
inline H3Error cellToChildren(H3Index cell, int childRes, H3Index *children)
{
    h3ToChildren(cell, childRes, children);
    return *children == H3_NULL ? E_FAILED : E_SUCCESS;
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error cellToParent(H3Index cell, int parentRes, H3Index *parent)
inline H3Index cellToParent(H3Index cell, int parentRes)
{
    return h3ToParent(cell, parentRes);
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error maxGridDiskSize(int k, int64_t *out)
inline int64_t maxGridDiskSize(int k) {
    return maxKringSize(k);
}


inline H3Error gridDisk(H3Index origin, int k, H3Index* out)
{
    kRing(origin, k, out);
    return *out == H3_NULL ? E_FAILED : E_SUCCESS;
}

inline H3Error cellToLatLng(H3Index cell, LatLng *g)
{
    h3ToGeo(cell, g);
    // NOTE(dakovalkov): There is no way to get the error in 3.x (is it even possible?).
    return E_SUCCESS;
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error cellsToDirectedEdge(H3Index origin, H3Index destination, H3Index *out)
inline H3Index cellsToDirectedEdge(H3Index origin, H3Index destination)
{
    return getH3UnidirectionalEdge(origin, destination);
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error gridPathCellsSize(H3Index start, H3Index end, int64_t* size);
inline int64_t gridPathCellsSize(H3Index start, H3Index end)
{
    return h3LineSize(start, end);
}

inline H3Error cellToBoundary(H3Index cell, CellBoundary *bndry)
{
    h3ToGeoBoundary(cell, bndry);
    // NOTE(dakovalkov): There is no way to get the error in 3.x (is it even possible?).
    return E_SUCCESS;
}

inline int isResClassIII(H3Index h)
{
    return h3IsResClassIII(h);
}

inline int isPentagon(H3Index h)
{
    return h3IsPentagon(h);
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error getHexagonEdgeLengthAvgKm(int res, double *out)
inline double getHexagonEdgeLengthAvgKm(int res)
{
    return edgeLengthKm(res);
}

inline H3Error getIcosahedronFaces(H3Index h, int* out)
{
    h3GetFaces(h, out);
    // NOTE(dakovalkov): There is no way to get the error in 3.x (is the error even possible?).
    return E_SUCCESS;
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error getHexagonAreaAvgKm2(int res, double *out)
inline double getHexagonAreaAvgKm2(int res)
{
    return hexAreaKm2(res);
}

inline H3Error gridPathCells(H3Index start, H3Index end, H3Index* out)
{
    return h3Line(start, end, out);
}

inline int isValidDirectedEdge(H3Index edge)
{
    return h3UnidirectionalEdgeIsValid(edge);
}

inline H3Error originToDirectedEdges(H3Index origin, H3Index* edges)
{
    // TODO(dakovalkov): Find out how to implement it via 3.x version.
    throw "Not implemented";
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error getDirectedEdgeDestination(H3Index edge, H3Index *out);
inline H3Index getDirectedEdgeDestination(H3Index edge)
{
    return getDestinationH3IndexFromUnidirectionalEdge(edge);
}

inline int res0CellCount()
{
    return res0IndexCount();
}

inline H3Error getRes0Cells(H3Index *out)
{
    getRes0Indexes(out);
    // NOTE(dakovalkov): There is no way to get the error in 3.x.
    return E_SUCCESS;
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error getNumCells(int res, int64_t *out);
inline int64_t getNumCells(int res)
{
    return numHexagons(res);
}

inline H3Error directedEdgeToBoundary(H3Index edge, CellBoundary* gb)
{
    getH3UnidirectionalEdgeBoundary(edge, gb);
    // NOTE(dakovalkov): There is no way to get the error in 3.x.
    return E_SUCCESS;
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error cellToCenterChild(H3Index cell, int childRes, H3Index *child);
inline H3Index cellToCenterChild(H3Index cell, int childRes)
{
    return h3ToCenterChild(cell, childRes);
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// double greatCircleDistanceKm(const LatLng *a, const LatLng *b)
inline double distanceKm(const LatLng *a, const LatLng *b)
{
    return pointDistKm(a, b);
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// double greatCircleDistanceM(const LatLng *a, const LatLng *b)
inline double distanceM(const LatLng *a, const LatLng *b)
{
    return pointDistM(a, b);
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// double greatCircleDistanceRads(const LatLng *a, const LatLng *b)
inline double distanceRads(const LatLng *a, const LatLng *b)
{
    return pointDistRads(a, b);
}

inline H3Error directedEdgeToCells(H3Index edge, H3Index* originDestination)
{
    getH3IndexesFromUnidirectionalEdge(edge, originDestination);
    return *originDestination == H3_NULL ? E_FAILED : E_SUCCESS;
}

// NOTE(dakovalkov): The real function signature in H3 4.x is
// H3Error getDirectedEdgeOrigin(H3Index edge, H3Index *out);
inline H3Index getDirectedEdgeOrigin(H3Index edge)
{
    return getOriginH3IndexFromUnidirectionalEdge(edge);
}

inline int pentagonCount()
{
    return pentagonIndexCount();
}

inline H3Error getPentagons(int res, H3Index *out)
{
    getPentagonIndexes(res, out);
    // NOTE(dakovalkov): There is no way to get the error in 3.x.
    return E_SUCCESS;
}

inline H3Error gridRingUnsafe(H3Index origin, int k, H3Index* out)
{
    return hexRing(origin, k, out);
}
