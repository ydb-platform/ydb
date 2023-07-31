#pragma once

#include "bbox.h"
#include "common.h"
#include "edge.h"
#include "part.h"
#include "point.h"
#include "region.h"

namespace NReverseGeocoder {
#pragma pack(push, 1)

    struct TPolygonBase {
        enum EType {
            TYPE_UNKNOWN = 0,
            TYPE_INNER = 1,
            TYPE_OUTER = 2,
        };

        // If TYPE_INNER and polygon contains given point, this means that region with RegionId
        // does not contains point.
        EType Type;

        ui32 Unused1;

        // Geographical data indetifiers.
        TGeoId RegionId;
        TGeoId PolygonId;

        // Rectangle in which lies that polygon.
        TBoundingBox Bbox;

        // Square of polygon. Need for determine which polygon is better. See better member function.
        TSquare Square;

        // Total points number of given polygon.
        TNumber PointsNumber;

        // Check that this polygon better then given polygon, which means that this polygons lying
        // deeper then given in polygons hierarchy.
        bool Better(const TPolygonBase& p, const TRegion* regions, TNumber regionsNumber) const;
    };

    // Polygon is a representation of persistent scanline data structure.
    struct TPolygon: public TPolygonBase {
        // Versions of persistent scanline.
        TNumber PartsOffset;
        TNumber PartsNumber;
        ui32 Unused2;

        // Fast point in polygon test using persistent scanline. You can see how this data structure
        // generated in generator/.
        bool Contains(const TPoint& point, const TPart* parts, const TRef* edgeRefs,
                      const TEdge* edges, const TPoint* points) const;
    };

    static_assert(sizeof(TPolygon) == 64, "NReverseGeocoder::TPolygon size mismatch");

    // Raw polygon is a polygon representation for slow tests.
    struct TRawPolygon: public TPolygonBase {
        // Raw polygon edge refs.
        TNumber EdgeRefsOffset;
        TNumber EdgeRefsNumber;
        ui32 Unused2;

        bool Contains(const TPoint& point, const TRef* edgeRefs, const TEdge* edges,
                      const TPoint* points) const;
    };

    static_assert(sizeof(TRawPolygon) == 64, "NReverseGeocoder::TRawPolygon size mismatch");

#pragma pack(pop)
}
