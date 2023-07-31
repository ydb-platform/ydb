#include "polygon.h"

#include <util/generic/algorithm.h>

using namespace NReverseGeocoder;

static bool Check(const TPart* part, const TPoint& point, const TRef* edgeRefs,
                  const TEdge* edges, const TPoint* points) {
    const TNumber edgeRefsNumber = (part + 1)->EdgeRefsOffset - part->EdgeRefsOffset;
    return part->Contains(point, edgeRefsNumber, edgeRefs, edges, points);
}

bool NReverseGeocoder::TPolygon::Contains(const TPoint& point, const TPart* parts, const TRef* edgeRefs,
                                          const TEdge* edges, const TPoint* points) const {
    if (!Bbox.Contains(point))
        return false;

    parts += PartsOffset;
    const TPart* partsEnd = parts + PartsNumber;

    // Find lower bound part, which can contains given point.
    const TPart* part = LowerBound(parts, partsEnd, point, [&](const TPart& a, const TPoint& b) {
        return a.Coordinate < b.X;
    });

    if (part->Coordinate > point.X) {
        if (part == parts)
            return false;
        --part;
    }

    if (point.X < part->Coordinate || point.X > (part + 1)->Coordinate)
        return false;

    if (point.X == part->Coordinate)
        if (part != parts && Check(part - 1, point, edgeRefs, edges, points))
            return true;

    return Check(part, point, edgeRefs, edges, points);
}

bool NReverseGeocoder::TPolygonBase::Better(const TPolygonBase& p, const TRegion* regions,
                                            TNumber regionsNumber) const {
    if (Square < p.Square)
        return true;

    if (Square == p.Square) {
        const TRegion* begin = regions;
        const TRegion* end = regions + regionsNumber;

        const TRegion* r1 = LowerBound(begin, end, TGeoId(RegionId));
        const TRegion* r2 = LowerBound(begin, end, TGeoId(p.RegionId));

        if (r1 == end || r1->RegionId != RegionId)
            return false;

        if (r2 == end || r2->RegionId != p.RegionId)
            return false;

        return r1->Better(*r2);
    }

    return false;
}

bool NReverseGeocoder::TRawPolygon::Contains(const TPoint& point, const TRef* edgeRefs, const TEdge* edges,
                                             const TPoint* points) const {
    if (!Bbox.Contains(point))
        return false;

    edgeRefs += EdgeRefsOffset;

    TNumber intersections = 0;
    for (TNumber i = 0; i < EdgeRefsNumber; ++i) {
        const TEdge& e = edges[edgeRefs[i]];

        if (e.Contains(point, points))
            return true;

        TPoint a = points[e.Beg];
        TPoint b = points[e.End];

        if (a.X > b.X)
            DoSwap(a, b);

        if (a.X < point.X && b.X >= point.X && e.Lower(point, points))
            ++intersections;
    }

    return intersections % 2 == 1;
}
