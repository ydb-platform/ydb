#include "part.h"

#include <library/cpp/reverse_geocoder/library/unaligned_iter.h>

#include <util/generic/algorithm.h>

using namespace NReverseGeocoder;

bool NReverseGeocoder::TPart::Contains(const TPoint& point, TNumber edgeRefsNumber, const TRef* edgeRefs,
                                       const TEdge* edges, const TPoint* points) const {
    auto edgeRefsBegin = UnalignedIter(edgeRefs) + EdgeRefsOffset;
    auto edgeRefsEnd = edgeRefsBegin + edgeRefsNumber;

    // Find lower bound edge, which lying below given point.
    auto cmp = [&](const TRef& e, const TPoint& p) {
        return edges[e].Lower(p, points);
    };

    auto edgeRef = LowerBound(edgeRefsBegin, edgeRefsEnd, point, cmp);

    if (edgeRef == edgeRefsEnd)
        return false;

    if (edges[*edgeRef].Contains(point, points))
        return true;

    // If the point is inside of the polygon then it will intersect the edge an odd number of times.
    return (edgeRef - edgeRefsBegin) % 2 == 1;
}
