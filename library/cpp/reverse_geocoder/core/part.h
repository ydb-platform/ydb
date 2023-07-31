#pragma once

#include "common.h"
#include "edge.h"
#include "point.h"

namespace NReverseGeocoder {
    // TPart contains version of persistent scanline. Parts lying in geofraphical data parts array,
    // ordered by Coordinate for each polygon. Variable EdgeRefsOffset refers on EdgeRefs array for
    // this part. For optimal usage of memory, part does not contain "EdgeRefsNumber" variable, because
    // it's can be computed as parts[i + 1].EdgeRefsOffset - parts[i].EdgeRefsOffset for every part
    // in geographical data. Especially for this, added fake part into IGeoData with correct
    // EdgeRefsOffset. Refs in EdgeRefs are in increasing order for each part. It is necessary to
    // quickly determine how many edges is under the point. See generator/ for details.
    struct Y_PACKED TPart {
        TCoordinate Coordinate;
        TNumber EdgeRefsOffset;

        // Checks point lying under odd numbers of edges or on edge.
        bool Contains(const TPoint& point, TNumber edgeRefsNumber, const TRef* edgeRefs,
                      const TEdge* edges, const TPoint* points) const;
    };

    static_assert(sizeof(TPart) == 8, "NReverseGeocoder::TPart size mismatch");

}
