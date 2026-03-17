/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#pragma once

#include <geos/export.h>

#include <vector>

#include <geos/algorithm/LineIntersector.h> // for composition
#include <geos/geom/Coordinate.h> // for use in vector
#include <geos/geom/PrecisionModel.h> // for inlines (should drop)
#include <geos/noding/SegmentIntersector.h>


// Forward declarations
namespace geos {
namespace geom {
class PrecisionModel;
}
namespace noding {
class SegmentString;
class NodedSegmentString;
namespace snap {
class SnappingPointIndex;
}
}
}

namespace geos {
namespace noding { // geos::noding
namespace snap { // geos::noding::snap

class GEOS_DLL SnappingIntersectionAdder: public SegmentIntersector { // implements SegmentIntersector

private:

    algorithm::LineIntersector li;
    double snapTolerance;
    SnappingPointIndex& snapPointIndex;

    /**
    * If an endpoint of one segment is near
    * the <i>interior</i> of the other segment, add it as an intersection.
    * EXCEPT if the endpoint is also close to a segment endpoint
    * (since this can introduce "zigs" in the linework).
    * <p>
    * This resolves situations where
    * a segment A endpoint is extremely close to another segment B,
    * but is not quite crossing.  Due to robustness issues
    * in orientation detection, this can
    * result in the snapped segment A crossing segment B
    * without a node being introduced.
    */
    void processNearVertex(
        SegmentString* srcSS,
        size_t srcIndex,
        const geom::Coordinate& p,
        SegmentString* ss,
        size_t segIndex,
        const geom::Coordinate& p0,
        const geom::Coordinate& p1);

    /**
    * Tests if segments are adjacent on the same SegmentString.
    * Closed segStrings require a check for the point shared by the beginning
    * and end segments.
    */
    static bool isAdjacent(SegmentString* ss0, size_t segIndex0, SegmentString* ss1, size_t segIndex1);


public:

    SnappingIntersectionAdder(double p_snapTolerance, SnappingPointIndex& p_snapPointIndex);

    /**
    * This method is called by clients
    * of the {@link SegmentIntersector} class to process
    * intersections for two segments of the {@link SegmentString}s being intersected.
    * Note that some clients (such as <code>MonotoneChain</code>s) may optimize away
    * this call for segment pairs which they have determined do not intersect
    * (e.g. by an disjoint envelope test).
    */
    void processIntersections(SegmentString* e0, size_t segIndex0, SegmentString* e1, size_t segIndex1) override;

    bool isDone() const override { return false; };


};

} // namespace geos::noding::snapround
} // namespace geos::noding
} // namespace geos





