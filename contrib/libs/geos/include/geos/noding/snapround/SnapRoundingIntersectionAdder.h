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
#include <memory>

#include <geos/inline.h>

#include <geos/noding/Noder.h> // for inheritance
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
namespace snapround {
class HotPixel;
}
}
}

namespace geos {
namespace noding { // geos::noding
namespace snapround { // geos::noding::snapround

/**
 * Finds intersections between line segments which will be snap-rounded,
 * and adds them as nodes to the segments.
 *
 * Intersections are detected and computed using full precision.
 * Snapping takes place in a subsequent phase.
 *
 * The intersection points are recorded, so that HotPixels can be created for them.
 *
 * To avoid robustness issues with vertices which lie very close to line segments
 * a heuristic is used:
 * nodes are created if a vertex lies within a tolerance distance
 * of the interior of a segment.
 * The tolerance distance is chosen to be significantly below the snap-rounding grid size.
 * This has empirically proven to eliminate noding failures.
 */
class GEOS_DLL SnapRoundingIntersectionAdder: public SegmentIntersector { // implements SegmentIntersector

private:
    /**
    * The division factor used to determine
    * nearness distance tolerance for interior intersection detection.
    */
    static constexpr int NEARNESS_FACTOR = 100;

    algorithm::LineIntersector li;
    std::unique_ptr<std::vector<geom::Coordinate>> intersections;
    // const geom::PrecisionModel* pm;
    double nearnessTol;

    /**
    * If an endpoint of one segment is near
    * the interior of the other segment, add it as an intersection.
    * EXCEPT if the endpoint is also close to a segment endpoint
    * (since this can introduce "zigs" in the linework).
    *
    * This resolves situations where
    * a segment A endpoint is extremely close to another segment B,
    * but is not quite crossing.  Due to robustness issues
    * in orientation detection, this can
    * result in the snapped segment A crossing segment B
    * without a node being introduced.
    */
    void processNearVertex(const geom::Coordinate& p, SegmentString* edge, size_t segIndex,
                           const geom::Coordinate& p0, const geom::Coordinate& p1);


public:

    SnapRoundingIntersectionAdder(const geom::PrecisionModel* newPm);

    std::unique_ptr<std::vector<geom::Coordinate>> getIntersections() { return std::move(intersections); };

    /**
    * This method is called by clients
    * of the {@link SegmentIntersector} class to process
    * intersections for two segments of the {@link SegmentString}s being intersected.
    * Note that some clients (such as MonotoneChains) may optimize away
    * this call for segment pairs which they have determined do not intersect
    * (e.g. by an disjoint envelope test).
    */
    void processIntersections(SegmentString* e0, size_t segIndex0, SegmentString* e1, size_t segIndex1) override;

    /**
    * Always process all intersections
    *
    */
    bool isDone() const override { return false; }


};

} // namespace geos::noding::snapround
} // namespace geos::noding
} // namespace geos





