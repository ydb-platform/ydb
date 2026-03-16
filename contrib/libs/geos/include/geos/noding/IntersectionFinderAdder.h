/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006      Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/IntersectionFinderAdder.java rev. 1.5 (JTS-1.9)
 *
 **********************************************************************/

#ifndef GEOS_NODING_INTERSECTIONFINDERADDER_H
#define GEOS_NODING_INTERSECTIONFINDERADDER_H

#include <geos/export.h>

#include <vector>
#include <iostream>

#include <geos/inline.h>

#include <geos/geom/Coordinate.h> // for use in vector
#include <geos/noding/SegmentIntersector.h> // for inheritance

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
namespace noding {
class SegmentString;
}
namespace algorithm {
class LineIntersector;
}
}

namespace geos {
namespace noding { // geos.noding

/** \brief
 * Finds proper and interior intersections in a set of SegmentStrings,
 * and adds them as nodes.
 *
 */
class GEOS_DLL IntersectionFinderAdder: public SegmentIntersector {

public:

    /** \brief
     * Creates an intersection finder which finds all proper intersections
     * and stores them in the provided Coordinate array
     *
     * @param newLi the LineIntersector to use
     * @param v  the Vector to push interior intersections to
     */
    IntersectionFinderAdder(algorithm::LineIntersector& newLi,
                            std::vector<geom::Coordinate>& v)
        :
        li(newLi),
        interiorIntersections(v)
    {}

    /** \brief
     * This method is called by clients of the SegmentIntersector
     * class to process intersections for two segments of the
     * {@link SegmentString}s being intersected.
     *
     * Note that some clients (such as `MonotoneChains`) may
     * optimize away this call for segment pairs which they have
     * determined do not intersect
     * (e.g. by an disjoint envelope test).
     */
    void processIntersections(
        SegmentString* e0,  size_t segIndex0,
        SegmentString* e1,  size_t segIndex1) override;

    std::vector<geom::Coordinate>&
    getInteriorIntersections()
    {
        return interiorIntersections;
    }

    /**
     * Always process all intersections
     *
     * @return false always
     */
    bool
    isDone() const override
    {
        return false;
    }

private:
    algorithm::LineIntersector& li;
    std::vector<geom::Coordinate>& interiorIntersections;

    // Declare type as noncopyable
    IntersectionFinderAdder(const IntersectionFinderAdder& other) = delete;
    IntersectionFinderAdder& operator=(const IntersectionFinderAdder& rhs) = delete;
};

} // namespace geos.noding
} // namespace geos

#endif // GEOS_NODING_INTERSECTIONFINDERADDER_H
