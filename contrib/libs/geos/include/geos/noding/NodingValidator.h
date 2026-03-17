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
 **********************************************************************/

#ifndef GEOS_NODING_NODINGVALIDATOR_H
#define GEOS_NODING_NODINGVALIDATOR_H

#include <geos/export.h>

#include <vector>
#include <iostream>

#include <geos/inline.h>

#include <geos/algorithm/LineIntersector.h>
//#include <geos/geom/Coordinate.h>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
namespace noding {
class SegmentString;
}
}

namespace geos {
namespace noding { // geos.noding

/** \brief
 * Validates that a collection of {@link SegmentString}s is correctly noded.
 * Throws a TopologyException if a noding error is found.
 *
 * Last port: noding/NodingValidator.java rev. 1.6 (JTS-1.7)
 *
 */
class GEOS_DLL NodingValidator {
private:
    algorithm::LineIntersector li;
    const std::vector<SegmentString*>& segStrings;

    /**
     * Checks if a segment string contains a segment
     * pattern a-b-a (which implies a self-intersection)
     */
    void checkCollapses() const;

    void checkCollapses(const SegmentString& ss) const;

    void checkCollapse(const geom::Coordinate& p0, const geom::Coordinate& p1,
                       const geom::Coordinate& p2) const;

    /**
     * Checks all pairs of segments for intersections at an
     * interior point of a segment
     */
    void checkInteriorIntersections();

    void checkInteriorIntersections(const SegmentString& ss0,
                                    const SegmentString& ss1);

    void checkInteriorIntersections(
        const SegmentString& e0, size_t segIndex0,
        const SegmentString& e1, size_t segIndex1);

    /**
     * Checks for intersections between an endpoint of a segment string
     * and an interior vertex of another segment string
     */
    void checkEndPtVertexIntersections() const;

    void checkEndPtVertexIntersections(const geom::Coordinate& testPt,
                                       const std::vector<SegmentString*>& segStrings) const;

    /**
     * @return true if there is an intersection point which is not an
     *         endpoint of the segment p0-p1
     */
    bool hasInteriorIntersection(const algorithm::LineIntersector& aLi,
                                 const geom::Coordinate& p0, const geom::Coordinate& p1) const;

    // Declare type as noncopyable
    NodingValidator(const NodingValidator& other) = delete;
    NodingValidator& operator=(const NodingValidator& rhs) = delete;

public:

    NodingValidator(const std::vector<SegmentString*>& newSegStrings):
        segStrings(newSegStrings)
    {}

    ~NodingValidator() {}

    void checkValid();

};


} // namespace geos.noding
} // namespace geos

#endif // GEOS_NODING_NODINGVALIDATOR_H
