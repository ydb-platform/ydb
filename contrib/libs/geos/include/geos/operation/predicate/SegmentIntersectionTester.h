/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/predicate/SegmentIntersectionTester.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_PREDICATE_SEGMENTINTERSECTIONTESTER_H
#define GEOS_OP_PREDICATE_SEGMENTINTERSECTIONTESTER_H

#include <geos/export.h>

#include <geos/algorithm/LineIntersector.h> // for composition
#include <geos/geom/Coordinate.h> // for composition

// Forward declarations
namespace geos {
namespace geom {
class LineString;
class CoordinateSequence;
}
}

namespace geos {
namespace operation { // geos::operation
namespace predicate { // geos::operation::predicate

/** \brief
 * Tests if any line segments in two sets of CoordinateSequences intersect.
 *
 * The algorithm is optimized for use when the first input has smaller extent
 * than the set of test lines.
 * The code is short-circuited to return as soon an intersection is found.
 *
 */
class GEOS_DLL SegmentIntersectionTester {

private:

    /// \brief
    /// For purposes of intersection testing,
    /// don't need to set precision model
    ///
    algorithm::LineIntersector li; // Robust

    bool hasIntersectionVar;

public:

    SegmentIntersectionTester(): hasIntersectionVar(false) {}

    bool hasIntersectionWithLineStrings(const geom::LineString& line,
                                        const std::vector<const geom::LineString*>& lines);

    bool hasIntersection(const geom::LineString& line,
                         const geom::LineString& testLine);

    /**
     * Tests the segments of a LineString against the segs in
     * another LineString for intersection.
     * Uses the envelope of the query LineString
     * to filter before testing segments directly.
     * This is optimized for the case when the query
     * LineString is a rectangle.
     *
     * Testing shows this is somewhat faster than not checking the envelope.
     *
     * @param line
     * @param testLine
     * @return
     */
    bool hasIntersectionWithEnvelopeFilter(const geom::LineString& line,
                                           const geom::LineString& testLine);


};

} // namespace geos::operation::predicate
} // namespace geos::operation
} // namespace geos

#endif // ifndef GEOS_OP_PREDICATE_SEGMENTINTERSECTIONTESTER_H
