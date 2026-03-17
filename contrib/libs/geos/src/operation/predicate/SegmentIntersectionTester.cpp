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

#include <geos/operation/predicate/SegmentIntersectionTester.h>
#include <geos/geom/LineString.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/algorithm/LineIntersector.h>

using namespace geos::geom;

namespace geos {
namespace operation {
namespace predicate {

bool
SegmentIntersectionTester::hasIntersectionWithLineStrings(
    const LineString& line,
    const LineString::ConstVect& lines)
{
    hasIntersectionVar = false;
    for(size_t i = 0, n = lines.size(); i < n; ++i) {
        const LineString* testLine = lines[i];
        hasIntersection(line, *testLine);
        if(hasIntersectionVar) {
            break;
        }
    }
    return hasIntersectionVar;
}

bool
SegmentIntersectionTester::hasIntersection(
    const LineString& line, const LineString& testLine)
{
    typedef std::size_t size_type;

    const CoordinateSequence& seq0 = *(line.getCoordinatesRO());
    size_type seq0size = seq0.getSize();

    const CoordinateSequence& seq1 = *(testLine.getCoordinatesRO());
    size_type seq1size = seq1.getSize();

    for(size_type i = 1; i < seq0size && !hasIntersectionVar; ++i) {
        const Coordinate& pt00 = seq0.getAt(i - 1);
        const Coordinate& pt01 = seq0.getAt(i);

        for(size_type j = 1; j < seq1size && !hasIntersectionVar; ++j) {
            const Coordinate& pt10 = seq1.getAt(j - 1);
            const Coordinate& pt11 = seq1.getAt(j);

            li.computeIntersection(pt00, pt01, pt10, pt11);
            if(li.hasIntersection()) {
                hasIntersectionVar = true;
            }
        }
    }

    return hasIntersectionVar;
}

bool
SegmentIntersectionTester::hasIntersectionWithEnvelopeFilter(
    const LineString& line, const LineString& testLine)
{
    typedef std::size_t size_type;

    const CoordinateSequence& seq0 = *(line.getCoordinatesRO());
    size_type seq0size = seq0.getSize();

    const CoordinateSequence& seq1 = *(testLine.getCoordinatesRO());
    size_type seq1size = seq1.getSize();

    const Envelope* lineEnv = line.getEnvelopeInternal();

    typedef std::size_t size_type;

    for(size_type i = 1; i < seq1size && !hasIntersectionVar; ++i) {
        const Coordinate& pt10 = seq1.getAt(i - 1);
        const Coordinate& pt11 = seq1.getAt(i);

        // skip test if segment does not intersect query envelope
        if(! lineEnv->intersects(Envelope(pt10, pt11))) {
            continue;
        }

        for(size_type j = 1; j < seq0size && !hasIntersectionVar; ++j) {
            const Coordinate& pt00 = seq0.getAt(j - 1);
            const Coordinate& pt01 = seq0.getAt(j);

            li.computeIntersection(pt00, pt01, pt10, pt11);
            if(li.hasIntersection()) {
                hasIntersectionVar = true;
            }
        }
    }

    return hasIntersectionVar;
}

} // namespace predicate
} // namespace operation
} // namespace geos



