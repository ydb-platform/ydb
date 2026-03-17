/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/NodingValidator.java rev. 1.6 (JTS-1.7)
 *
 **********************************************************************/

#include <sstream>
#include <vector>

#include <geos/util/TopologyException.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/noding/NodingValidator.h>
#include <geos/noding/SegmentString.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>

using namespace std;
using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace noding { // geos.noding

/*public*/
void
NodingValidator::checkValid()
{
    checkEndPtVertexIntersections();
    checkInteriorIntersections();
    checkCollapses();
}

/*private*/
void
NodingValidator::checkCollapses() const
{
    for(SegmentString::NonConstVect::const_iterator
            it = segStrings.begin(), itEnd = segStrings.end();
            it != itEnd;
            ++it) {
        const SegmentString* ss = *it;
        checkCollapses(*ss);
    }
}

/* private */
void
NodingValidator::checkCollapses(const SegmentString& ss) const
{
    const CoordinateSequence& pts = *(ss.getCoordinates());
    for(size_t i = 0, n = pts.size() - 2; i < n; ++i) {
        checkCollapse(pts[i], pts[i + 1], pts[i + 2]);
    }
}

/* private */
void
NodingValidator::checkCollapse(const Coordinate& p0,
                               const Coordinate& p1, const Coordinate& p2) const
{
    if(p0.equals2D(p2))
        throw util::TopologyException("found non-noded collapse at " +
                                      p0.toString() + ", " +
                                      p1.toString() + ", " +
                                      p2.toString());
}

/*private*/
void
NodingValidator::checkInteriorIntersections()
{
    for(SegmentString::NonConstVect::const_iterator
            it = segStrings.begin(), itEnd = segStrings.end();
            it != itEnd;
            ++it) {
        SegmentString* ss0 = *it;
        for(SegmentString::NonConstVect::const_iterator
                j = segStrings.begin(), jEnd = segStrings.end();
                j != jEnd; ++j) {
            const SegmentString* ss1 = *j;
            checkInteriorIntersections(*ss0, *ss1);
        }
    }

}

/* private */
void
NodingValidator::checkInteriorIntersections(const SegmentString& ss0,
        const SegmentString& ss1)
{
    const CoordinateSequence& pts0 = *(ss0.getCoordinates());
    const CoordinateSequence& pts1 = *(ss1.getCoordinates());
    for(size_t i0 = 0, n0 = pts0.size(); i0 < n0 - 1; ++i0) {
        for(size_t i1 = 0, n1 = pts1.size(); i1 < n1 - 1; ++i1) {
            checkInteriorIntersections(ss0, i0, ss1, i1);
        }
    }
}


/* private */
void
NodingValidator::checkInteriorIntersections(
    const SegmentString& e0, size_t segIndex0,
    const SegmentString& e1, size_t segIndex1)
{
    if(&e0 == &e1 && segIndex0 == segIndex1) {
        return;
    }
    const Coordinate& p00 = e0.getCoordinates()->getAt(segIndex0);
    const Coordinate& p01 = e0.getCoordinates()->getAt(segIndex0 + 1);
    const Coordinate& p10 = e1.getCoordinates()->getAt(segIndex1);
    const Coordinate& p11 = e1.getCoordinates()->getAt(segIndex1 + 1);

    li.computeIntersection(p00, p01, p10, p11);
    if(li.hasIntersection()) {
        if(li.isProper()
                || hasInteriorIntersection(li, p00, p01)
                || hasInteriorIntersection(li, p10, p11)) {
            throw util::TopologyException(
                "found non-noded intersection at "
                + p00.toString() + "-" + p01.toString()
                + " and "
                + p10.toString() + "-" + p11.toString());
        }
    }
}

/* private */
void
NodingValidator::checkEndPtVertexIntersections() const
{
    for(SegmentString::NonConstVect::const_iterator
            it = segStrings.begin(), itEnd = segStrings.end();
            it != itEnd;
            ++it) {
        const SegmentString* ss = *it;
        const CoordinateSequence& pts = *(ss->getCoordinates());
        checkEndPtVertexIntersections(pts[0], segStrings);
        checkEndPtVertexIntersections(pts[pts.size() - 1], segStrings);
    }
}

/* private */
void
NodingValidator::checkEndPtVertexIntersections(const Coordinate& testPt,
        const SegmentString::NonConstVect& p_segStrings) const
{
    for(SegmentString::NonConstVect::const_iterator
            it = p_segStrings.begin(), itEnd = p_segStrings.end();
            it != itEnd;
            ++it) {
        const SegmentString* ss0 = *it;
        const CoordinateSequence& pts = *(ss0->getCoordinates());
        for(size_t j = 1, n = pts.size() - 1; j < n; ++j) {
            if(pts[j].equals(testPt)) {
                stringstream s;
                s << "found endpt/interior pt intersection ";
                s << "at index " << j << " :pt " << testPt;
                throw util::TopologyException(s.str());
            }
        }
    }
}





/* private */
bool
NodingValidator::hasInteriorIntersection(const LineIntersector& aLi,
        const Coordinate& p0, const Coordinate& p1) const
{
    for(size_t i = 0, n = aLi.getIntersectionNum(); i < n; ++i) {
        const Coordinate& intPt = aLi.getIntersection(i);
        if(!(intPt == p0 || intPt == p1)) {
            return true;
        }
    }
    return false;
}


} // namespace geos.noding
} // namespace geos

