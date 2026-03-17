/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009    Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/OrientedCoordinateArray.java rev. 1.1 (JTS-1.9)
 *
 **********************************************************************/

//#include <cmath>
//#include <sstream>

#include <geos/noding/OrientedCoordinateArray.h>

#include <geos/geom/CoordinateSequence.h>

using namespace geos::geom;

#ifdef _MSC_VER
#pragma warning(disable : 4127)
#endif

namespace geos {
namespace noding { // geos.noding

/* private static */
bool
OrientedCoordinateArray::orientation(const CoordinateSequence& pts)
{
    return CoordinateSequence::increasingDirection(pts) == 1;
}

int
OrientedCoordinateArray::compareTo(const OrientedCoordinateArray& oca) const
{
    int comp = compareOriented(*pts, orientationVar,
                               *oca.pts, oca.orientationVar);
#if 0 // MD - testing only
    int oldComp = SegmentStringDissolver.ptsComp.compare(pts, oca.pts);
    if((oldComp == 0 || comp == 0) && oldComp != comp) {
        System.out.println("bidir mismatch");

        boolean orient1 = orientation(pts);
        boolean orient2 = orientation(oca.pts);
        int comp2 = compareOriented(pts, orientation,
                                    oca.pts, oca.orientation);
        int oldComp2 = SegmentStringDissolver.ptsComp.compare(pts, oca.pts);
    }
#endif

    return comp;
}


/* private static */
int
OrientedCoordinateArray::compareOriented(const geom::CoordinateSequence& pts1,
        bool orientation1,
        const geom::CoordinateSequence& pts2,
        bool orientation2)
{
    int dir1 = orientation1 ? 1 : -1;
    int dir2 = orientation2 ? 1 : -1;
    auto limit1 = orientation1 ? pts1.size() : -1;
    auto limit2 = orientation2 ? pts2.size() : -1;

    auto i1 = orientation1 ? 0 : pts1.size() - 1;
    auto i2 = orientation2 ? 0 : pts2.size() - 1;
    //int comp = 0; // unused, but is in JTS ...
    while(true) {
        int compPt = pts1[i1].compareTo(pts2[i2]);
        if(compPt != 0) {
            return compPt;
        }
        i1 += dir1;
        i2 += dir2;
        bool done1 = i1 == limit1;
        bool done2 = i2 == limit2;
        if(done1 && ! done2) {
            return -1;
        }
        if(! done1 && done2) {
            return 1;
        }
        if(done1 && done2) {
            return 0;
        }
    }
}

bool
OrientedCoordinateArray::operator==(const OrientedCoordinateArray& other) const {
    auto sz1 = pts->size();
    auto sz2 = other.pts->size();

    if (sz1 != sz2) {
        return false;
    }

    if (orientationVar == other.orientationVar) {
        for (size_t i = 0; i < sz1; i++) {
            if (pts->getAt(i) != other.pts->getAt(i)) {
                return false;
            }
        }
    } else {
        for (size_t i = 0; i < sz1; i++) {
            if (pts->getAt(i) != other.pts->getAt(sz2 - i - 1)) {
                return false;
            }
        }
    }

    return true;
}

size_t
OrientedCoordinateArray::HashCode::operator()(const geos::noding::OrientedCoordinateArray &oca) const {
    Coordinate::HashCode coordHash;

    auto sz = oca.pts->getSize();

    size_t result = std::hash<size_t>{}(sz);

    if (oca.orientationVar) {
        for (size_t i = 0; i < sz; i++) {
            result ^= coordHash(oca.pts->getAt(i));
        }
    } else {
        for (size_t i = sz; i > 0; i--) {
            result ^= coordHash(oca.pts->getAt(i-1));
        }
    }

    return result;
}

} // namespace geos.noding
} // namespace geos

