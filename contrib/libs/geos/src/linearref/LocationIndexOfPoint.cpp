/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: linearref/LocationIndexOfPoint.java r466
 *
 **********************************************************************/


#include <geos/geom/LineSegment.h>
#include <geos/linearref/LinearIterator.h>
#include <geos/linearref/LinearLocation.h>
#include <geos/linearref/LocationIndexOfPoint.h>
#include <geos/util/IllegalArgumentException.h>

#include <cassert>
#include <limits>

using namespace std;

using namespace geos::geom;

namespace geos {
namespace linearref { // geos.linearref

LinearLocation
LocationIndexOfPoint::indexOfFromStart(const Coordinate& inputPt,
                                       const LinearLocation* minIndex) const
{
    double minDistance = numeric_limits<double>::max();
    size_t minComponentIndex = 0;
    size_t minSegmentIndex = 0;
    double minFrac = -1.0;

    LineSegment seg;
    for(LinearIterator it(linearGeom);
            it.hasNext(); it.next()) {
        if(! it.isEndOfLine()) {
            seg.p0 = it.getSegmentStart();
            seg.p1 = it.getSegmentEnd();
            double segDistance = seg.distance(inputPt);
            double segFrac = seg.segmentFraction(inputPt);

            auto candidateComponentIndex = it.getComponentIndex();
            auto candidateSegmentIndex = it.getVertexIndex();
            if(segDistance < minDistance) {
                // ensure after minLocation, if any
                if(!minIndex ||
                        minIndex->compareLocationValues(candidateComponentIndex, candidateSegmentIndex, segFrac) < 0) {
                    // otherwise, save this as new minimum
                    minComponentIndex = candidateComponentIndex;
                    minSegmentIndex = candidateSegmentIndex;
                    minFrac = segFrac;
                    minDistance = segDistance;
                }
            }
        }
    }
    LinearLocation loc(minComponentIndex, minSegmentIndex, minFrac);
    return loc;
}


LinearLocation
LocationIndexOfPoint::indexOf(const Geometry* linearGeom, const Coordinate& inputPt)
{
    LocationIndexOfPoint locater(linearGeom);
    return locater.indexOf(inputPt);
}

LinearLocation
LocationIndexOfPoint::indexOfAfter(const Geometry* linearGeom, const Coordinate& inputPt,
                                   const LinearLocation* minIndex)
{
    LocationIndexOfPoint locater(linearGeom);
    return locater.indexOfAfter(inputPt, minIndex);
}

LocationIndexOfPoint::LocationIndexOfPoint(const Geometry* p_linearGeom) :
    linearGeom(p_linearGeom)
{}

LinearLocation
LocationIndexOfPoint::indexOf(const Coordinate& inputPt) const
{
    return indexOfFromStart(inputPt, nullptr);
}

LinearLocation
LocationIndexOfPoint::indexOfAfter(const Coordinate& inputPt,
                                   const LinearLocation* minIndex) const
{
    if(!minIndex) {
        return indexOf(inputPt);
    }

    // sanity check for minLocation at or past end of line
    LinearLocation endLoc = LinearLocation::getEndLocation(linearGeom);
    if(endLoc.compareTo(*minIndex) <= 0) {
        return endLoc;
    }

    LinearLocation closestAfter = indexOfFromStart(inputPt, minIndex);
    /*
     * Return the minDistanceLocation found.
     * This will not be null, since it was initialized to minLocation
     */
    if(closestAfter.compareTo(*minIndex) < 0) {
        throw util::IllegalArgumentException("computed location is before specified minimum location");
    }
    return closestAfter;
}
}
}
