/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
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
 * Last port: algorithm/RobustLineIntersector.java rev. 1.35
 *
 **********************************************************************/

#include <geos/linearref/ExtractLineByLocation.h>
#include <geos/linearref/LengthIndexedLine.h>
#include <geos/linearref/LinearIterator.h>
#include <geos/linearref/LengthLocationMap.h>
#include <geos/linearref/LengthIndexOfPoint.h>
#include <geos/util/IllegalArgumentException.h>

#include <limits>

using namespace std;

using namespace geos::geom;

namespace geos {
namespace linearref { // geos.linearref

double
LengthIndexOfPoint::indexOf(const Geometry* linearGeom, const Coordinate& inputPt)
{
    LengthIndexOfPoint locater(linearGeom);
    return locater.indexOf(inputPt);
}

double
LengthIndexOfPoint::indexOfAfter(const Geometry* linearGeom, const Coordinate& inputPt, double minIndex)
{
    LengthIndexOfPoint locater(linearGeom);
    return locater.indexOfAfter(inputPt, minIndex);
}

LengthIndexOfPoint::LengthIndexOfPoint(const Geometry* p_linearGeom):
    linearGeom(p_linearGeom) {}

double
LengthIndexOfPoint::indexOf(const Coordinate& inputPt) const
{
    return indexOfFromStart(inputPt, -1.0);
}


double
LengthIndexOfPoint::indexOfAfter(const Coordinate& inputPt, double minIndex) const
{
    if(minIndex < 0.0) {
        return indexOf(inputPt);
    }

    // sanity check for minIndex at or past end of line
    double endIndex = linearGeom->getLength();
    if(endIndex < minIndex) {
        return endIndex;
    }

    double closestAfter = indexOfFromStart(inputPt, minIndex);
    /*
     * Return the minDistanceLocation found.
     * This will not be null, since it was initialized to minLocation
     */
    if(closestAfter <= minIndex) {
        throw util::IllegalArgumentException("computed index is before specified minimum index");
    }
    return closestAfter;
}

double
LengthIndexOfPoint::indexOfFromStart(const Coordinate& inputPt, double minIndex) const
{
    double minDistance = numeric_limits<double>::max();

    double ptMeasure = minIndex;
    double segmentStartMeasure = 0.0;
    LineSegment seg;
    LinearIterator it(linearGeom);
    while(it.hasNext()) {
        if(! it.isEndOfLine()) {
            seg.p0 = it.getSegmentStart();
            seg.p1 = it.getSegmentEnd();
            double segDistance = seg.distance(inputPt);
            double segMeasureToPt = segmentNearestMeasure(&seg, inputPt, segmentStartMeasure);
            if(segDistance < minDistance
                    && segMeasureToPt > minIndex) {
                ptMeasure = segMeasureToPt;
                minDistance = segDistance;
            }
            segmentStartMeasure += seg.getLength();
        }
        it.next();
    }
    return ptMeasure;
}

double
LengthIndexOfPoint::segmentNearestMeasure(const LineSegment* seg,
        const Coordinate& inputPt,
        double segmentStartMeasure) const
{
    // found new minimum, so compute location distance of point
    double projFactor = seg->projectionFactor(inputPt);
    if(projFactor <= 0.0) {
        return segmentStartMeasure;
    }
    if(projFactor <= 1.0) {
        return segmentStartMeasure + projFactor * seg->getLength();
    }
    // projFactor > 1.0
    return segmentStartMeasure + seg->getLength();
}
}
}

