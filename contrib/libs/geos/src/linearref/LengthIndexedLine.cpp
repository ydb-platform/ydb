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
 * Last port: linearref/LengthIndexedLine.java r463
 *
 **********************************************************************/

#include <geos/linearref/ExtractLineByLocation.h>
#include <geos/linearref/LengthIndexedLine.h>
#include <geos/linearref/LocationIndexedLine.h>
#include <geos/linearref/LinearLocation.h>
#include <geos/linearref/LengthLocationMap.h>
#include <geos/linearref/LengthIndexOfPoint.h>
#include <geos/linearref/LocationIndexOfLine.h>

using namespace std;

using namespace geos::geom;

namespace geos {
namespace linearref { // geos.linearref

LengthIndexedLine::LengthIndexedLine(const Geometry* p_linearGeom) :
    linearGeom(p_linearGeom) {}

Coordinate
LengthIndexedLine::extractPoint(double index) const
{
    LinearLocation loc = LengthLocationMap::getLocation(linearGeom, index);
    Coordinate coord = loc.getCoordinate(linearGeom);
    return coord;
}

Coordinate
LengthIndexedLine::extractPoint(double index, double offsetDistance) const
{
    LinearLocation loc = LengthLocationMap::getLocation(linearGeom, index);
    Coordinate ret;
    loc.getSegment(linearGeom)->pointAlongOffset(loc.getSegmentFraction(), offsetDistance, ret);
    return ret;
}


std::unique_ptr<Geometry>
LengthIndexedLine::extractLine(double startIndex, double endIndex) const
{
    const LocationIndexedLine lil(linearGeom);
    const double startIndex2 = clampIndex(startIndex);
    const double endIndex2 = clampIndex(endIndex);
    // if extracted line is zero-length, resolve start lower as well to
    // ensure they are equal
    const bool resolveStartLower = (startIndex2 == endIndex2);
    const LinearLocation startLoc = locationOf(startIndex2, resolveStartLower);
    const LinearLocation endLoc = locationOf(endIndex2);
//    LinearLocation endLoc = locationOf(endIndex2, true);
//    LinearLocation startLoc = locationOf(startIndex2);
    return ExtractLineByLocation::extract(linearGeom, startLoc, endLoc);
}

LinearLocation
LengthIndexedLine::locationOf(double index) const
{
    return LengthLocationMap::getLocation(linearGeom, index);
}

LinearLocation
LengthIndexedLine::locationOf(double index, bool resolveLower) const
{
    return LengthLocationMap::getLocation(linearGeom, index, resolveLower);
}


double
LengthIndexedLine::indexOf(const Coordinate& pt) const
{
    return LengthIndexOfPoint::indexOf(linearGeom, pt);
}


double
LengthIndexedLine::indexOfAfter(const Coordinate& pt, double minIndex) const
{
    return LengthIndexOfPoint::indexOfAfter(linearGeom, pt, minIndex);
}


double*
LengthIndexedLine::indicesOf(const Geometry* subLine) const
{
    LinearLocation* locIndex = LocationIndexOfLine::indicesOf(linearGeom, subLine);
    double* index = new double[2];
    index[0] = LengthLocationMap::getLength(linearGeom, locIndex[0]);
    index[1] = LengthLocationMap::getLength(linearGeom, locIndex[1]);
    delete [] locIndex;
    return index;
}


double
LengthIndexedLine::project(const Coordinate& pt) const
{
    return LengthIndexOfPoint::indexOf(linearGeom, pt);
}

double
LengthIndexedLine::getStartIndex() const
{
    return 0.0;
}

double
LengthIndexedLine::getEndIndex() const
{
    return linearGeom->getLength();
}

bool
LengthIndexedLine::isValidIndex(double index) const
{
    return (index >= getStartIndex()
            && index <= getEndIndex());
}

/* public */
double
LengthIndexedLine::clampIndex(double index) const
{
    double posIndex = positiveIndex(index);
    double startIndex = getStartIndex();
    if(posIndex < startIndex) {
        return startIndex;
    }

    double endIndex = getEndIndex();
    if(posIndex > endIndex) {
        return endIndex;
    }

    return posIndex;
}

/* private */
double
LengthIndexedLine::positiveIndex(double index) const
{
    if(index >= 0.0) {
        return index;
    }
    return linearGeom->getLength() + index;
}

} // geos.linearref
} // geos
