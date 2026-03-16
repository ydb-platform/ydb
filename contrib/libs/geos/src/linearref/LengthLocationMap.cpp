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
 * Last port: algorithm/LengthLocationMap.java r463
 *
 **********************************************************************/


#include <geos/linearref/LengthIndexedLine.h>
#include <geos/linearref/LinearIterator.h>
#include <geos/linearref/LinearLocation.h>
#include <geos/linearref/LengthLocationMap.h>

using namespace std;

using namespace geos::geom;

namespace geos {
namespace linearref { // geos.linearref


double
LengthLocationMap::getLength(const Geometry* linearGeom, const LinearLocation& loc)
{
    LengthLocationMap locater(linearGeom);
    return locater.getLength(loc);
}


LengthLocationMap::LengthLocationMap(const Geometry* p_linearGeom) :
    linearGeom(p_linearGeom) {}

LinearLocation
LengthLocationMap::getLocation(double length) const
{
    double forwardLength = length;
    if(length < 0.0) {
        double lineLen = linearGeom->getLength();
        forwardLength = lineLen + length;
    }
    return getLocationForward(forwardLength);
}

LinearLocation
LengthLocationMap::getLocation(double length, bool resolveLower) const
{
    double forwardLength = length;

    // negative values are measured from end of geometry
    if(length < 0.0) {
        double lineLen = linearGeom->getLength();
        forwardLength = lineLen + length;
    }

    LinearLocation loc = getLocationForward(forwardLength);
    if(resolveLower) {
        return loc;
    }
    return resolveHigher(loc);
}

/* private */
LinearLocation
LengthLocationMap::getLocationForward(double length) const
{
    if(length <= 0.0) {
        return LinearLocation();
    }

    double totalLength = 0.0;

    LinearIterator it(linearGeom);
    while(it.hasNext()) {
        /*
         * Special handling is required for the situation when the
         * length references exactly to a component endpoint.
         * In this case, the endpoint location of the current component
         * is returned,
         * rather than the startpoint location of the next component.
         * This produces consistent behaviour with the project method.
         */
        if(it.isEndOfLine()) {
            if(totalLength == length) {
                auto compIndex = it.getComponentIndex();
                auto segIndex = it.getVertexIndex();
                return LinearLocation(compIndex, segIndex, 0.0);
            }
        }
        else {
            Coordinate p0 = it.getSegmentStart();
            Coordinate p1 = it.getSegmentEnd();
            double segLen = p1.distance(p0);
            // length falls in this segment
            if(totalLength + segLen > length) {
                double frac = (length - totalLength) / segLen;
                auto compIndex = it.getComponentIndex();
                auto segIndex = it.getVertexIndex();
                return LinearLocation(compIndex, segIndex, frac);
            }
            totalLength += segLen;
        }

        it.next();
    }
    // length is longer than line - return end location
    return LinearLocation::getEndLocation(linearGeom);
}

/* private */
LinearLocation
LengthLocationMap::resolveHigher(const LinearLocation& loc) const
{
    if(! loc.isEndpoint(*linearGeom)) {
        return loc;
    }

    auto compIndex = loc.getComponentIndex();
    // if last component can't resolve any higher
    if(compIndex >= linearGeom->getNumGeometries() - 1) {
        return loc;
    }

    do {
        compIndex++;
    }
    while(compIndex < linearGeom->getNumGeometries() - 1
            && linearGeom->getGeometryN(compIndex)->getLength() == 0);

    // resolve to next higher location
    return LinearLocation(compIndex, 0, 0.0);
}


double
LengthLocationMap::getLength(const LinearLocation& loc) const
{
    double totalLength = 0.0;

    LinearIterator it(linearGeom);
    while(it.hasNext()) {
        if(! it.isEndOfLine()) {
            Coordinate p0 = it.getSegmentStart();
            Coordinate p1 = it.getSegmentEnd();
            double segLen = p1.distance(p0);
            // length falls in this segment
            if(loc.getComponentIndex() == it.getComponentIndex()
                    && loc.getSegmentIndex() == it.getVertexIndex()) {
                return totalLength + segLen * loc.getSegmentFraction();
            }
            totalLength += segLen;
        }
        it.next();
    }
    return totalLength;
}

}
}
