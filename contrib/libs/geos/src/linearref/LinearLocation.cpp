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
 * Last port: linearref/LinearLocation.java r463
 *
 **********************************************************************/

#include <geos/geom/LineString.h>
#include <geos/linearref/LengthIndexedLine.h>
#include <geos/linearref/LinearLocation.h>
#include <geos/linearref/LengthLocationMap.h>
#include <geos/util/IllegalArgumentException.h>

using namespace std;

using namespace geos::geom;

namespace geos {
namespace linearref { // geos::linearref

/* public static */
LinearLocation
LinearLocation::getEndLocation(const Geometry* linear)
{
    // assert: linear is LineString or MultiLineString
    LinearLocation loc;
    loc.setToEnd(linear);
    return loc;
}

/* public static */
Coordinate
LinearLocation::pointAlongSegmentByFraction(const Coordinate& p0, const Coordinate& p1, double frac)
{
    if(frac <= 0.0) {
        return p0;
    }
    if(frac >= 1.0) {
        return p1;
    }

    double x = (p1.x - p0.x) * frac + p0.x;
    double y = (p1.y - p0.y) * frac + p0.y;
    // interpolate Z value. If either input Z is NaN, result z will be NaN as well.
    double z = (p1.z - p0.z) * frac + p0.z;
    return Coordinate(x, y, z);
}

/* public */
LinearLocation::LinearLocation(size_t p_segmentIndex,
                               double p_segmentFraction)
    :
    componentIndex(0),
    segmentIndex(p_segmentIndex),
    segmentFraction(p_segmentFraction)
{}


/* public */
LinearLocation::LinearLocation(size_t p_componentIndex,
                               size_t p_segmentIndex, double p_segmentFraction)
    :
    componentIndex(p_componentIndex),
    segmentIndex(p_segmentIndex),
    segmentFraction(p_segmentFraction)
{
    normalize();
}

/* private */
void
LinearLocation::normalize()
{
    if(segmentFraction < 0.0) {
        segmentFraction = 0.0;
    }
    if(segmentFraction > 1.0) {
        segmentFraction = 1.0;
    }

    if(segmentFraction == 1.0) {
        segmentFraction = 0.0;
        segmentIndex += 1;
    }
}

/* public */
void
LinearLocation::clamp(const Geometry* linear)
{
    if(componentIndex >= linear->getNumGeometries()) {
        setToEnd(linear);
        return;
    }
    if(segmentIndex >= linear->getNumPoints()) {
        const LineString* line = dynamic_cast<const LineString*>(linear->getGeometryN(componentIndex));
        segmentIndex = line->getNumPoints() - 1;
        segmentFraction = 1.0;
    }
}

/* public */
void
LinearLocation::snapToVertex(const Geometry* linearGeom, double minDistance)
{
    if(segmentFraction <= 0.0 || segmentFraction >= 1.0) {
        return;
    }
    double segLen = getSegmentLength(linearGeom);
    double lenToStart = segmentFraction * segLen;
    double lenToEnd = segLen - lenToStart;
    if(lenToStart <= lenToEnd && lenToStart < minDistance) {
        segmentFraction = 0.0;
    }
    else if(lenToEnd <= lenToStart && lenToEnd < minDistance) {
        segmentFraction = 1.0;
    }
}

/* public */
double
LinearLocation::getSegmentLength(const Geometry* linearGeom) const
{
    const LineString* lineComp = dynamic_cast<const LineString*>(linearGeom->getGeometryN(componentIndex));

    // ensure segment index is valid
    size_t segIndex = segmentIndex;
    if(segmentIndex >= lineComp->getNumPoints() - 1) {
        segIndex = lineComp->getNumPoints() - 2;
    }

    Coordinate p0 = lineComp->getCoordinateN(segIndex);
    Coordinate p1 = lineComp->getCoordinateN(segIndex + 1);
    return p0.distance(p1);
}

/* public */
void
LinearLocation::setToEnd(const Geometry* linear)
{
    componentIndex = linear->getNumGeometries();
    if ( componentIndex == 0 )
    {
        segmentIndex = 0;
        segmentFraction = 0;
        return;
    }
    componentIndex--;
    const LineString* lastLine = dynamic_cast<const LineString*>(linear->getGeometryN(componentIndex));
    segmentIndex = lastLine->getNumPoints() - 1;
    segmentFraction = 1.0;
}

/* public */
size_t
LinearLocation::getComponentIndex() const
{
    return componentIndex;
}

/* public */
size_t
LinearLocation::getSegmentIndex() const
{
    return segmentIndex;
}

/* public */
double
LinearLocation::getSegmentFraction() const
{
    return segmentFraction;
}

/* public */
bool
LinearLocation::isVertex() const
{
    return segmentFraction <= 0.0 || segmentFraction >= 1.0;
}

/* public */
Coordinate
LinearLocation::getCoordinate(const Geometry* linearGeom) const
{
    if(linearGeom->isEmpty()) {
        return Coordinate::getNull();
    }
    const LineString* lineComp = dynamic_cast<const LineString*>(linearGeom->getGeometryN(componentIndex));
    if(! lineComp) {
        throw util::IllegalArgumentException("LinearLocation::getCoordinate only works with LineString geometries");
    }
    Coordinate p0 = lineComp->getCoordinateN(segmentIndex);
    if(segmentIndex >= lineComp->getNumPoints() - 1) {
        return p0;
    }
    Coordinate p1 = lineComp->getCoordinateN(segmentIndex + 1);
    return pointAlongSegmentByFraction(p0, p1, segmentFraction);
}

/* public */
std::unique_ptr<LineSegment>
LinearLocation::getSegment(const Geometry* linearGeom) const
{
    const LineString* lineComp = dynamic_cast<const LineString*>(linearGeom->getGeometryN(componentIndex));
    Coordinate p0 = lineComp->getCoordinateN(segmentIndex);
    // check for endpoint - return last segment of the line if so
    if(segmentIndex >= lineComp->getNumPoints() - 1) {
        Coordinate prev = lineComp->getCoordinateN(lineComp->getNumPoints() - 2);
        return std::unique_ptr<LineSegment>(new LineSegment(prev, p0));
    }
    Coordinate p1 = lineComp->getCoordinateN(segmentIndex + 1);
    return std::unique_ptr<LineSegment>(new LineSegment(p0, p1));
}

/* public */
bool
LinearLocation::isValid(const Geometry* linearGeom) const
{
    if(componentIndex >= linearGeom->getNumGeometries()) {
        return false;
    }

    const LineString* lineComp = dynamic_cast<const LineString*>(linearGeom->getGeometryN(componentIndex));
    if(segmentIndex > lineComp->getNumPoints()) {
        return false;
    }
    if(segmentIndex == lineComp->getNumPoints() && segmentFraction != 0.0) {
        return false;
    }

    if(segmentFraction < 0.0 || segmentFraction > 1.0) {
        return false;
    }
    return true;
}

/* public */
int
LinearLocation::compareTo(const LinearLocation& other) const
{
    // compare component indices
    if(componentIndex < other.componentIndex) {
        return -1;
    }
    if(componentIndex > other.componentIndex) {
        return 1;
    }
    // compare segments
    if(segmentIndex < other.segmentIndex) {
        return -1;
    }
    if(segmentIndex > other.segmentIndex) {
        return 1;
    }
    // same segment, so compare segment fraction
    if(segmentFraction < other.segmentFraction) {
        return -1;
    }
    if(segmentFraction > other.segmentFraction) {
        return 1;
    }
    // same location
    return 0;
}

/* public */
int
LinearLocation::compareLocationValues(size_t componentIndex1,
                                      size_t segmentIndex1, double segmentFraction1) const
{
    // compare component indices
    if(componentIndex < componentIndex1) {
        return -1;
    }
    if(componentIndex > componentIndex1) {
        return 1;
    }
    // compare segments
    if(segmentIndex < segmentIndex1) {
        return -1;
    }
    if(segmentIndex > segmentIndex1) {
        return 1;
    }
    // same segment, so compare segment fraction
    if(segmentFraction < segmentFraction1) {
        return -1;
    }
    if(segmentFraction > segmentFraction1) {
        return 1;
    }
    // same location
    return 0;
}


/* public static */
int
LinearLocation::compareLocationValues(
    size_t componentIndex0, size_t segmentIndex0,
    double segmentFraction0,
    size_t componentIndex1, size_t segmentIndex1,
    double segmentFraction1)
{
    // compare component indices
    if(componentIndex0 < componentIndex1) {
        return -1;
    }
    if(componentIndex0 > componentIndex1) {
        return 1;
    }
    // compare segments
    if(segmentIndex0 < segmentIndex1) {
        return -1;
    }
    if(segmentIndex0 > segmentIndex1) {
        return 1;
    }
    // same segment, so compare segment fraction
    if(segmentFraction0 < segmentFraction1) {
        return -1;
    }
    if(segmentFraction0 > segmentFraction1) {
        return 1;
    }
    // same location
    return 0;
}


/* public */
bool
LinearLocation::isOnSameSegment(const LinearLocation& loc) const
{
    if(componentIndex != loc.componentIndex) {
        return false;
    }
    if(segmentIndex == loc.segmentIndex) {
        return true;
    }
    if(loc.segmentIndex - segmentIndex == 1
            && loc.segmentFraction == 0.0) {
        return true;
    }
    if(segmentIndex - loc.segmentIndex == 1
            && segmentFraction == 0.0) {
        return true;
    }
    return false;
}

/* public */
bool
LinearLocation::isEndpoint(const Geometry& linearGeom) const
{
    const LineString& lineComp = dynamic_cast<const LineString&>(
                                     *(linearGeom.getGeometryN(componentIndex)));
    // check for endpoint
    auto nseg = lineComp.getNumPoints() - 1;
    return segmentIndex >= nseg
           || (segmentIndex == nseg && segmentFraction >= 1.0);

}


ostream&
operator<<(ostream& out, const LinearLocation& obj)
{
    return out << "LinearLoc[" << obj.componentIndex << ", " <<
           obj.segmentIndex << ", " << obj.segmentFraction << "]";
}

} // namespace geos.linearref
} // namespace geos

