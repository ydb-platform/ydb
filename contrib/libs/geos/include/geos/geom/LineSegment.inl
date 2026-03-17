/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: geom/LineSegment.java r18 (JTS-1.11)
 *
 **********************************************************************/

#ifndef GEOS_LINESEGMENT_INL
#define GEOS_LINESEGMENT_INL

#include <geos/geom/LineSegment.h>
#include <geos/algorithm/Distance.h>
#include <geos/algorithm/Orientation.h>

#include <cassert>
#include <cmath> // for atan2

namespace geos {
namespace geom { // geos::geom

INLINE
LineSegment::LineSegment(const Coordinate& c0, const Coordinate& c1)
    :
    p0(c0),
    p1(c1)
{
}

INLINE
LineSegment::LineSegment(double x0, double y0, double x1, double y1)
    :
    p0(x0, y0),
    p1(x1, y1)
{
}

INLINE
LineSegment::LineSegment()
{
}

INLINE double
LineSegment::distancePerpendicular(const Coordinate& p) const
{
    return algorithm::Distance::pointToLinePerpendicular(p, p0, p1);
}

INLINE void
LineSegment::pointAlong(double segmentLengthFraction, Coordinate& ret) const
{
    ret = Coordinate(
              p0.x + segmentLengthFraction * (p1.x - p0.x),
              p0.y + segmentLengthFraction * (p1.y - p0.y));
}

INLINE double
LineSegment::distance(const LineSegment& ls) const
{
    return algorithm::Distance::segmentToSegment(p0, p1, ls.p0, ls.p1);
}

/*public*/
INLINE double
LineSegment::distance(const Coordinate& p) const
{
    return algorithm::Distance::pointToSegment(p, p0, p1);
}

INLINE void
LineSegment::normalize()
{
    if(p1.compareTo(p0) < 0) {
        reverse();
    }
}


INLINE void
LineSegment::setCoordinates(const Coordinate& c0, const Coordinate& c1)
{
    p0 = c0;
    p1 = c1;
}

INLINE const Coordinate&
LineSegment::operator[](std::size_t i) const
{
    if(i == 0) {
        return p0;
    }
    assert(i == 1);
    return p1;
}

INLINE Coordinate&
LineSegment::operator[](std::size_t i)
{
    if(i == 0) {
        return p0;
    }
    assert(i == 1);
    return p1;
}

INLINE void
LineSegment::setCoordinates(const LineSegment& ls)
{
    setCoordinates(ls.p0, ls.p1);
}

INLINE double
LineSegment::getLength() const
{
    return p0.distance(p1);
}

INLINE bool
LineSegment::isHorizontal() const
{
    return p0.y == p1.y;
}

INLINE bool
LineSegment::isVertical() const
{
    return p0.x == p1.x;
}

INLINE int
LineSegment::orientationIndex(const LineSegment* seg) const
{
    assert(seg);
    return orientationIndex(*seg);
}

INLINE int
LineSegment::orientationIndex(const Coordinate& p) const
{
    return algorithm::Orientation::index(p0, p1, p);
}

INLINE std::array<Coordinate, 2>
LineSegment::closestPoints(const LineSegment* line)
{
    assert(line);
    return closestPoints(*line);
}

INLINE double
LineSegment::angle() const
{
    return std::atan2(p1.y - p0.y, p1.x - p0.x);
}

INLINE Coordinate
LineSegment::midPoint() const
{
    return midPoint(p0, p1);
}

INLINE std::ostream&
operator<< (std::ostream& o, const LineSegment& l)
{
    return o << "LINESEGMENT(" << l.p0.x << " " << l.p0.y << "," << l.p1.x << " " << l.p1.y << ")";
}

INLINE bool
operator==(const LineSegment& a, const LineSegment& b)
{
    return a.p0 == b.p0 && a.p1 == b.p1;
}


} // namespace geos::geom
} // namespace geos

#endif // GEOS_LINESEGMENT_INL
