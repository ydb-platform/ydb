/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/geom/Triangle.h>
#include <geos/geom/Coordinate.h>
#include <geos/algorithm/CGAlgorithmsDD.h>

namespace geos {
namespace geom { // geos::geom


bool
Triangle::isIsoceles()
{
    double len0 = p1.distance(p2);
    double len1 = p0.distance(p2);
    double len2 = p0.distance(p1);
    if (len0 == len1 || len1 == len2 || len2 == len0)
        return true;
    else
        return false;
}

void
Triangle::inCentre(Coordinate& result)
{
    // the lengths of the sides, labelled by their opposite vertex
    double len0 = p1.distance(p2);
    double len1 = p0.distance(p2);
    double len2 = p0.distance(p1);
    double circum = len0 + len1 + len2;
    double inCentreX = (len0 * p0.x + len1 * p1.x + len2 * p2.x)  / circum;
    double inCentreY = (len0 * p0.y + len1 * p1.y + len2 * p2.y)  / circum;

    result = Coordinate(inCentreX, inCentreY);
}

void
Triangle::circumcentre(Coordinate& result)
{
    double cx = p2.x;
    double cy = p2.y;
    double ax = p0.x - cx;
    double ay = p0.y - cy;
    double bx = p1.x - cx;
    double by = p1.y - cy;

    double denom = 2 * det(ax, ay, bx, by);
    double numx = det(ay, ax * ax + ay * ay, by, bx * bx + by * by);
    double numy = det(ax, ax * ax + ay * ay, bx, bx * bx + by * by);

    double ccx = cx - numx / denom;
    double ccy = cy + numy / denom;

    result = Coordinate(ccx, ccy);
}

void
Triangle::circumcentreDD(Coordinate& result)
{
    result = algorithm::CGAlgorithmsDD::circumcentreDD(p0, p1, p2);
}

/* public static */
const Coordinate
Triangle::circumcentre(const Coordinate& p0, const Coordinate& p1, const Coordinate& p2)
{
    Triangle t(p0, p1, p2);
    Coordinate c;
    t.circumcentre(c);
    return c;
}

/* private */
double
Triangle::det(double m00, double m01, double m10, double m11) const
{
    return m00 * m11 - m01 * m10;
}


} // namespace geos::geom
} // namespace geos
