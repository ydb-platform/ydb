/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Excensus LLC.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: triangulate/Vertex.java r705
 *
 **********************************************************************/

#include <geos/triangulate/quadedge/Vertex.h>

#include <geos/triangulate/quadedge/TrianglePredicate.h>
#include <geos/triangulate/quadedge/QuadEdge.h>
#include <geos/algorithm/NotRepresentableException.h>
#include <geos/util.h>

namespace geos {
namespace triangulate { //geos.triangulate
namespace quadedge { //geos.triangulate.quadedge

using namespace algorithm;
using namespace geom;

Vertex::Vertex(double _x, double _y) : p(_x, _y)
{
}

Vertex::Vertex(double _x, double _y, double _z): p(_x, _y, _z)
{
}

Vertex::Vertex(const Coordinate& _p) : p(_p)
{
}

Vertex::Vertex() : p()
{
}

int
Vertex::classify(const Vertex& p0, const Vertex& p1)
{
    Vertex& p2 = *this;
    std::unique_ptr<Vertex> a = p1.sub(p0);
    std::unique_ptr<Vertex> b = p2.sub(p0);
    double sa = a->crossProduct(*b);

    if(sa > 0.0) {
        return LEFT;
    }
    if(sa < 0.0) {
        return RIGHT;
    }
    if((a->getX() * b->getX() < 0.0) || (a->getY() * b->getY() < 0.0)) {
        return BEHIND;
    }
    if(a->magn() < b->magn()) {
        return BEYOND;
    }
    if(p0.equals(p2)) {
        return ORIGIN;
    }
    if(p1.equals(p2)) {
        return DESTINATION;
    }
    else {
        return BETWEEN;
    }
}

bool
Vertex::rightOf(const QuadEdge& e) const
{
    return isCCW(e.dest(), e.orig());
}

bool
Vertex::leftOf(const QuadEdge& e) const
{
    return isCCW(e.orig(), e.dest());
}

std::unique_ptr<HCoordinate>
Vertex::bisector(const Vertex& a, const Vertex& b)
{
    // returns the perpendicular bisector of the line segment ab
    double dx = b.getX() - a.getX();
    double dy = b.getY() - a.getY();
    HCoordinate l1 = HCoordinate(a.getX() + dx / 2.0, a.getY() + dy / 2.0, 1.0);
    HCoordinate l2 = HCoordinate(a.getX() - dy + dx / 2.0, a.getY() + dx + dy / 2.0, 1.0);

    return detail::make_unique<HCoordinate>(l1, l2);
}

double
Vertex::circumRadiusRatio(const Vertex& b, const Vertex& c)
{
    std::unique_ptr<Vertex> x(circleCenter(b, c));
    double radius = distance(*x, b);
    double edgeLength = distance(*this, b);
    double el = distance(b, c);
    if(el < edgeLength) {
        edgeLength = el;
    }
    el = distance(c, *this);
    if(el < edgeLength) {
        edgeLength = el;
    }

    return radius / edgeLength;
}

std::unique_ptr<Vertex>
Vertex::midPoint(const Vertex& a)
{
    double xm = (p.x + a.getX()) / 2.0;
    double ym = (p.y + a.getY()) / 2.0;
    double zm = (p.z + a.getZ()) / 2.0;
    return detail::make_unique<Vertex>(xm, ym, zm);
}

std::unique_ptr<Vertex>
Vertex::circleCenter(const Vertex& b, const Vertex& c) const
{
    auto a = detail::make_unique<Vertex>(getX(), getY());
    // compute the perpendicular bisector of cord ab
    std::unique_ptr<HCoordinate> cab = bisector(*a, b);
    // compute the perpendicular bisector of cord bc
    std::unique_ptr<HCoordinate> cbc = bisector(b, c);
    // compute the intersection of the bisectors (circle radii)
    std::unique_ptr<HCoordinate> hcc = detail::make_unique<HCoordinate>(*cab, *cbc);
    std::unique_ptr<Vertex> cc;

    try {
        cc.reset(new Vertex(hcc->getX(), hcc->getY()));
    }
    catch(NotRepresentableException &) {
    }

    return cc;
}

double
Vertex::interpolateZValue(const Vertex& v0, const Vertex& v1,
                          const Vertex& v2) const
{
    double x0 = v0.getX();
    double y0 = v0.getY();
    double a = v1.getX() - x0;
    double b = v2.getX() - x0;
    double c = v1.getY() - y0;
    double d = v2.getY() - y0;
    double det = a * d - b * c;
    double dx = this->getX() - x0;
    double dy = this->getY() - y0;
    double t = (d * dx - b * dy) / det;
    double u = (-c * dx + a * dy) / det;
    double z = v0.getZ() + t * (v1.getZ() - v0.getZ()) + u * (v2.getZ() - v0.getZ());
    return z;
}

double
Vertex::interpolateZ(const Coordinate& p, const Coordinate& v0,
                     const Coordinate& v1, const Coordinate& v2)
{
    double x0 = v0.x;
    double y0 = v0.y;
    double a = v1.x - x0;
    double b = v2.x - x0;
    double c = v1.y - y0;
    double d = v2.y - y0;
    double det = a * d - b * c;
    double dx = p.x - x0;
    double dy = p.y - y0;
    double t = (d * dx - b * dy) / det;
    double u = (-c * dx + a * dy) / det;
    double z = v0.z + t * (v1.z - v0.z) + u * (v2.z - v0.z);
    return z;
}

double
Vertex::interpolateZ(const Coordinate& p, const Coordinate& p0,
                     const Coordinate& p1)
{
    double segLen = p0.distance(p1);
    double ptLen = p.distance(p0);
    double dz = p1.z - p0.z;
    double pz = p0.z + dz * (ptLen / segLen);
    return pz;
}

} //namespace geos.triangulate.quadedge
} //namespace geos.triangulate
} //namespace geos
