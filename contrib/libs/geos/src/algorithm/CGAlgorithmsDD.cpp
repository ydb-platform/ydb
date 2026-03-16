/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2014 Mateusz Loskot <mateusz@loskot.net>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/CGAlgorithmsDD.java r789 (JTS-1.14)
 *
 **********************************************************************/


#include <geos/algorithm/CGAlgorithmsDD.h>
#include <geos/geom/Coordinate.h>
#include <geos/util/IllegalArgumentException.h>
#include <sstream>
#include <cmath>

using namespace geos::geom;
using namespace geos::algorithm;

namespace {

/**
 * A value which is safely greater than the relative round-off
 * error in double-precision numbers
 */
double constexpr DP_SAFE_EPSILON =  1e-15;

inline int
OrientationDD(const DD &dd)
{
    static DD const zero(0.0);
    if(dd < zero) {
        return CGAlgorithmsDD::RIGHT;
    }

    if(dd > zero) {
        return CGAlgorithmsDD::LEFT;
    }

    return CGAlgorithmsDD::STRAIGHT;
}


}

namespace geos {
namespace algorithm { // geos::algorithm


int
CGAlgorithmsDD::orientationIndex(double p1x, double p1y,
                                 double p2x, double p2y,
                                 double qx,  double qy)
{
    if(!std::isfinite(qx) || !std::isfinite(qy)) {
        throw util::IllegalArgumentException("CGAlgorithmsDD::orientationIndex encountered NaN/Inf numbers");
    }

    // fast filter for orientation index
    // avoids use of slow extended-precision arithmetic in many cases
    int index = orientationIndexFilter(p1x, p1y, p2x, p2y, qx, qy);
    if(index <= 1) {
        return index;
    }

    // normalize coordinates
    DD dx1 = DD(p2x) + DD(-p1x);
    DD dy1 = DD(p2y) + DD(-p1y);
    DD dx2 = DD(qx) + DD(-p2x);
    DD dy2 = DD(qy) + DD(-p2y);

    // sign of determinant - inlined for performance
    DD mx1y2(dx1 * dy2);
    DD my1x2(dy1 * dx2);
    DD d = mx1y2 - my1x2;
    return OrientationDD(d);
}


int
CGAlgorithmsDD::orientationIndex(const Coordinate& p1,
                                 const Coordinate& p2,
                                 const Coordinate& q)
{

    return orientationIndex(p1.x, p1.y, p2.x, p2.y, q.x, q.y);
}


int
CGAlgorithmsDD::signOfDet2x2(const DD& x1, const DD& y1, const DD& x2, const DD& y2)
{
    DD mx1y2(x1 * y2);
    DD my1x2(y1 * x2);
    DD d = mx1y2 - my1x2;
    return OrientationDD(d);
}

int
CGAlgorithmsDD::signOfDet2x2(double dx1, double dy1, double dx2, double dy2)
{
    if(!std::isfinite(dx1) || !std::isfinite(dy1) || !std::isfinite(dx2) || !std::isfinite(dy2)) {
        throw util::IllegalArgumentException("CGAlgorithmsDD::signOfDet2x2 encountered NaN/Inf numbers");
    }
    DD x1(dx1);
    DD y1(dy1);
    DD x2(dx2);
    DD y2(dy2);
    return CGAlgorithmsDD::signOfDet2x2(x1, y1, x2, y2);
}

int
CGAlgorithmsDD::orientationIndexFilter(double pax, double pay,
                                       double pbx, double pby,
                                       double pcx, double pcy)
{
    double detsum;
    double const detleft = (pax - pcx) * (pby - pcy);
    double const detright = (pay - pcy) * (pbx - pcx);
    double const det = detleft - detright;

    if(detleft > 0.0) {
        if(detright <= 0.0) {
            return orientation(det);
        }
        else {
            detsum = detleft + detright;
        }
    }
    else if(detleft < 0.0) {
        if(detright >= 0.0) {
            return orientation(det);
        }
        else {
            detsum = -detleft - detright;
        }
    }
    else {
        return orientation(det);
    }

    double const errbound = DP_SAFE_EPSILON * detsum;
    if((det >= errbound) || (-det >= errbound)) {
        return orientation(det);
    }
    return CGAlgorithmsDD::FAILURE;
}

Coordinate
CGAlgorithmsDD::intersection(const Coordinate& p1, const Coordinate& p2,
                             const Coordinate& q1, const Coordinate& q2)
{
    DD q1x(q1.x);
    DD q1y(q1.y);
    DD q2x(q2.x);
    DD q2y(q2.y);

    DD p1x(p1.x);
    DD p1y(p1.y);
    DD p2x(p2.x);
    DD p2y(p2.y);

    DD px = p1y - p2y;
    DD py = p2x - p1x;
    DD pw = (p1x * p2y) - (p2x * p1y);

    DD qx = q1y - q2y;
    DD qy = q2x - q1x;
    DD qw = (q1x * q2y) - (q2x * q1y);

    DD x = (py * qw) - (qy * pw);
    DD y = (qx * pw) - (px * qw);
    DD w = (px * qy) - (qx * py);

    double xInt = (x / w).ToDouble();
    double yInt = (y / w).ToDouble();

    Coordinate rv;

    if (!std::isfinite(xInt) || !std::isfinite(yInt)) {
        rv.setNull();
        return rv;
    }

    rv.x = xInt;
    rv.y = yInt;
    return rv;
}

/* public static */
Coordinate
CGAlgorithmsDD::circumcentreDD(const Coordinate& a, const Coordinate& b, const Coordinate& c)
{
    DD ax = DD(a.x) - DD(c.x);
    DD ay = DD(a.y) - DD(c.y);
    DD bx = DD(b.x) - DD(c.x);
    DD by = DD(b.y) - DD(c.y);

    DD denom = DD(2) * detDD(ax, ay, bx, by);
    DD asqr = (ax * ax) + (ay * ay);
    DD bsqr = (bx * bx) + (by * by);
    DD numx = detDD(ay, asqr, by, bsqr);
    DD numy = detDD(ax, asqr, bx, bsqr);
    double ccx = (DD(c.x) - (numx / denom)).ToDouble();
    double ccy = (DD(c.y) + (numy / denom)).ToDouble();
    Coordinate cc(ccx, ccy);
    return cc;
}

/* public static */
DD
CGAlgorithmsDD::detDD(double x1, double y1, double x2, double y2)
{
    return detDD(DD(x1), DD(y1), DD(x2), DD(y2));
}

/* public static */
DD
CGAlgorithmsDD::detDD(const DD& x1, const DD& y1, const DD& x2, const DD& y2)
{
    return (x1 * y2) - (y1 * x2);
}

} // namespace geos::algorithm
} // namespace geos


