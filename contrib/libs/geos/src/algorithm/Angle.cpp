/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009-2011 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/Angle.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <cmath>

#include <geos/algorithm/Angle.h>
#include <geos/geom/Coordinate.h>

namespace geos {
namespace algorithm { // geos.algorithm

namespace {
const double PI = 3.14159265358979323846;
}

const double Angle::PI_TIMES_2 = 2.0 * PI;
const double Angle::PI_OVER_2 = PI / 2.0;
const double Angle::PI_OVER_4 = PI / 4.0;

/* public static */
double
Angle::toDegrees(double radians)
{
    return (radians * 180) / (PI);
}

/* public static */
double
Angle::toRadians(double angleDegrees)
{
    return (angleDegrees * PI) / 180.0;
}

/* public static */
double
Angle::angle(const geom::Coordinate& p0,
             const geom::Coordinate& p1)
{
    double dx = p1.x - p0.x;
    double dy = p1.y - p0.y;
    return atan2(dy, dx);
}

/* public static */
double
Angle::angle(const geom::Coordinate& p)
{
    return atan2(p.y, p.x);
}

/* public static */
bool
Angle::isAcute(const geom::Coordinate& p0,
               const geom::Coordinate& p1,
               const geom::Coordinate& p2)
{
    // relies on fact that A dot B is positive iff A ang B is acute
    double dx0 = p0.x - p1.x;
    double dy0 = p0.y - p1.y;
    double dx1 = p2.x - p1.x;
    double dy1 = p2.y - p1.y;
    double dotprod = dx0 * dx1 + dy0 * dy1;
    return dotprod > 0;
}

/* public static */
bool
Angle::isObtuse(const geom::Coordinate& p0,
                const geom::Coordinate& p1,
                const geom::Coordinate& p2)
{
    // relies on fact that A dot B is negative iff A ang B is obtuse
    double dx0 = p0.x - p1.x;
    double dy0 = p0.y - p1.y;
    double dx1 = p2.x - p1.x;
    double dy1 = p2.y - p1.y;
    double dotprod = dx0 * dx1 + dy0 * dy1;
    return dotprod < 0;
}

/* public static */
double
Angle::angleBetween(const geom::Coordinate& tip1,
                    const geom::Coordinate& tail,
                    const geom::Coordinate& tip2)
{
    double a1 = angle(tail, tip1);
    double a2 = angle(tail, tip2);

    return diff(a1, a2);
}

/* public static */
double
Angle::angleBetweenOriented(const geom::Coordinate& tip1,
                            const geom::Coordinate& tail,
                            const geom::Coordinate& tip2)
{
    double a1 = angle(tail, tip1);
    double a2 = angle(tail, tip2);
    double angDel = a2 - a1;

    // normalize, maintaining orientation
    if(angDel <= -PI) {
        return angDel + PI_TIMES_2;
    }
    if(angDel > PI) {
        return angDel - PI_TIMES_2;
    }
    return angDel;
}

/* public static */
double
Angle::interiorAngle(const geom::Coordinate& p0, const geom::Coordinate& p1,
                     const geom::Coordinate& p2)
{
    double anglePrev = angle(p1, p0);
    double angleNext = angle(p1, p2);
    return fabs(angleNext - anglePrev);
}

/* public static */
int
Angle::getTurn(double ang1, double ang2)
{
    double crossproduct = sin(ang2 - ang1);

    if(crossproduct > 0) {
        return COUNTERCLOCKWISE;
    }
    if(crossproduct < 0) {
        return CLOCKWISE;
    }
    return NONE;
}

/* public static */
double
Angle::normalize(double angle)
{
    while(angle > PI) {
        angle -= PI_TIMES_2;
    }
    while(angle <= -PI) {
        angle += PI_TIMES_2;
    }
    return angle;
}

/* public static */
double
Angle::normalizePositive(double angle)
{
    if(angle < 0.0) {
        while(angle < 0.0) {
            angle += PI_TIMES_2;
        }
        // in case round-off error bumps the value over
        if(angle >= PI_TIMES_2) {
            angle = 0.0;
        }
    }
    else {
        while(angle >= PI_TIMES_2) {
            angle -= PI_TIMES_2;
        }
        // in case round-off error bumps the value under
        if(angle < 0.0) {
            angle = 0.0;
        }
    }
    return angle;
}

/* public static */
double
Angle::diff(double ang1, double ang2)
{
    double delAngle;

    if(ang1 < ang2) {
        delAngle = ang2 - ang1;
    }
    else {
        delAngle = ang1 - ang2;
    }

    if(delAngle > PI) {
        delAngle = (2 * PI) - delAngle;
    }

    return delAngle;
}

} // namespace geos.algorithm
} //namespace geos

