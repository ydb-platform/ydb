/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2018 Paul Ramsey <pramsey@cleverlephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/Distance.java @ 2017-09-04
 *
 **********************************************************************/

#include <cmath>
#include <vector>
#include <algorithm>

#include <geos/algorithm/Distance.h>
#include <geos/geom/Envelope.h>
#include <geos/util/IllegalArgumentException.h>

namespace geos {
namespace algorithm { // geos.algorithm

/*public static*/
double
Distance::pointToSegment(const geom::Coordinate& p,
                         const geom::Coordinate& A,
                         const geom::Coordinate& B)
{
    /* if start==end, then use pt distance */
    if(A == B) {
        return p.distance(A);
    }

    /*
        otherwise use comp.graphics.algorithms method:
        (1)
                        AC dot AB
                    r = ---------
                        ||AB||^2

        r has the following meaning:
        r=0 P = A
        r=1 P = B
        r<0 P is on the backward extension of AB
        r>1 P is on the forward extension of AB
        0<r<1 P is interior to AB
    */

    double r = ((p.x - A.x) * (B.x - A.x) + (p.y - A.y) * (B.y - A.y)) /
               ((B.x - A.x) * (B.x - A.x) + (B.y - A.y) * (B.y - A.y));

    if(r <= 0.0) {
        return p.distance(A);
    }
    if(r >= 1.0) {
        return p.distance(B);
    }

    /*
        (2)
                (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)
            s = -----------------------------
                            L^2

        Then the distance from C to P = |s|*L.
    */

    double s = ((A.y - p.y) * (B.x - A.x) - (A.x - p.x) * (B.y - A.y)) /
               ((B.x - A.x) * (B.x - A.x) + (B.y - A.y) * (B.y - A.y));

    return fabs(s) * sqrt(((B.x - A.x) * (B.x - A.x) + (B.y - A.y) * (B.y - A.y)));
}

/*public static*/
double
Distance::pointToLinePerpendicular(const geom::Coordinate& p,
                                   const geom::Coordinate& A, const geom::Coordinate& B)
{
    /*
        use comp.graphics.algorithms method

        (2)
                (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)
            s = -----------------------------
                                 L^2

            Then the distance from C to P = |s|*L.
    */

    double s = ((A.y - p.y) * (B.x - A.x) - (A.x - p.x) * (B.y - A.y))
               /
               ((B.x - A.x) * (B.x - A.x) + (B.y - A.y) * (B.y - A.y));
    return fabs(s) * sqrt(((B.x - A.x) * (B.x - A.x) + (B.y - A.y) * (B.y - A.y)));
}

/*public static*/
double
Distance::segmentToSegment(const geom::Coordinate& A,
                           const geom::Coordinate& B, const geom::Coordinate& C,
                           const geom::Coordinate& D)
{
    /* Check for zero-length segments */
    if(A == B) {
        return pointToSegment(A, C, D);
    }
    if(C == D) {
        return pointToSegment(D, A, B);
    }

    /* AB and CD are line segments */
    /*
        From comp.graphics.algo

        Solving the above for r and s yields

            (Ay-Cy)(Dx-Cx)-(Ax-Cx)(Dy-Cy)
        r = ----------------------------- (eqn 1)
            (Bx-Ax)(Dy-Cy)-(By-Ay)(Dx-Cx)

            (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)
        s = ----------------------------- (eqn 2)
            (Bx-Ax)(Dy-Cy)-(By-Ay)(Dx-Cx)

        Let P be the position vector of the intersection point, then

            P=A+r(B-A) or
            Px=Ax+r(Bx-Ax)
            Py=Ay+r(By-Ay)

        By examining the values of r & s, you can also determine some other
        limiting conditions:

        If 0<=r<=1 & 0<=s<=1, intersection exists;
        If r<0 or r>1 or s<0 or s>1, line segments do not intersect;
        If the denominator in eqn 1 is zero, AB & CD are parallel;
        If the numerator in eqn 1 is also zero, AB & CD are collinear.
    */

    bool noIntersection = false;

    if (!geom::Envelope::intersects(A, B, C, D)) {
        noIntersection = true;
    } else {
        double denom = (B.x - A.x) * (D.y - C.y) - (B.y - A.y) * (D.x - C.x);

        if (denom == 0) {
            noIntersection = true;
        } else {
            double r_num = (A.y - C.y) * (D.x - C.x) - (A.x - C.x) * (D.y - C.y);
            double s_num = (A.y - C.y) * (B.x - A.x) - (A.x - C.x) * (B.y - A.y);

            double s = s_num / denom;
            double r = r_num / denom;

            if ((r < 0) || (r > 1) || (s < 0) || (s > 1)) {
                noIntersection = true;
            }
        }
    }

    if (noIntersection) {
        /* no intersection */
        return std::min(pointToSegment(A, C, D),
                        std::min(pointToSegment(B, C, D),
                                 std::min(pointToSegment(C, A, B), pointToSegment(D, A, B))));
    }

    return 0.0; /* intersection exists */
}

/*public static*/
double
Distance::pointToSegmentString(const geom::Coordinate& p,
                               const geom::CoordinateSequence* seq)
{
    if(seq->isEmpty()) {
        throw util::IllegalArgumentException(
            "Line array must contain at least one vertex");
    }

    /* this handles the case of length = 1 */
    double minDistance = p.distance(seq->getAt(0));
    for(std::size_t i = 0; i < seq->size() - 1; i++) {
        const geom::Coordinate& si = seq->getAt(i);
        const geom::Coordinate& si1 = seq->getAt(i + 1);
        double dist = pointToSegment(p, si, si1);

        if(dist < minDistance) {
            minDistance = dist;
        }
    }

    return minDistance;
}


} // namespace geos.algorithm
} //namespace geos

