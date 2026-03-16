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

#ifndef GEOS_ALGORITHM_CGALGORITHMDD_H
#define GEOS_ALGORITHM_CGALGORITHMDD_H
#include <geos/export.h>
#include <geos/math/DD.h>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class CoordinateSequence;
}
}

using namespace geos::math;

namespace geos {
namespace algorithm { // geos::algorithm

/// Implements basic computational geometry algorithms using extended precision float-point arithmetic.
class GEOS_DLL CGAlgorithmsDD {

public:

    enum {
        CLOCKWISE = -1,
        COLLINEAR = 0,
        COUNTERCLOCKWISE = 1
    };

    enum {
        RIGHT = -1,
        LEFT = 1,
        STRAIGHT = 0,
        FAILURE = 2
    };

    /** \brief
     * Returns the index of the direction of the point `q` relative to
     * a vector specified by `p1-p2`.
     *
     * @param p1 the origin point of the vector
     * @param p2 the final point of the vector
     * @param q the point to compute the direction to
     *
     * @return 1 if q is counter-clockwise (left) from p1-p2
     * @return -1 if q is clockwise (right) from p1-p2
     * @return 0 if q is collinear with p1-p2
     */
    static int orientationIndex(const geom::Coordinate& p1,
                                const geom::Coordinate& p2,
                                const geom::Coordinate& q);


    static int orientationIndex(double p1x, double p1y,
                                double p2x, double p2y,
                                double qx,  double qy);

    /**
     * A filter for computing the orientation index of three coordinates.
     *
     * If the orientation can be computed safely using standard DP arithmetic,
     * this routine returns the orientation index. Otherwise, a value `i > 1` is
     * returned. In this case the orientation index must be computed using some
     * other more robust method.
     *
     * The filter is fast to compute, so can be used to avoid the use of slower
     * robust methods except when they are really needed, thus providing better
     * average performance.
     *
     * Uses an approach due to Jonathan Shewchuk, which is in the public domain.
     */
    static int orientationIndexFilter(double pax, double pay,
                                      double pbx, double pby,
                                      double pcx, double pcy);


    static int
    orientation(double x)
    {
        if(x < 0) {
            return CGAlgorithmsDD::RIGHT;
        }
        if(x > 0) {
            return CGAlgorithmsDD::LEFT;
        }
        return CGAlgorithmsDD::STRAIGHT;
    }

    /**
     * If the lines are parallel (either identical
     * or separate) a null value is returned.
     * @param p1 an endpoint of line segment 1
     * @param p2 an endpoint of line segment 1
     * @param q1 an endpoint of line segment 2
     * @param q2 an endpoint of line segment 2
     * @return an intersection point if one exists, or null if the lines are parallel
     */
    static geom::Coordinate intersection(const geom::Coordinate& p1, const geom::Coordinate& p2,
                             const geom::Coordinate& q1, const geom::Coordinate& q2);

    static int signOfDet2x2(double dx1, double dy1, double dx2, double dy2);

    static DD detDD(double x1, double y1, double x2, double y2);
    static DD detDD(const DD& x1, const DD& y1, const DD& x2, const DD& y2);

    /** \brief
     * Computes the circumcentre of a triangle.
     *
     * The circumcentre is the centre of the circumcircle, the smallest circle
     * which encloses the triangle. It is also the common intersection point of
     * the perpendicular bisectors of the sides of the triangle, and is the only
     * point which has equal distance to all three vertices of the triangle.
     *
     * The circumcentre does not necessarily lie within the triangle. For example,
     * the circumcentre of an obtuse isosceles triangle lies outside the triangle.
     *
     * This method uses @ref geos::math::DD extended-precision arithmetic to provide more accurate
     * results than [circumcentre(Coordinate, Coordinate, Coordinate)]
     * (@ref geos::geom::Triangle::circumcentre(const Coordinate& p0, const Coordinate& p1, const Coordinate& p2)).
     *
     * @param a
     *          a vertex of the triangle
     * @param b
     *          a vertex of the triangle
     * @param c
     *          a vertex of the triangle
     * @return the circumcentre of the triangle
     */
    static geom::Coordinate circumcentreDD(const geom::Coordinate& a, const geom::Coordinate& b, const geom::Coordinate& c);

protected:

    static int signOfDet2x2(const DD& x1, const DD& y1, const DD& x2, const DD& y2);

};

} // namespace geos::algorithm
} // namespace geos

#endif // GEOS_ALGORITHM_CGALGORITHM_H
