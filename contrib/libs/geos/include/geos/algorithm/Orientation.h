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
 * Last port: algorithm/Orientation.java @ 2017-09-04
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_ORIENTATION_H
#define GEOS_ALGORITHM_ORIENTATION_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>

namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Functions to compute the orientation of basic geometric structures
 * including point triplets (triangles) and rings.
 *
 * Orientation is a fundamental property of planar geometries
 * (and more generally geometry on two-dimensional manifolds).
 *
 * Orientation is notoriously subject to numerical precision errors
 * in the case of collinear or nearly collinear points.
 * JTS uses extended-precision arithmetic to increase
 * the robustness of the computation.
 *
 * @author Martin Davis
 */
class GEOS_DLL Orientation {
public:

    /* A value that indicates an orientation or turn */
    enum {
        CLOCKWISE = -1,
        COLLINEAR = 0,
        COUNTERCLOCKWISE = 1,
        RIGHT = -1,
        LEFT = 1,
        STRAIGHT = 0
    };

    /** \brief
     * Returns the orientation index of the direction of the point q relative to
     * a directed infinite line specified by p1-p2.
     *
     * The index indicates whether the point lies to the
     * `Orientation::LEFT` or `Orientation::RIGHT`
     * of the line, or lies on it `Orientation::COLLINEAR`.
     * The index also indicates the orientation of the triangle formed
     * by the three points
     * ( `Orientation::COUNTERCLOCKWISE`,
     * `Orientation::CLOCKWISE`, or `Orientation::STRAIGHT` )
     */
    static int index(const geom::Coordinate& p1, const geom::Coordinate& p2,
                     const geom::Coordinate& q);

    /**
    * Computes whether a ring defined by a geom::CoordinateSequence is
    * oriented counter-clockwise.
    *
    *  * The list of points is assumed to have the first and last points equal.
    *  * This handles coordinate lists which contain repeated points.
    *  * This handles rings which contain collapsed segments
    *    (in particular, along the top of the ring).
    *
    * This algorithm is guaranteed to work with valid rings.
    * It also works with "mildly invalid" rings
    * which contain collapsed (coincident) flat segments along the top of the ring.
    * If the ring is "more" invalid (e.g. self-crosses or touches),
    * the computed result may not be correct.
    *
    * @param ring a CoordinateSequence forming a ring (with first and last point identical)
    * @return true if the ring is oriented counter-clockwise.
    * @throws IllegalArgumentException if there are too few points to determine orientation (< 4)
    */
    static bool isCCW(const geom::CoordinateSequence* ring);

    /**
    * Tests if a ring defined by a CoordinateSequence is
    * oriented counter-clockwise, using the signed area of the ring.
    *
    *  * The list of points is assumed to have the first and last points equal.
    *  * This handles coordinate lists which contain repeated points.
    *  * This handles rings which contain collapsed segments
    *    (in particular, along the top of the ring).
    *  * This handles rings which are invalid due to self-intersection
    *
    * This algorithm is guaranteed to work with valid rings.
    * For invalid rings (containing self-intersections),
    * the algorithm determines the orientation of
    * the largest enclosed area (including overlaps).
    * This provides a more useful result in some situations, such as buffering.
    *
    * However, this approach may be less accurate in the case of
    * rings with almost zero area.
    * (Note that the orientation of rings with zero area is essentially
    * undefined, and hence non-deterministic.)
    *
    * @param ring a CoordinateSequence forming a ring (with first and last point identical)
    * @return true if the ring is oriented counter-clockwise.
    */
    static bool isCCWArea(const geom::CoordinateSequence* ring);

};


} // namespace geos::algorithm
} // namespace geos


#endif // GEOS_ALGORITHM_ORIENTATION_H
