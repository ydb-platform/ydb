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

#ifndef GEOS_ALGORITHM_DISTANCE_H
#define GEOS_ALGORITHM_DISTANCE_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>

namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Functions to compute distance between basic geometric structures.
 *
 * @author Martin Davis
 *
 */
class GEOS_DLL Distance {
public:

    /**
     * Computes the distance from a line segment AB to a line segment CD
     *
     * Note: NON-ROBUST!
     *
     * @param A
     *          a point of one line
     * @param B
     *          the second point of (must be different to A)
     * @param C
     *          one point of the line
     * @param D
     *          another point of the line (must be different to A)
     */
    // formerly distanceLineLine
    static double segmentToSegment(const geom::Coordinate& A,
                                   const geom::Coordinate& B,
                                   const geom::Coordinate& C,
                                   const geom::Coordinate& D);

    /**
    * Computes the distance from a point to a sequence of line segments.
    *
    * @param p
    *          a point
    * @param seq
    *          a sequence of contiguous line segments defined by their vertices
    * @return the minimum distance between the point and the line segments
    */
    static double pointToSegmentString(const geom::Coordinate& p,
                                       const geom::CoordinateSequence* seq);

    /**
    * Computes the distance from a point p to a line segment AB
    *
    * Note: NON-ROBUST!
    *
    * @param p
    *          the point to compute the distance for
    * @param A
    *          one point of the line
    * @param B
    *          another point of the line (must be different to A)
    * @return the distance from p to line segment AB
    */
    // formerly distancePointLine
    static double pointToSegment(const geom::Coordinate& p,
                                 const geom::Coordinate& A,
                                 const geom::Coordinate& B);

    /**
    * Computes the perpendicular distance from a point p to the (infinite) line
    * containing the points AB
    *
    * @param p
    *          the point to compute the distance for
    * @param A
    *          one point of the line
    * @param B
    *          another point of the line (must be different to A)
    * @return the distance from p to line AB
    */
    // formerly distancePointLinePerpendicular
    static double pointToLinePerpendicular(const geom::Coordinate& p,
                                           const geom::Coordinate& A,
                                           const geom::Coordinate& B);

};

} // namespace geos::algorithm
} // namespace geos

#endif // GEOS_ALGORITHM_DISTANCE_H
