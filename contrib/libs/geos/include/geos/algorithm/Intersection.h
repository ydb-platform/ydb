/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Paul Ramsey <pramsey@cleverlephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_INTERSECTION_H
#define GEOS_ALGORITHM_INTERSECTION_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>

namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Computes the intersection point of two lines.
 * If the lines are parallel or collinear this case is detected
 * and <code>null</code> is returned.
 * <p>
 * In general it is not possible to accurately compute
 * the intersection point of two lines, due to
 * numerical roundoff.
 * This is particularly true when the input lines are nearly parallel.
 * This routine uses numerical conditioning on the input values
 * to ensure that the computed value should be very close to the correct value.
 *
 * @param p1 an endpoint of line 1
 * @param p2 an endpoint of line 1
 * @param q1 an endpoint of line 2
 * @param q2 an endpoint of line 2
 * @return the intersection point between the lines, if there is one,
 * or null if the lines are parallel or collinear
 *
 * @see CGAlgorithmsDD#intersection(Coordinate, Coordinate, Coordinate, Coordinate)
 */
class GEOS_DLL Intersection {

public:

static geom::Coordinate intersection(const geom::Coordinate& p1, const geom::Coordinate& p2,
                                     const geom::Coordinate& q1, const geom::Coordinate& q2);

};


} // namespace geos::algorithm
} // namespace geos


#endif // GEOS_ALGORITHM_INTERSECTION_H
