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
 * Last port: algorithm/PointLocation.java @ 2017-09-04
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_POINTLOCATION_H
#define GEOS_ALGORITHM_POINTLOCATION_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Location.h>

namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Functions for locating points within basic geometric
 * structures such as lines and rings.
 *
 * @author Martin Davis
 *
 */
class GEOS_DLL PointLocation {
public:

    /** \brief
     * Tests whether a point lies on the line defined by a
     * [CoordinateSequence](@ref geom::CoordinateSequence).
     *
     * @param p the point to test
     * @param line the line coordinates
     * @return true if the point is a vertex of the line or lies in the interior
     *         of a line segment in the line
     */
    static bool isOnLine(const geom::Coordinate& p, const geom::CoordinateSequence* line);

    /** \brief
     * Tests whether a point lies inside or on a ring.
     *
     * The ring may be oriented in either direction. A point lying exactly
     * on the ring boundary is considered to be inside the ring.
     *
     * This method does **not** first check the point against the envelope of
     * the ring.
     *
     * @param p point to check for ring inclusion
     * @param ring an array of coordinates representing the ring (which must have
     *             first point identical to last point)
     * @return `true` if p is inside ring
     *
     * @see RayCrossingCounter::locatePointInRing()
     */
    static bool isInRing(const geom::Coordinate& p, const std::vector<const geom::Coordinate*>& ring);
    static bool isInRing(const geom::Coordinate& p, const geom::CoordinateSequence* ring);

    /** \brief
     * Determines whether a point lies in the interior, on the boundary, or in the
     * exterior of a ring. The ring may be oriented in either direction.
     *
     * This method does *not* first check the point against the envelope of
     * the ring.
     *
     * @param p point to check for ring inclusion
     * @param ring an array of coordinates representing the ring (which must have
     *             first point identical to last point)
     * @return the [Location](@ref geom::Location) of p relative to the ring
     */
    static geom::Location locateInRing(const geom::Coordinate& p, const std::vector<const geom::Coordinate*>& ring);
    static geom::Location locateInRing(const geom::Coordinate& p, const geom::CoordinateSequence& ring);

};


} // namespace geos::algorithm
} // namespace geos


#endif // GEOS_ALGORITHM_POINTLOCATION_H
