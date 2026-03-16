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
 * Last port: algorithm/Area.java @ 2017-09-04
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_AREA_H
#define GEOS_ALGORITHM_AREA_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>

namespace geos {
namespace algorithm { // geos::algorithm


class GEOS_DLL Area {
public:

    /**
    * Computes the area for a ring.
    *
    * @param ring the coordinates forming the ring
    * @return the area of the ring
    */
    static double ofRing(const std::vector<geom::Coordinate>& ring);

    /**
    * Computes the area for a ring.
    *
    * @param ring the coordinates forming the ring
    * @return the area of the ring
    */
    static double ofRing(const geom::CoordinateSequence* ring);

    /**
    * Computes the signed area for a ring. The signed area is positive if the
    * ring is oriented CW, negative if the ring is oriented CCW, and zero if the
    * ring is degenerate or flat.
    *
    * @param ring
    *          the coordinates forming the ring
    * @return the signed area of the ring
    */
    static double ofRingSigned(const std::vector<geom::Coordinate>& ring);

    /**
    * Computes the signed area for a ring. The signed area is positive if the
    * ring is oriented CW, negative if the ring is oriented CCW, and zero if the
    * ring is degenerate or flat.
    *
    * @param ring
    *          the coordinates forming the ring
    * @return the signed area of the ring
    */
    static double ofRingSigned(const geom::CoordinateSequence* ring);

};


} // namespace geos::algorithm
} // namespace geos


#endif // GEOS_ALGORITHM_AREA_H
