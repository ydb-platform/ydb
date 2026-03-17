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
 * Last port: algorithm/Length.java @ 2017-09-04
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_LENGTH_H
#define GEOS_ALGORITHM_LENGTH_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>

namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Functions for computing length.
 *
 * @author Martin Davis
 */
class GEOS_DLL Length {
public:

    /**
     * Computes the length of a linestring specified by a sequence of points.
     *
     * @param ring the points specifying the linestring
     * @return the length of the linestring
     */
    static double ofLine(const geom::CoordinateSequence* ring);

};


} // namespace geos::algorithm
} // namespace geos


#endif // GEOS_ALGORITHM_LENGTH_H
