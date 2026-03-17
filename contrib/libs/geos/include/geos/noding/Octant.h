/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006      Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_NODING_OCTANT_H
#define GEOS_NODING_OCTANT_H

#include <geos/export.h>

#include <geos/inline.h>
#include <geos/util.h>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace noding { // geos.noding

/** \brief
 * Methods for computing and working with octants of the Cartesian plane.
 *
 * Octants are numbered as follows:
 *
 *   2|1
 * 3  |  0
 * ---+--
 * 4  |  7
 *   5|6
 *
 * If line segments lie along a coordinate axis, the octant is the lower of the two
 * possible values.
 *
 * Last port: noding/Octant.java rev. 1.2 (JTS-1.7)
 */
class GEOS_DLL Octant {
private:
    Octant() {} // Can't instanciate it
public:

    /**
     * Returns the octant of a directed line segment (specified
     * as x and y displacements, which cannot both be 0).
     */
    static int octant(double dx, double dy);

    /**
     * Returns the octant of a directed line segment from p0 to p1.
     */
    static int octant(const geom::Coordinate& p0, const geom::Coordinate& p1);

    static int
    octant(const geom::Coordinate* p0, const geom::Coordinate* p1)
    {
        ::geos::ignore_unused_variable_warning(p0);
        return octant(*p0, *p1);
    }
};


} // namespace geos.noding
} // namespace geos

#endif
