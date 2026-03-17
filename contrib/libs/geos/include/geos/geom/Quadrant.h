/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/Quadrant.java rev. 1.8 (JTS-1.10)
 *
 **********************************************************************/


#ifndef GEOS_GEOM_QUADRANT_H
#define GEOS_GEOM_QUADRANT_H

#include <geos/export.h>
#include <string>

#include <geos/inline.h>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace geom { // geos.geom

/** \brief
 * Utility functions for working with quadrants.
 *
 * The quadrants are numbered as follows:
 * <pre>
 * 1 | 0
 * --+--
 * 2 | 3
 * </pre>
 *
 */
class GEOS_DLL Quadrant {

public:

    static const int NE = 0;
    static const int NW = 1;
    static const int SW = 2;
    static const int SE = 3;

    /**
     * Returns the quadrant of a directed line segment
     * (specified as x and y displacements, which cannot both be 0).
     *
     * @throws IllegalArgumentException if the displacements are both 0
     */
    static int quadrant(double dx, double dy);

    /**
     * Returns the quadrant of a directed line segment from p0 to p1.
     *
     * @throws IllegalArgumentException if the points are equal
     */
    static int quadrant(const geom::Coordinate& p0,
                        const geom::Coordinate& p1);

    /**
     * Returns true if the quadrants are 1 and 3, or 2 and 4
     */
    static bool isOpposite(int quad1, int quad2);

    /*
     * Returns the right-hand quadrant of the halfplane defined by
     * the two quadrants,
     * or -1 if the quadrants are opposite, or the quadrant if they
     * are identical.
     */
    static int commonHalfPlane(int quad1, int quad2);

    /**
     * Returns whether the given quadrant lies within the given halfplane
     * (specified by its right-hand quadrant).
     */
    static bool isInHalfPlane(int quad, int halfPlane);

    /**
     * Returns true if the given quadrant is 0 or 1.
     */
    static bool isNorthern(int quad);
};


} // namespace geos.geom
} // namespace geos

#ifdef GEOS_INLINE
# include "geos/geom/Quadrant.inl"
#endif

#endif // ifndef GEOS_GEOM_QUADRANT_H

