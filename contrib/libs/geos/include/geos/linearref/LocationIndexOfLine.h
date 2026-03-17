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
 * Last port: linearref/LocationIndexOfLine.java r731
 *
 **********************************************************************/

#ifndef GEOS_LINEARREF_LOCATIONINDEXOFLINE_H
#define GEOS_LINEARREF_LOCATIONINDEXOFLINE_H

#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/linearref/LinearLocation.h>

namespace geos {
namespace linearref { // geos::linearref

/** \brief
 * Determines the location of a subline along a linear [Geometry](@ref geom::Geometry).
 *
 * The location is reported as a pair of {@link LinearLocation}s.
 *
 * @note Currently this algorithm is not guaranteed to
 *       return the correct substring in some situations where
 *       an endpoint of the test line occurs more than once in the input line.
 *       (However, the common case of a ring is always handled correctly).
 */
class LocationIndexOfLine {
    /**
     * MD - this algorithm has been extracted into a class
     * because it is intended to validate that the subline truly is a subline,
     * and also to use the internal vertex information to unambiguously locate the subline.
     */
private:
    const geom::Geometry* linearGeom;

public:

    /** \brief
     * Determines the location of a subline along a linear [Geometry](@ref geom::Geometry).
     *
     * The location is reported as a pair of [LinearLocations](@ref LinearLocation).
     *
     * @note Currently this algorithm is not guaranteed to
     *       return the correct substring in some situations where
     *       an endpoint of the test line occurs more than once in the input line.
     *       (However, the common case of a ring is always handled correctly).
     *
     * @note Caller must take of releasing with delete[]
     *
     */
    static LinearLocation* indicesOf(const geom::Geometry* linearGeom, const geom::Geometry* subLine);

    LocationIndexOfLine(const geom::Geometry* linearGeom);

    /// Caller must take of releasing with delete[]
    LinearLocation* indicesOf(const geom::Geometry* subLine) const;
};
}
}

#endif
