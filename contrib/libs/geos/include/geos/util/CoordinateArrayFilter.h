/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_UTIL_COORDINATEARRAYFILTER_H
#define GEOS_UTIL_COORDINATEARRAYFILTER_H

#include <geos/export.h>
#include <cassert>

#include <geos/geom/CoordinateFilter.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Coordinate.h>

namespace geos {
namespace util { // geos::util

/** \brief
 * A CoordinateFilter that adds read-only pointers
 * to every Coordinate in a Geometry to a given
 * vector.
 *
 * Last port: util/CoordinateArrayFilter.java rev. 1.15
 */
class GEOS_DLL CoordinateArrayFilter: public geom::CoordinateFilter {
private:
    geom::Coordinate::ConstVect& pts; // target vector reference
public:
    /** \brief
     * Constructs a CoordinateArrayFilter.
     *
     * @param target The destination vector.
     */
    CoordinateArrayFilter(geom::Coordinate::ConstVect& target)
        :
        pts(target)
    {}

    virtual
    ~CoordinateArrayFilter() {}

    virtual void
    filter_ro(const geom::Coordinate* coord)
    {
        pts.push_back(coord);
    }
};


} // namespace geos.util
} // namespace geos

#endif // GEOS_UTIL_COORDINATEARRAYFILTER_H
