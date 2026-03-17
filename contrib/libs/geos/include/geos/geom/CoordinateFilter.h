/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_GEOM_COORDINATEFILTER_H
#define GEOS_GEOM_COORDINATEFILTER_H

#include <geos/export.h>
#include <geos/inline.h>

#include <cassert>

namespace geos {
namespace geom { // geos::geom

class Coordinate;

/** \brief
 * Geometry classes support the concept of applying a
 * coordinate filter to every coordinate in the Geometry.
 *
 * A  coordinate filter can either record information about each coordinate or
 * change the coordinate in some way. Coordinate filters implement the
 * interface <code>CoordinateFilter</code>. (<code>CoordinateFilter</code> is
 * an example of the Gang-of-Four Visitor pattern). Coordinate filters can be
 * used to implement such things as coordinate transformations, centroid and
 * envelope computation, and many other functions.
 *
 * TODO: provide geom::CoordinateInspector and geom::CoordinateMutator instead
 * of having the two versions of filter_rw and filter_ro
 *
 */
class GEOS_DLL CoordinateFilter {
public:
    virtual
    ~CoordinateFilter() {}

    /** \brief
     * Performs an operation on `coord`.
     *
     * **param** `coord` a Coordinate to which the filter is applied.
     */
    virtual void
    filter_rw(Coordinate* /*coord*/) const
    {
        assert(0);
    }

    /** \brief
     * Performs an operation with `coord`.
     *
     * **param** `coord`  a Coordinate to which the filter is applied.
     */
    virtual void
    filter_ro(const Coordinate* /*coord*/)
    {
        assert(0);
    }
};

} // namespace geos::geom
} // namespace geos

#endif // ndef GEOS_GEOM_COORDINATEFILTER_H
