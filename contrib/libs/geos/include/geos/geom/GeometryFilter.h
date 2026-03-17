/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_GEOM_GEOMETRYFILTER_H
#define GEOS_GEOM_GEOMETRYFILTER_H

#include <geos/export.h>
#include <geos/inline.h>

#include <string>
#include <vector>
#include <cassert>

namespace geos {
namespace geom { // geos::geom
class Geometry;
}
}

namespace geos {
namespace geom { // geos::geom


/** \brief
 * Geometry classes support the concept of applying a Geometry
 * filter to the Geometry.
 *
 * In the case of GeometryCollection
 * subclasses, the filter is applied to every element Geometry.
 * A Geometry filter can either record information about the Geometry
 * or change the Geometry in some way.
 * Geometry filters implement the interface GeometryFilter.
 * (GeometryFilter is an example of the Gang-of-Four Visitor pattern).
 */
class GEOS_DLL GeometryFilter {
public:
    /*
     * Performs an operation with or on <code>geom</code>.
     *
     * @param  geom  a <code>Geometry</code> to which the filter
     *         is applied.
     *
     * NOTE: this are not pure abstract to allow read-only
     * or read-write-only filters to avoid defining a fake
     * version of the not-implemented kind.
     */
    virtual void
    filter_ro(const Geometry* /*geom*/)
    {
        assert(0);
    }
    virtual void
    filter_rw(Geometry* /*geom*/)
    {
        assert(0);
    }

    virtual
    ~GeometryFilter() {}
};

} // namespace geos::geom
} // namespace geos

#endif // ndef GEOS_GEOM_GEOMETRYFILTER_H
