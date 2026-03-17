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

#ifndef GEOS_GEOM_GEOMETRYCOMPONENTFILTER_H
#define GEOS_GEOM_GEOMETRYCOMPONENTFILTER_H

#include <geos/export.h>
#include <geos/inline.h>

namespace geos {
namespace geom { // geos::geom
class Geometry;
}
}

namespace geos {
namespace geom { // geos::geom

/**
 *  <code>Geometry</code> classes support the concept of applying
 *  a <code>GeometryComponentFilter</code>
 *  filter to the <code>Geometry</code>.
 *  The filter is applied to every component of the <code>Geometry</code>
 *  which is itself a <code>Geometry</code>.
 *  A <code>GeometryComponentFilter</code> filter can either
 *  record information about the <code>Geometry</code>
 *  or change the <code>Geometry</code> in some way.
 *  <code>GeometryComponentFilter</code>
 *  is an example of the Gang-of-Four Visitor pattern.
 *
 */
class GEOS_DLL GeometryComponentFilter {
public:

    /**
     *  Performs an operation with or on <code>geom</code>.
     *
     * @param  geom  a <code>Geometry</code> to which the filter
     * is applied.
     */
    virtual void filter_rw(Geometry* geom);
    virtual void filter_ro(const Geometry* geom);

    virtual bool isDone() { return false; }

    virtual
    ~GeometryComponentFilter() {}
};

} // namespace geos::geom
} // namespace geos

#endif // ndef GEOS_GEOM_GEOMETRYCOMPONENTFILTER_H
