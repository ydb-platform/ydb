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

#ifndef GEOS_GEOM_UTIL_SHORTCIRCUITEDGEOMETRYVISITOR_H
#define GEOS_GEOM_UTIL_SHORTCIRCUITEDGEOMETRYVISITOR_H

#include <geos/export.h>

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
}
}


namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

/** \brief
 * A visitor to Geometry elements which can
 * be short-circuited by a given condition
 *
 * Last port: geom/util/ShortCircuitedGeometryVisitor.java rev. 1.1 (JTS-1.7)
 */
class GEOS_DLL ShortCircuitedGeometryVisitor {

private:

    bool done;

protected:

    virtual void visit(const Geometry& element) = 0;
    virtual bool isDone() = 0;

public:

    ShortCircuitedGeometryVisitor()
        :
        done(false)
    {}

    void applyTo(const Geometry& geom);

    virtual
    ~ShortCircuitedGeometryVisitor() {}

};

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#endif
