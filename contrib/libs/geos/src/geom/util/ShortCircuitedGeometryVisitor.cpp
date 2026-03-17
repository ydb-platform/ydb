/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/util/ShortCircuitedGeometryVisitor.java rev. 1.1 (JTS-1.7)
 *
 **********************************************************************/


#include <geos/geom/util/ShortCircuitedGeometryVisitor.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryCollection.h>

using namespace geos::geom;

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

void
ShortCircuitedGeometryVisitor::applyTo(const Geometry& geom)
{
    for(std::size_t i = 0, n = geom.getNumGeometries(); i < n; ++i) {
        const Geometry* element = geom.getGeometryN(i);
        if (element == nullptr) {
            continue;
        }

        if(dynamic_cast<const GeometryCollection*>(element)) {
            applyTo(*element);
        }
        else {
            // calls the abstract virtual
            visit(*element);
            if(isDone()) {
                done = true;
            }
        }

        if(done) {
            return;
        }
    }
}


} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

