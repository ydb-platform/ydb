/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 Refractions Research Inc.
 * Copyright (C) 2010 Safe Software Inc.
 * Copyright (C) 2010 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2019 Daniel Baston <dbaston@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_OP_VALID_INDEXEDNESTEDSHELLTESTER_H
#define GEOS_OP_VALID_INDEXEDNESTEDSHELLTESTER_H

#include <geos/geom/Polygon.h>
#include <geos/index/SpatialIndex.h>

#include <cstddef>
#include <memory>

// Forward declarations
namespace geos {
namespace algorithm {
namespace locate {
    class IndexedPointInAreaLocator;
}
}
namespace geom {
    class Polygon;
}
namespace geomgraph {
    class GeometryGraph;
}
namespace operation {
namespace valid {
    class PolygonIndexedLocators;
}
}
}

namespace geos {
namespace operation {
namespace valid {

class IndexedNestedShellTester {

public:
    IndexedNestedShellTester(const geomgraph::GeometryGraph& g, size_t initialCapacity);

    void add(const geom::Polygon& p) {
        polys.push_back(&p);
    }

    const geom::Coordinate* getNestedPoint();

    bool isNonNested();

private:
    void compute();

    /**
     * Check if a shell is incorrectly nested within a polygon.
     * This is the case if the shell is inside the polygon shell,
     * but not inside a polygon hole.
     * (If the shell is inside a polygon hole, the nesting is valid.)
     *
     * The algorithm used relies on the fact that the rings must be
     * properly contained.
     * E.g. they cannot partially overlap (this has been previously
     * checked by <code>checkRelateConsistency</code>
     */
    void checkShellNotNested(const geom::LinearRing* shell, PolygonIndexedLocators & locs);

    /**
     * This routine checks to see if a shell is properly contained
     * in a hole.
     * It assumes that the edges of the shell and hole do not
     * properly intersect.
     *
     * @return <code>null</code> if the shell is properly contained, or
     *   a Coordinate which is not inside the hole if it is not
     */
    const geom::Coordinate* checkShellInsideHole(const geom::LinearRing* shell,
            algorithm::locate::IndexedPointInAreaLocator & holeLoc);

    /// Externally owned
    const geomgraph::GeometryGraph& graph;

    std::vector<const geom::Polygon*> polys;

    // Externally owned, if not null
    const geom::Coordinate* nestedPt;

    bool processed;
};

}
}
}

#endif //GEOS_INDEXEDNESTEDSHELLTESTER_H
