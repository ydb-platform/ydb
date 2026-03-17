/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/valid/IndexedNestedRingTester.java r399 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_VALID_OFFSETCURVEVERTEXLIST_H
#define GEOS_OP_VALID_OFFSETCURVEVERTEXLIST_H

#include <cstddef>
#include <vector> // for composition

// Forward declarations
namespace geos {
namespace geom {
//class Envelope;
class Coordinate;
class LinearRing;
}
namespace index {
class SpatialIndex;
}
namespace geomgraph {
class GeometryGraph;
}
}

namespace geos {
namespace operation { // geos.operation
namespace valid { // geos.operation.valid

/** \brief
 * Tests whether any of a set of [LinearRings](@ref geom::LinearRing) are
 * nested inside another ring in the set, using a spatial
 * index to speed up the comparisons.
 *
 */
class IndexedNestedRingTester {
public:
    // @param newGraph : ownership retained by caller
    IndexedNestedRingTester(geomgraph::GeometryGraph* newGraph, size_t initialCapacity)
        :
        graph(newGraph),
        index(nullptr),
        nestedPt(nullptr)
    {
        rings.reserve(initialCapacity);
    }

    ~IndexedNestedRingTester();

    /*
     * Be aware that the returned Coordinate (if != NULL)
     * will point to storage owned by one of the LinearRing
     * previously added. If you destroy them, this
     * will point to an invalid memory address.
     */
    const geom::Coordinate*
    getNestedPoint() const
    {
        return nestedPt;
    }

    /// @param ring : ownership retained by caller
    void
    add(const geom::LinearRing* ring)
    {
        rings.push_back(ring);
    }

    bool isNonNested();

private:

    /// Externally owned
    geomgraph::GeometryGraph* graph;

    /// Ownership of this vector elements are externally owned
    std::vector<const geom::LinearRing*> rings;

    // Owned by us (use unique_ptr ?)
    geos::index::SpatialIndex* index; // 'index' in JTS

    // Externally owned, if not null
    const geom::Coordinate* nestedPt;

    void buildIndex();
};

} // namespace geos.operation.valid
} // namespace geos.operation
} // namespace geos

#endif // GEOS_OP_VALID_OFFSETCURVEVERTEXLIST_H
