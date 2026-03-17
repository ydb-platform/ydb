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
 * Last port: operation/valid/QuadtreeNestedRingTester.java rev. 1.12 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_QUADTREENESTEDRINGTESTER_H
#define GEOS_OP_QUADTREENESTEDRINGTESTER_H

#include <geos/export.h>

#include <geos/geom/Envelope.h> // for composition

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class LinearRing;
class Coordinate;
}
namespace index {
namespace quadtree {
class Quadtree;
}
}
namespace geomgraph {
class GeometryGraph;
}
}

namespace geos {
namespace operation { // geos::operation
namespace valid { // geos::operation::valid

/** \brief
 * Tests whether any of a set of [LinearRings](@ref geom::LinearRing) are nested
 * inside another ring in the set, using a [Quadtree](@ref index::quadtree::Quadtree)
 * index to speed up the comparisons.
 *
 */
class GEOS_DLL QuadtreeNestedRingTester {
public:

    /// Caller retains ownership of GeometryGraph
    QuadtreeNestedRingTester(geomgraph::GeometryGraph* newGraph);

    ~QuadtreeNestedRingTester();

    /*
     * Be aware that the returned Coordinate (if != NULL)
     * will point to storage owned by one of the LinearRing
     * previously added. If you destroy them, this
     * will point to an invalid memory address.
     */
    geom::Coordinate* getNestedPoint();

    void add(const geom::LinearRing* ring);

    bool isNonNested();

private:

    geomgraph::GeometryGraph* graph;  // used to find non-node vertices

    std::vector<const geom::LinearRing*> rings;

    geom::Envelope totalEnv;

    index::quadtree::Quadtree* qt;

    geom::Coordinate* nestedPt;

    void buildQuadtree();
};

} // namespace geos::operation::valid
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_QUADTREENESTEDRINGTESTER_H
