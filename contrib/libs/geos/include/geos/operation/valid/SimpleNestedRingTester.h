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
 * Last port: operation/valid/SimpleNestedRingTester.java rev. 1.14 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_SIMPLENESTEDRINGTESTER_H
#define GEOS_OP_SIMPLENESTEDRINGTESTER_H

#include <geos/export.h>

#include <cstddef>
#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class LinearRing;
}
namespace geomgraph {
class GeometryGraph;
}
}

namespace geos {
namespace operation { // geos::operation
namespace valid { // geos::operation::valid

/** \brief
 * Tests whether any of a set of [LinearRings](@ref geom::LinearRing) are
 * nested inside another ring in the set, using a simple O(n^2)
 * comparison.
 *
 */
class GEOS_DLL SimpleNestedRingTester {
private:
    geomgraph::GeometryGraph* graph;  // used to find non-node vertices
    std::vector<geom::LinearRing*> rings;
    geom::Coordinate* nestedPt;
public:
    SimpleNestedRingTester(geomgraph::GeometryGraph* newGraph)
        :
        graph(newGraph),
        rings(),
        nestedPt(nullptr)
    {}

    ~SimpleNestedRingTester()
    {
    }

    void
    add(geom::LinearRing* ring)
    {
        rings.push_back(ring);
    }

    /*
     * Be aware that the returned Coordinate (if != NULL)
     * will point to storage owned by one of the LinearRing
     * previously added. If you destroy them, this
     * will point to an invalid memory address.
     */
    geom::Coordinate*
    getNestedPoint()
    {
        return nestedPt;
    }

    bool isNonNested();
};

} // namespace geos.operation.valid
} // namespace geos.operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_SIMPLENESTEDRINGTESTER_H
