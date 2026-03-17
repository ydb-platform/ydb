/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://trac.osgeo.org/geos
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: ORIGINAL WORK, generalization of CascadedPolygonUnion
 *
 **********************************************************************/

#ifndef GEOS_OP_UNION_CASCADEDUNION_H
#define GEOS_OP_UNION_CASCADEDUNION_H

#include <geos/export.h>

#include <vector>
#include <algorithm>

#include "GeometryListHolder.h"

// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class Geometry;
class Geometry;
class Envelope;
}
namespace index {
namespace strtree {
class ItemsList;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace geounion {  // geos::operation::geounion

/**
 * \brief
 * Provides an efficient method of unioning a collection of Geometries
 *
 * This algorithm is more robust than the simple iterated approach
 * of repeatedly unioning each geometry to a result geometry.
 */
class GEOS_DLL CascadedUnion {
private:
    const std::vector<geom::Geometry*>* inputGeoms;
    geom::GeometryFactory const* geomFactory;

    /**
     * The effectiveness of the index is somewhat sensitive
     * to the node capacity.
     * Testing indicates that a smaller capacity is better.
     * For an STRtree, 4 is probably a good number (since
     * this produces 2x2 "squares").
     */
    static int const STRTREE_NODE_CAPACITY = 4;

public:
    CascadedUnion();

    /**
     * Computes the union of a collection of {@link geom::Geometry}s.
     *
     * @param geoms a collection of {@link geom::Geometry}s.
     *        ownership of elements _and_ vector are left to caller.
     */
    static geom::Geometry* Union(std::vector<geom::Geometry*>* geoms);

    /**
     * Computes the union of a set of {@link geom::Geometry}s.
     *
     * @tparam T an iterator yelding something castable to const Geometry *
     * @param start start iterator
     * @param end end iterator
     */
    template <class T>
    static geom::Geometry*
    Union(T start, T end)
    {
        std::vector<geom::Geometry*> polys;
        for(T i = start; i != end; ++i) {
            const geom::Geometry* p = dynamic_cast<const geom::Geometry*>(*i);
            polys.push_back(const_cast<geom::Geometry*>(p));
        }
        return Union(&polys);
    }

    /**
     * Creates a new instance to union
     * the given collection of {@link geom::Geometry}s.
     *
     * @param geoms a collection of {@link geom::Geometry}s.
     *              Ownership of elements _and_ vector are left to caller.
     */
    CascadedUnion(const std::vector<geom::Geometry*>* geoms)
        : inputGeoms(geoms),
          geomFactory(nullptr)
    {}

    /**
     * Computes the union of the input geometries.
     *
     * @return the union of the input geometries
     * @return null if no input geometries were provided
     */
    geom::Geometry* Union();

private:
    geom::Geometry* unionTree(index::strtree::ItemsList* geomTree);

    /**
     * Unions a list of geometries
     * by treating the list as a flattened binary tree,
     * and performing a cascaded union on the tree.
     */
    geom::Geometry* binaryUnion(GeometryListHolder* geoms);

    /**
     * Unions a section of a list using a recursive binary union on each half
     * of the section.
     *
     * @param geoms
     * @param start
     * @param end
     * @return the union of the list section
     */
    geom::Geometry* binaryUnion(GeometryListHolder* geoms, std::size_t start,
                                std::size_t end);

    /**
     * Reduces a tree of geometries to a list of geometries
     * by recursively unioning the subtrees in the list.
     *
     * @param geomTree a tree-structured list of geometries
     * @return a list of Geometrys
     */
    GeometryListHolder* reduceToGeometries(index::strtree::ItemsList* geomTree);

    /**
     * Computes the union of two geometries,
     * either of both of which may be null.
     *
     * @param g0 a Geometry
     * @param g1 a Geometry
     * @return the union of the input(s)
     * @return null if both inputs are null
     */
    geom::Geometry* unionSafe(geom::Geometry* g0, geom::Geometry* g1);

    geom::Geometry* unionOptimized(geom::Geometry* g0, geom::Geometry* g1);

    /**
     * Unions two geometries.
     * The case of multi geometries is optimized to union only
     * the components which lie in the intersection of the two geometry's
     * envelopes.
     * Geometrys outside this region can simply be combined with the union
     * result, which is potentially much faster.
     * This case is likely to occur often during cascaded union, and may also
     * occur in real world data (such as unioning data for parcels on
     * different street blocks).
     *
     * @param g0 a geometry
     * @param g1 a geometry
     * @param common the intersection of the envelopes of the inputs
     * @return the union of the inputs
     */
    geom::Geometry* unionUsingEnvelopeIntersection(geom::Geometry* g0,
            geom::Geometry* g1, geom::Envelope const& common);

    geom::Geometry* extractByEnvelope(geom::Envelope const& env,
                                      geom::Geometry* geom, std::vector<const geom::Geometry*>& disjointGeoms);

    /**
     * Encapsulates the actual unioning of two polygonal geometries.
     *
     * @param g0
     * @param g1
     * @return
     */
    static geom::Geometry* unionActual(geom::Geometry* g0, geom::Geometry* g1);
};

} // namespace geos::operation::union
} // namespace geos::operation
} // namespace geos

#endif
