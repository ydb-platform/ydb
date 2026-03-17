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

#include <geos/operation/union/CascadedUnion.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/util/GeometryCombiner.h>
#include <geos/index/strtree/STRtree.h>
// std
#include <cassert>
#include <cstddef>
#include <memory>
#include <vector>

namespace geos {
namespace operation { // geos.operation
namespace geounion {  // geos.operation.geounion

geom::Geometry*
CascadedUnion::Union(std::vector<geom::Geometry*>* polys)
{
    CascadedUnion op(polys);
    return op.Union();
}

geom::Geometry*
CascadedUnion::Union()
{
    if(inputGeoms->empty()) {
        return nullptr;
    }

    geomFactory = inputGeoms->front()->getFactory();

    /*
     * A spatial index to organize the collection
     * into groups of close geometries.
     * This makes unioning more efficient, since vertices are more likely
     * to be eliminated on each round.
     */
    index::strtree::STRtree index(STRTREE_NODE_CAPACITY);

    typedef std::vector<geom::Geometry*>::const_iterator iterator_type;
    iterator_type end = inputGeoms->end();
    for(iterator_type i = inputGeoms->begin(); i != end; ++i) {
        geom::Geometry* g = *i;
        index.insert(g->getEnvelopeInternal(), g);
    }

    std::unique_ptr<index::strtree::ItemsList> itemTree(index.itemsTree());

    return unionTree(itemTree.get());
}

geom::Geometry*
CascadedUnion::unionTree(
    index::strtree::ItemsList* geomTree)
{
    /*
     * Recursively unions all subtrees in the list into single geometries.
     * The result is a list of Geometry's only
     */
    std::unique_ptr<GeometryListHolder> geoms(reduceToGeometries(geomTree));
    return binaryUnion(geoms.get());
}

geom::Geometry*
CascadedUnion::binaryUnion(GeometryListHolder* geoms)
{
    return binaryUnion(geoms, 0, geoms->size());
}

geom::Geometry*
CascadedUnion::binaryUnion(GeometryListHolder* geoms,
                           std::size_t start, std::size_t end)
{
    if(end - start <= 1) {
        return unionSafe(geoms->getGeometry(start), nullptr);
    }
    else if(end - start == 2) {
        return unionSafe(geoms->getGeometry(start), geoms->getGeometry(start + 1));
    }
    else {
        // recurse on both halves of the list
        std::size_t mid = (end + start) / 2;
        std::unique_ptr<geom::Geometry> g0(binaryUnion(geoms, start, mid));
        std::unique_ptr<geom::Geometry> g1(binaryUnion(geoms, mid, end));
        return unionSafe(g0.get(), g1.get());
    }
}

GeometryListHolder*
CascadedUnion::reduceToGeometries(index::strtree::ItemsList* geomTree)
{
    std::unique_ptr<GeometryListHolder> geoms(new GeometryListHolder());

    typedef index::strtree::ItemsList::iterator iterator_type;
    iterator_type end = geomTree->end();
    for(iterator_type i = geomTree->begin(); i != end; ++i) {
        if((*i).get_type() == index::strtree::ItemsListItem::item_is_list) {
            std::unique_ptr<geom::Geometry> geom(unionTree((*i).get_itemslist()));
            geoms->push_back_owned(geom.get());
            geom.release();
        }
        else if((*i).get_type() == index::strtree::ItemsListItem::item_is_geometry) {
            geoms->push_back(reinterpret_cast<geom::Geometry*>((*i).get_geometry()));
        }
        else {
            assert(!static_cast<bool>("should never be reached"));
        }
    }

    return geoms.release();
}

geom::Geometry*
CascadedUnion::unionSafe(geom::Geometry* g0, geom::Geometry* g1)
{
    if(g0 == nullptr && g1 == nullptr) {
        return nullptr;
    }

    if(g0 == nullptr) {
        return g1->clone().release();
    }
    if(g1 == nullptr) {
        return g0->clone().release();
    }

    return unionOptimized(g0, g1);
}

geom::Geometry*
CascadedUnion::unionOptimized(geom::Geometry* g0, geom::Geometry* g1)
{
    geom::Envelope const* g0Env = g0->getEnvelopeInternal();
    geom::Envelope const* g1Env = g1->getEnvelopeInternal();

    if(!g0Env->intersects(g1Env)) {
        return geom::util::GeometryCombiner::combine(g0, g1).release();
    }

    if(g0->getNumGeometries() <= 1 && g1->getNumGeometries() <= 1) {
        return unionActual(g0, g1);
    }

    geom::Envelope commonEnv;
    g0Env->intersection(*g1Env, commonEnv);
    return unionUsingEnvelopeIntersection(g0, g1, commonEnv);
}

geom::Geometry*
CascadedUnion::unionUsingEnvelopeIntersection(geom::Geometry* g0,
        geom::Geometry* g1, geom::Envelope const& common)
{
    std::vector<const geom::Geometry*> disjointPolys;

    std::unique_ptr<geom::Geometry> g0Int(extractByEnvelope(common, g0, disjointPolys));
    std::unique_ptr<geom::Geometry> g1Int(extractByEnvelope(common, g1, disjointPolys));

    std::unique_ptr<geom::Geometry> u(unionActual(g0Int.get(), g1Int.get()));
    disjointPolys.push_back(u.get());

    return geom::util::GeometryCombiner::combine(disjointPolys).release();
}

geom::Geometry*
CascadedUnion::extractByEnvelope(geom::Envelope const& env,
                                 geom::Geometry* geom, std::vector<const geom::Geometry*>& disjointGeoms)
{
    std::vector<const geom::Geometry*> intersectingGeoms;

    for(std::size_t i = 0; i < geom->getNumGeometries(); i++) {
        const geom::Geometry* elem = geom->getGeometryN(i);
        if(elem->getEnvelopeInternal()->intersects(env)) {
            intersectingGeoms.push_back(elem);
        }
        else {
            disjointGeoms.push_back(elem);
        }
    }

    return geomFactory->buildGeometry(intersectingGeoms);
}

geom::Geometry*
CascadedUnion::unionActual(geom::Geometry* g0, geom::Geometry* g1)
{
    return g0->Union(g1).release();
}

} // namespace geos.operation.union
} // namespace geos.operation
} // namespace geos
