/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/union/CascadedPolygonUnion.java r487 (JTS-1.12+)
 * Includes custom code to deal with https://trac.osgeo.org/geos/ticket/837
 *
 **********************************************************************/


#include <geos/operation/union/CascadedPolygonUnion.h>
#include <geos/operation/union/OverlapUnion.h>
#include <geos/operation/overlay/OverlayOp.h>
#include <geos/geom/HeuristicOverlay.h>
#include <geos/geom/Dimension.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/util/PolygonExtracter.h>
#include <geos/index/strtree/STRtree.h>

// std
#include <cassert>
#include <cstddef>
#include <memory>
#include <vector>
#include <sstream>

#include <geos/operation/valid/IsValidOp.h>
#include <geos/operation/IsSimpleOp.h>
#include <geos/algorithm/BoundaryNodeRule.h>
#include <geos/util/TopologyException.h>
#include <string>
#include <iomanip>

//#define GEOS_DEBUG_CASCADED_UNION 1
//#define GEOS_DEBUG_CASCADED_UNION_PRINT_INVALID 1

namespace {

#if GEOS_DEBUG
inline bool
check_valid(const geos::geom::Geometry& g, const std::string& label, bool doThrow = false, bool validOnly = false)
{
    using namespace geos;

    if(g.isLineal()) {
        if(! validOnly) {
            operation::IsSimpleOp sop(g, algorithm::BoundaryNodeRule::getBoundaryEndPoint());
            if(! sop.isSimple()) {
                if(doThrow) {
                    throw geos::util::TopologyException(
                        label + " is not simple");
                }
                return false;
            }
        }
    }
    else {
        operation::valid::IsValidOp ivo(&g);
        if(! ivo.isValid()) {
            using operation::valid::TopologyValidationError;
            TopologyValidationError* err = ivo.getValidationError();
#ifdef GEOS_DEBUG_CASCADED_UNION
            std::cerr << label << " is INVALID: "
                      << err->toString()
                      << " (" << std::setprecision(20)
                      << err->getCoordinate() << ")"
                      << std::endl
#ifdef GEOS_DEBUG_CASCADED_UNION_PRINT_INVALID
                      << "<a>" << std::endl
                      << g.toString() << std::endl
                      << "</a>" << std::endl
#endif
                      ;
#endif // GEOS_DEBUG_CASCADED_UNION
            if(doThrow) {
                throw geos::util::TopologyException(
                    label + " is invalid: " + err->toString(),
                    err->getCoordinate());
            }
            return false;
        }
    }
    return true;
}
#endif

} // anonymous namespace


namespace geos {
namespace operation { // geos.operation
namespace geounion {  // geos.operation.geounion

// ////////////////////////////////////////////////////////////////////////////
void
GeometryListHolder::deleteItem(geom::Geometry* item)
{
    delete item;
}

// ////////////////////////////////////////////////////////////////////////////
geom::Geometry*
CascadedPolygonUnion::Union(std::vector<geom::Polygon*>* polys)
{
    CascadedPolygonUnion op(polys);
    return op.Union();
}

geom::Geometry*
CascadedPolygonUnion::Union(std::vector<geom::Polygon*>* polys, UnionStrategy* unionFun)
{
    CascadedPolygonUnion op(polys, unionFun);
    return op.Union();
}

geom::Geometry*
CascadedPolygonUnion::Union(const geom::MultiPolygon* multipoly)
{
    std::vector<geom::Polygon*> polys;

    for(const auto& g : *multipoly) {
        polys.push_back(dynamic_cast<geom::Polygon*>(g.get()));
    }

    CascadedPolygonUnion op(&polys);
    return op.Union();
}

geom::Geometry*
CascadedPolygonUnion::Union()
{
    if(inputPolys->empty()) {
        return nullptr;
    }

    geomFactory = inputPolys->front()->getFactory();

    /*
     * A spatial index to organize the collection
     * into groups of close geometries.
     * This makes unioning more efficient, since vertices are more likely
     * to be eliminated on each round.
     */

    index::strtree::STRtree index(STRTREE_NODE_CAPACITY);

    typedef std::vector<geom::Polygon*>::iterator iterator_type;
    iterator_type end = inputPolys->end();
    for(iterator_type i = inputPolys->begin(); i != end; ++i) {
        geom::Geometry* g = dynamic_cast<geom::Geometry*>(*i);
        index.insert(g->getEnvelopeInternal(), g);
    }

    std::unique_ptr<index::strtree::ItemsList> itemTree(index.itemsTree());

    return unionTree(itemTree.get());
}

geom::Geometry*
CascadedPolygonUnion::unionTree(
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
CascadedPolygonUnion::binaryUnion(GeometryListHolder* geoms)
{
    return binaryUnion(geoms, 0, geoms->size());
}

geom::Geometry*
CascadedPolygonUnion::binaryUnion(GeometryListHolder* geoms,
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
CascadedPolygonUnion::reduceToGeometries(index::strtree::ItemsList* geomTree)
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
CascadedPolygonUnion::unionSafe(geom::Geometry* g0, geom::Geometry* g1)
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

    return unionActual(g0, g1);
}

// geom::Geometry*
// CascadedPolygonUnion::unionActual(geom::Geometry* g0, geom::Geometry* g1)
// {
//     OverlapUnion unionOp(g0, g1);
//     geom::Geometry* justPolys = restrictToPolygons(
//         std::unique_ptr<geom::Geometry>(unionOp.doUnion())
//         ).release();
//     return justPolys;
// }

geom::Geometry*
CascadedPolygonUnion::unionActual(geom::Geometry* g0, geom::Geometry* g1)
{
    std::unique_ptr<geom::Geometry> ug;
    ug = unionFunction->Union(g0, g1);
    return restrictToPolygons(std::move(ug)).release();
}

std::unique_ptr<geom::Geometry>
CascadedPolygonUnion::restrictToPolygons(std::unique_ptr<geom::Geometry> g)
{
    using namespace geom;
    using namespace std;

    if(g->isPolygonal()) {
        return g;
    }

    Polygon::ConstVect polygons;
    geom::util::PolygonExtracter::getPolygons(*g, polygons);

    if(polygons.size() == 1) {
        return std::unique_ptr<Geometry>(polygons[0]->clone());
    }

    typedef vector<Geometry*> GeomVect;

    Polygon::ConstVect::size_type n = polygons.size();
    GeomVect* newpolys = new GeomVect(n);
    for(Polygon::ConstVect::size_type i = 0; i < n; ++i) {
        (*newpolys)[i] = polygons[i]->clone().release();
    }
    return unique_ptr<Geometry>(g->getFactory()->createMultiPolygon(newpolys));
}

/************************************************************************/

using operation::overlay::OverlayOp;

std::unique_ptr<geom::Geometry>
ClassicUnionStrategy::Union(const geom::Geometry* g0, const geom::Geometry* g1)
{
    try {
        // return SnapIfNeededOverlayOp.union(g0, g1);
        return geom::HeuristicOverlay(g0, g1, overlay::OverlayOp::opUNION);
    }
    catch (const util::TopologyException &ex) {
        // union-by-buffer only works for polygons
        if (g0->getDimension() != 2 || g1->getDimension() != 2)
          throw ex;
        return unionPolygonsByBuffer(g0, g1);
    }
}

bool
ClassicUnionStrategy::isFloatingPrecision() const
{
  return true;
}

/*private*/
std::unique_ptr<geom::Geometry>
ClassicUnionStrategy::unionPolygonsByBuffer(const geom::Geometry* g0, const geom::Geometry* g1)
{
    std::vector<std::unique_ptr<geom::Geometry>> geoms;
    geoms.push_back(g0->clone());
    geoms.push_back(g1->clone());
    std::unique_ptr<geom::GeometryCollection> coll = g0->getFactory()->createGeometryCollection(std::move(geoms));
    return coll->buffer(0);
}





} // namespace geos.operation.union
} // namespace geos.operation
} // namespace geos
