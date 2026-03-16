/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006-2011 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/util/GeometryCombiner.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/geom/util/GeometryCombiner.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/GeometryCollection.h>

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

std::unique_ptr<Geometry>
GeometryCombiner::combine(std::vector<std::unique_ptr<Geometry>> const& geoms)
{
    std::vector<const Geometry*> geomptrs;
    for(const auto& g : geoms) {
        geomptrs.push_back(g.get());
    }
    GeometryCombiner combiner(geomptrs);
    return combiner.combine();
}

std::unique_ptr<Geometry>
GeometryCombiner::combine(std::vector<const Geometry*> const& geoms)
{
    GeometryCombiner combiner(geoms);
    return combiner.combine();
}

std::unique_ptr<Geometry>
GeometryCombiner::combine(const Geometry* g0, const Geometry* g1)
{
    std::vector<const Geometry*> geoms;
    geoms.push_back(g0);
    geoms.push_back(g1);

    GeometryCombiner combiner(geoms);
    return combiner.combine();
}

std::unique_ptr<Geometry>
GeometryCombiner::combine(const Geometry* g0, const Geometry* g1,
                          const Geometry* g2)
{
    std::vector<const Geometry*> geoms;
    geoms.push_back(g0);
    geoms.push_back(g1);
    geoms.push_back(g2);

    GeometryCombiner combiner(geoms);
    return combiner.combine();
}

GeometryCombiner::GeometryCombiner(std::vector<const Geometry*> const& geoms)
    : geomFactory(extractFactory(geoms)), skipEmpty(false), inputGeoms(geoms)
{
}

GeometryFactory const*
GeometryCombiner::extractFactory(std::vector<const Geometry*> const& geoms)
{
    return geoms.empty() ? nullptr : geoms.front()->getFactory();
}

std::unique_ptr<Geometry>
GeometryCombiner::combine()
{
    std::vector<const Geometry*> elems;

    for(const auto& geom : inputGeoms) {
        extractElements(geom, elems);
    }

    if(elems.empty()) {
        if(geomFactory != nullptr) {
            return std::unique_ptr<Geometry>(geomFactory->createGeometryCollection());
        }
        return nullptr;
    }

    // return the "simplest possible" geometry
    return std::unique_ptr<Geometry>(geomFactory->buildGeometry(elems));
}

void
GeometryCombiner::extractElements(const Geometry* geom, std::vector<const Geometry*>& elems)
{
    if(geom == nullptr) {
        return;
    }

    for(std::size_t i = 0; i < geom->getNumGeometries(); ++i) {
        const Geometry* elemGeom = geom->getGeometryN(i);
        if(skipEmpty && elemGeom->isEmpty()) {
            continue;
        }
        elems.push_back(elemGeom);
    }
}

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

