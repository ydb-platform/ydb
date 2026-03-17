/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/MultiPolygon.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/geom/Geometry.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Dimension.h>

#include <cassert>
#include <string>
#include <vector>
#include <algorithm>

#ifndef GEOS_INLINE
# include "geos/geom/MultiPolygon.inl"
#endif

namespace geos {
namespace geom { // geos::geom

/*protected*/
MultiPolygon::MultiPolygon(std::vector<Geometry*>* newPolys, const GeometryFactory* factory)
      : GeometryCollection(newPolys, factory)
{}

MultiPolygon::MultiPolygon(std::vector<std::unique_ptr<Polygon>> && newPolys, const GeometryFactory& factory)
      : GeometryCollection(std::move(newPolys), factory)
{}

MultiPolygon::MultiPolygon(std::vector<std::unique_ptr<Geometry>> && newPolys, const GeometryFactory& factory)
        : GeometryCollection(std::move(newPolys), factory)
{}

MultiPolygon::~MultiPolygon() {}

Dimension::DimensionType
MultiPolygon::getDimension() const
{
    return Dimension::A; // area
}

int
MultiPolygon::getBoundaryDimension() const
{
    return 1;
}

std::string
MultiPolygon::getGeometryType() const
{
    return "MultiPolygon";
}

std::unique_ptr<Geometry>
MultiPolygon::getBoundary() const
{
    if(isEmpty()) {
        return std::unique_ptr<Geometry>(getFactory()->createMultiLineString());
    }

    std::vector<std::unique_ptr<Geometry>> allRings;
    for(const auto& pg : geometries) {
        auto g = pg->getBoundary();

        if(g->getNumGeometries() == 1) {
            allRings.push_back(std::move(g));
        } else {
            for(size_t i = 0; i < g->getNumGeometries(); ++i) {
                // TODO avoid this clone
                allRings.push_back(g->getGeometryN(i)->clone());
            }
        }
    }

    return getFactory()->createMultiLineString(std::move(allRings));
}

bool
MultiPolygon::equalsExact(const Geometry* other, double tolerance) const
{
    if(!isEquivalentClass(other)) {
        return false;
    }
    return GeometryCollection::equalsExact(other, tolerance);
}
GeometryTypeId
MultiPolygon::getGeometryTypeId() const
{
    return GEOS_MULTIPOLYGON;
}

std::unique_ptr<Geometry>
MultiPolygon::reverse() const
{
    if(isEmpty()) {
        return clone();
    }

    std::vector<std::unique_ptr<Geometry>> reversed(geometries.size());

    std::transform(geometries.begin(),
                   geometries.end(),
                   reversed.begin(),
    [](const std::unique_ptr<Geometry> & g) {
        return g->reverse();
    });

    return getFactory()->createMultiPolygon(std::move(reversed));
}

const Polygon*
MultiPolygon::getGeometryN(size_t i) const
{
    return static_cast<const Polygon*>(geometries[i].get());
}

} // namespace geos::geom
} // namespace geos
