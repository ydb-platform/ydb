/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/util/GeometryEditor.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/geom/util/GeometryEditor.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/util/GeometryEditorOperation.h>
#include <geos/util/UnsupportedOperationException.h>
#include <geos/util.h>

#include <vector>
#include <cassert>
#include <typeinfo>

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

/**
 * Creates a new GeometryEditor object which will create
 * an edited Geometry with the same GeometryFactory as the input Geometry.
 */
GeometryEditor::GeometryEditor()
{
    factory = nullptr;
}

/**
 * Creates a new GeometryEditor object which will create
 * the edited Geometry with the given {@link GeometryFactory}
 *
 * @param factory the GeometryFactory to create the edited Geometry with
 */
GeometryEditor::GeometryEditor(const GeometryFactory* newFactory)
{
    factory = newFactory;
}

/**
 * Edit the input {@link Geometry} with the given edit operation.
 * Clients will create subclasses of GeometryEditorOperation or
 * CoordinateOperation to perform required modifications.
 *
 * @param geometry the Geometry to edit
 * @param operation the edit operation to carry out
 * @return a new {@link Geometry} which is the result of the editing
 */
std::unique_ptr<Geometry>
GeometryEditor::edit(const Geometry* geometry, GeometryEditorOperation* operation)
{
    // if client did not supply a GeometryFactory, use the one from the input Geometry
    if(factory == nullptr) {
        factory = geometry->getFactory();
    }

    if(const GeometryCollection* gc =
                dynamic_cast<const GeometryCollection*>(geometry)) {
        return editGeometryCollection(gc, operation);
    }

    if(const Polygon* p = dynamic_cast<const Polygon*>(geometry)) {
        return editPolygon(p, operation);
    }

    if(dynamic_cast<const Point*>(geometry)) {
        return operation->edit(geometry, factory);
    }

    if(dynamic_cast<const LineString*>(geometry)) {
        return operation->edit(geometry, factory);
    }

    // Unsupported Geometry classes should be caught in the GeometryEditorOperation.
    assert(!static_cast<bool>("SHOULD NEVER GET HERE"));
    return nullptr;
}

std::unique_ptr<Polygon>
GeometryEditor::editPolygon(const Polygon* polygon, GeometryEditorOperation* operation)
{
    std::unique_ptr<Polygon> newPolygon(dynamic_cast<Polygon*>(
                                                operation->edit(polygon, factory).release()
                                        ));
    if(newPolygon->isEmpty()) {
        //RemoveSelectedPlugIn relies on this behaviour. [Jon Aquino]
        if(newPolygon->getFactory() != factory) {
            std::unique_ptr<Polygon> ret(factory->createPolygon(polygon->getCoordinateDimension()));
            return ret;
        }
        else {
            return newPolygon;
        }
    }

    std::unique_ptr<LinearRing> shell(dynamic_cast<LinearRing*>(
            edit(newPolygon->getExteriorRing(), operation).release()));

    if(shell->isEmpty()) {
        //RemoveSelectedPlugIn relies on this behaviour. [Jon Aquino]
        return std::unique_ptr<Polygon>(factory->createPolygon(polygon->getCoordinateDimension()));
    }

    auto holes = detail::make_unique<std::vector<LinearRing*>>();
    for(size_t i = 0, n = newPolygon->getNumInteriorRing(); i < n; ++i) {

        std::unique_ptr<LinearRing> hole(dynamic_cast<LinearRing*>(
                edit(newPolygon->getInteriorRingN(i), operation).release()));

        assert(hole);

        if(hole->isEmpty()) {
            continue;
        }
        holes->push_back(hole.release());
    }

    return std::unique_ptr<Polygon>(factory->createPolygon(shell.release(), holes.release()));
}

std::unique_ptr<GeometryCollection>
GeometryEditor::editGeometryCollection(const GeometryCollection* collection, GeometryEditorOperation* operation)
{
    auto newCollection = operation->edit(collection, factory);
    std::vector<std::unique_ptr<Geometry>> geometries;
    for(std::size_t i = 0, n = newCollection->getNumGeometries(); i < n; i++) {
        auto geometry = edit(newCollection->getGeometryN(i),
                                  operation);
        if(geometry->isEmpty()) {
            continue;
        }
        geometries.push_back(std::move(geometry));
    }

    if(newCollection->getGeometryTypeId() == GEOS_MULTIPOINT) {
        return factory->createMultiPoint(std::move(geometries));
    }
    else if(newCollection->getGeometryTypeId() == GEOS_MULTILINESTRING) {
        return factory->createMultiLineString(std::move(geometries));
    }
    else if(newCollection->getGeometryTypeId() == GEOS_MULTIPOLYGON) {
        return factory->createMultiPolygon(std::move(geometries));
    }
    else {
        return factory->createGeometryCollection(std::move(geometries));
    }
}

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos
