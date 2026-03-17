/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/


#pragma once

#include <geos/edgegraph/EdgeGraph.h>

#include <geos/export.h>
#include <string>
#include <cassert>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class LineString;
class Geometry;
class GeometryCollection;
}
}

namespace geos {
namespace edgegraph { // geos.edgegraph


/**
 * Builds an edge graph from geometries containing edges.
 *
 * @author mdavis
 *
 */
class GEOS_DLL EdgeGraphBuilder {

private:

    /* members */
    std::unique_ptr<EdgeGraph> graph;



public:

    EdgeGraphBuilder() :
        graph(new EdgeGraph())
        {};

    static std::unique_ptr<EdgeGraph> build(const geom::GeometryCollection* geoms);

    std::unique_ptr<EdgeGraph> getGraph();

    /**
    * Adds the edges of a Geometry to the graph.
    * May be called multiple times.
    * Any dimension of Geometry may be added; the constituent edges are
    * extracted.
    *
    * @param geometry geometry to be added
    */
    void add(const geom::Geometry* geometry);
    void add(const geom::LineString* linestring);

    /**
    * Adds the edges in a collection of {@link geom::Geometry} to the graph.
    * May be called multiple times.
    * Any dimension of Geometry may be added.
    *
    * @param geometries the geometries to be added
    */
    void add(const geom::GeometryCollection* geometries);

};


} // namespace geos.edgegraph
} // namespace geos




