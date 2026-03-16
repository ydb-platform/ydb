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

#ifdef _MSC_VER
#pragma warning(disable:4355)
#endif

#include <memory>

#include <geos/edgegraph/EdgeGraphBuilder.h>
#include <geos/edgegraph/EdgeGraph.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/LineString.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/Geometry.h>


using namespace geos::geom;

namespace geos {
namespace edgegraph { // geos.edgegraph

/*public static*/
std::unique_ptr<EdgeGraph>
EdgeGraphBuilder::build(const GeometryCollection* geoms)
{
    EdgeGraphBuilder builder;
    builder.add(geoms);
    return builder.getGraph();
}

/*public*/
std::unique_ptr<EdgeGraph>
EdgeGraphBuilder::getGraph()
{
    return std::move(graph);
}

struct EdgeGraphLinestringFilter: public GeometryComponentFilter {

    EdgeGraphBuilder* egb;
    EdgeGraphLinestringFilter(EdgeGraphBuilder* p_egb): egb(p_egb) {}

    void
    filter(const Geometry* geom)
    {
        const LineString* ls = dynamic_cast<const LineString*>(geom);
        if(ls) {
            egb->add(ls);
        }
    }
};

/*public*/
void
EdgeGraphBuilder::add(const Geometry* geom)
{
    EdgeGraphLinestringFilter eglf(this);
    geom->applyComponentFilter(eglf);
}


/*public*/
void
EdgeGraphBuilder::add(const GeometryCollection* geoms)
{
    for(const auto &geom : *geoms) {
        add(geom.get());
    }
}

/*public*/
void
EdgeGraphBuilder::add(const LineString* line)
{
    const CoordinateSequence* seq = line->getCoordinatesRO();
    for (std::size_t i = 1, sz = seq->getSize(); i < sz; i++) {
        graph->addEdge(seq->getAt(i-1), seq->getAt(i));
    }
}



} // namespace geos.edgegraph
} // namespace geos



