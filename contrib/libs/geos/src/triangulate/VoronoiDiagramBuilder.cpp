/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Excensus LLC.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: triangulate/VoronoiDiagramBuilder.java rev. r524
 *
 **********************************************************************/

#include <geos/triangulate/VoronoiDiagramBuilder.h>

#include <algorithm>
#include <math.h>
#include <vector>
#include <iostream>

#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/triangulate/IncrementalDelaunayTriangulator.h>
#include <geos/triangulate/DelaunayTriangulationBuilder.h>
#include <geos/triangulate/quadedge/QuadEdgeSubdivision.h>
#include <geos/operation/valid/RepeatedPointRemover.h>
#include <geos/util.h>

using geos::detail::make_unique;

namespace geos {
namespace triangulate { //geos.triangulate

using namespace geos::geom;


VoronoiDiagramBuilder::VoronoiDiagramBuilder() :
    tolerance(0.0), clipEnv(nullptr)
{
}

void
VoronoiDiagramBuilder::setSites(const geom::Geometry& geom)
{
    siteCoords = DelaunayTriangulationBuilder::extractUniqueCoordinates(geom);
}

void
VoronoiDiagramBuilder::setSites(const geom::CoordinateSequence& coords)
{
    siteCoords = DelaunayTriangulationBuilder::unique(&coords);
}

void
VoronoiDiagramBuilder::setClipEnvelope(const geom::Envelope* nClipEnv)
{
    clipEnv = nClipEnv;
}

void
VoronoiDiagramBuilder::setTolerance(double nTolerance)
{
    tolerance = nTolerance;
}

void
VoronoiDiagramBuilder::create()
{
    if(subdiv.get()) {
        return;
    }

    diagramEnv = DelaunayTriangulationBuilder::envelope(*siteCoords);
    //adding buffer around the final envelope
    double expandBy = std::max(diagramEnv.getWidth(), diagramEnv.getHeight());
    diagramEnv.expandBy(expandBy);
    if(clipEnv) {
        diagramEnv.expandToInclude(clipEnv);
    }

    auto vertices = DelaunayTriangulationBuilder::toVertices(*siteCoords);
    std::sort(vertices.begin(), vertices.end()); // Best performance from locator when inserting points near each other

    subdiv.reset(new quadedge::QuadEdgeSubdivision(diagramEnv, tolerance));
    IncrementalDelaunayTriangulator triangulator(subdiv.get());
    triangulator.insertSites(vertices);
}

std::unique_ptr<quadedge::QuadEdgeSubdivision>
VoronoiDiagramBuilder::getSubdivision()
{
    create();
    // NOTE: Apparently, this is 'source' method giving up the object resource.
    return std::move(subdiv);
}

std::unique_ptr<geom::GeometryCollection>
VoronoiDiagramBuilder::getDiagram(const geom::GeometryFactory& geomFact)
{
    create();

    auto polys = subdiv->getVoronoiCellPolygons(geomFact);
    auto ret = clipGeometryCollection(polys, diagramEnv);

    if (ret == nullptr) {
        return std::unique_ptr<geom::GeometryCollection>(geomFact.createGeometryCollection());
    }

    return ret;
}

std::unique_ptr<geom::Geometry>
VoronoiDiagramBuilder::getDiagramEdges(const geom::GeometryFactory& geomFact)
{
    create();
    std::unique_ptr<geom::MultiLineString> edges = subdiv->getVoronoiDiagramEdges(geomFact);
    if(edges->isEmpty()) {
        return std::unique_ptr<Geometry>(edges.release());
    }
    std::unique_ptr<geom::Geometry> clipPoly(geomFact.toGeometry(&diagramEnv));
    std::unique_ptr<Geometry> clipped(clipPoly->intersection(edges.get()));
    return clipped;
}

std::unique_ptr<geom::GeometryCollection>
VoronoiDiagramBuilder::clipGeometryCollection(std::vector<std::unique_ptr<Geometry>> & geoms, const geom::Envelope& clipEnv)
{
    if (geoms.empty()) {
        return nullptr;
    }

    auto gfact = geoms[0]->getFactory();

    std::unique_ptr<geom::Geometry> clipPoly(gfact->toGeometry(&clipEnv));
    std::vector<std::unique_ptr<Geometry>> clipped;

    for(auto& g : geoms) {
        // don't clip unless necessary
        if(clipEnv.contains(g->getEnvelopeInternal())) {
            clipped.push_back(std::move(g));
            // TODO: check if userData is correctly cloned here?
        } else if(clipEnv.intersects(g->getEnvelopeInternal())) {
            auto result = clipPoly->intersection(g.get());
            result->setUserData(g->getUserData()); // TODO: needed ?
            if (!result->isEmpty()) {
                clipped.push_back(std::move(result));
            }
        }
    }

    return gfact->createGeometryCollection(std::move(clipped));
}

} //namespace geos.triangulate
} //namespace geos
