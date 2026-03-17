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
 * Last port: triangulate/DelaunayTriangulationBuilder.java rev. r524
 *
 **********************************************************************/

#include <geos/triangulate/DelaunayTriangulationBuilder.h>

#include <algorithm>

#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/operation/valid/RepeatedPointRemover.h>
#include <geos/triangulate/IncrementalDelaunayTriangulator.h>
#include <geos/triangulate/quadedge/QuadEdgeSubdivision.h>
#include <geos/operation/valid/RepeatedPointRemover.h>
#include <geos/operation/valid/RepeatedPointTester.h>
#include <geos/util.h>

namespace geos {
namespace triangulate { //geos.triangulate

using namespace geos::geom;

std::unique_ptr<CoordinateSequence>
DelaunayTriangulationBuilder::extractUniqueCoordinates(
    const Geometry& geom)
{
    std::unique_ptr<CoordinateSequence> seq(geom.getCoordinates());
    return unique(seq.get());
}

std::unique_ptr<CoordinateSequence>
DelaunayTriangulationBuilder::unique(const CoordinateSequence* seq)
{
    auto seqFactory = CoordinateArraySequenceFactory::instance();
    auto dim = seq->getDimension();

    std::vector<Coordinate> coords;
    seq->toVector(coords);
    std::sort(coords.begin(), coords.end(), geos::geom::CoordinateLessThen());

    std::unique_ptr<CoordinateSequence> sortedSeq(seqFactory->create(std::move(coords), dim));

    operation::valid::RepeatedPointTester rpt;
    if (rpt.hasRepeatedPoint(sortedSeq.get())) {
        return operation::valid::RepeatedPointRemover::removeRepeatedPoints(sortedSeq.get());
    } else {
        return sortedSeq;
    }
}

IncrementalDelaunayTriangulator::VertexList
DelaunayTriangulationBuilder::toVertices(
    const CoordinateSequence& coords)
{
    IncrementalDelaunayTriangulator::VertexList vertexList(coords.size());

    for(size_t i = 0; i < coords.size(); i++) {
        vertexList[i] = quadedge::Vertex(coords.getAt(i));
    }
    return vertexList;
}

DelaunayTriangulationBuilder::DelaunayTriangulationBuilder() :
    siteCoords(nullptr), tolerance(0.0), subdiv(nullptr)
{
}

void
DelaunayTriangulationBuilder::setSites(const Geometry& geom)
{
    // remove any duplicate points (they will cause the triangulation to fail)
    siteCoords = extractUniqueCoordinates(geom);
}

void
DelaunayTriangulationBuilder::setSites(const CoordinateSequence& coords)
{
    // remove any duplicate points (they will cause the triangulation to fail)
    siteCoords = unique(&coords);
}

void
DelaunayTriangulationBuilder::create()
{
    if(subdiv != nullptr || siteCoords == nullptr) {
        return;
    }

    Envelope siteEnv;
    siteCoords ->expandEnvelope(siteEnv);
    auto vertices = toVertices(*siteCoords);
    std::sort(vertices.begin(), vertices.end()); // Best performance from locator when inserting points near each other

    subdiv.reset(new quadedge::QuadEdgeSubdivision(siteEnv, tolerance));
    IncrementalDelaunayTriangulator triangulator = IncrementalDelaunayTriangulator(subdiv.get());
    triangulator.insertSites(vertices);
}

quadedge::QuadEdgeSubdivision&
DelaunayTriangulationBuilder::getSubdivision()
{
    create();
    return *subdiv;
}

std::unique_ptr<MultiLineString>
DelaunayTriangulationBuilder::getEdges(
    const GeometryFactory& geomFact)
{
    create();
    return subdiv->getEdges(geomFact);
}

std::unique_ptr<geom::GeometryCollection>
DelaunayTriangulationBuilder::getTriangles(
    const geom::GeometryFactory& geomFact)
{
    create();
    return subdiv->getTriangles(geomFact);
}

geom::Envelope
DelaunayTriangulationBuilder::envelope(const geom::CoordinateSequence& coords)
{
    Envelope env;
    coords.expandEnvelope(env);
    return env;
}


} //namespace geos.triangulate
} //namespace goes

