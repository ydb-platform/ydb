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
 * Last port: triangulate/IncrementalDelaunayTriangulator.java rev. r524
 *
 **********************************************************************/

#include <geos/triangulate/IncrementalDelaunayTriangulator.h>

#include <geos/triangulate/quadedge/QuadEdge.h>
#include <geos/triangulate/quadedge/QuadEdgeSubdivision.h>
#include <geos/triangulate/quadedge/LocateFailureException.h>

namespace geos {
namespace triangulate { //geos.triangulate

using namespace quadedge;

IncrementalDelaunayTriangulator::IncrementalDelaunayTriangulator(
    QuadEdgeSubdivision* p_subdiv) :
    subdiv(p_subdiv), isUsingTolerance(p_subdiv->getTolerance() > 0.0)
{
}

void
IncrementalDelaunayTriangulator::insertSites(const VertexList& vertices)
{
    for(const auto& vertex : vertices) {
        insertSite(vertex);
    }
}

QuadEdge&
IncrementalDelaunayTriangulator::insertSite(const Vertex& v)
{
    /*
     * This code is based on Guibas and Stolfi (1985), with minor modifications
     * and a bug fix from Dani Lischinski (Graphic Gems 1993). (The modification
     * I believe is the test for the inserted site falling exactly on an
     * existing edge. Without this test zero-width triangles have been observed
     * to be created)
     */
    QuadEdge* e = subdiv->locate(v);

    if(!e) {
        throw LocateFailureException("Could not locate vertex.");
    }

    if(subdiv->isVertexOfEdge(*e, v)) {
        // point is already in subdivision.
        return *e;
    }
    else if(subdiv->isOnEdge(*e, v.getCoordinate())) {
        // the point lies exactly on an edge, so delete the edge
        // (it will be replaced by a pair of edges which have the point as a vertex)
        e = &e->oPrev();
        subdiv->remove(e->oNext());
    }

    /*
     * Connect the new point to the vertices of the containing triangle
     * (or quadrilateral, if the new point fell on an existing edge.)
     */
    QuadEdge* base = &subdiv->makeEdge(e->orig(), v);

    QuadEdge::splice(*base, *e);
    QuadEdge* startEdge = base;
    do {
        base = &subdiv->connect(*e, base->sym());
        e = &base->oPrev();
    }
    while(&e->lNext() != startEdge);


    // Examine suspect edges to ensure that the Delaunay condition
    // is satisfied.
    for(;;) {
        QuadEdge* t = &e->oPrev();
        if(t->dest().rightOf(*e) &&
                v.isInCircle(e->orig(), t->dest(), e->dest())) {
            QuadEdge::swap(*e);
            e = &e->oPrev();
        }
        else if(&e->oNext() == startEdge) {
            return *base; // no more suspect edges.
        }
        else {
            e = &e->oNext().lPrev();
        }
    }
}

} //namespace geos.triangulate
} //namespace goes

