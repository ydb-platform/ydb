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
 * Last port: triangulate/IncrementalDelaunayTriangulator.java r524
 *
 **********************************************************************/

#ifndef GEOS_TRIANGULATE_INCREMENTALDELAUNAYTRIANGULATOR_H
#define GEOS_TRIANGULATE_INCREMENTALDELAUNAYTRIANGULATOR_H

#include <list>

#include <geos/triangulate/quadedge/Vertex.h>


namespace geos {
namespace triangulate { //geos.triangulate

namespace quadedge {
class QuadEdge;
class QuadEdgeSubdivision;
}

/** \brief
 * Computes a Delauanay Triangulation of a set of {@link quadedge::Vertex}es,
 * using an incrementatal insertion algorithm.
 *
 * @author JTS: Martin Davis
 * @author Benjamin Campbell
 */
class GEOS_DLL IncrementalDelaunayTriangulator {
private:
    quadedge::QuadEdgeSubdivision* subdiv;
    bool isUsingTolerance;

public:
    /**
     * Creates a new triangulator using the given {@link quadedge::QuadEdgeSubdivision}.
     * The triangulator uses the tolerance of the supplied subdivision.
     *
     * @param subdiv
     *          a subdivision in which to build the TIN
     */
    IncrementalDelaunayTriangulator(quadedge::QuadEdgeSubdivision* subdiv);

    typedef std::vector<quadedge::Vertex> VertexList;

    /**
     * Inserts all sites in a collection. The inserted vertices <b>MUST</b> be
     * unique up to the provided tolerance value. (i.e. no two vertices should be
     * closer than the provided tolerance value). They do not have to be rounded
     * to the tolerance grid, however.
     *
     * @param vertices a Collection of Vertex
     *
     * @throws LocateFailureException if the location algorithm
     *         fails to converge in a reasonable number of iterations
     */
    void insertSites(const VertexList& vertices);

    /**
     * Inserts a new point into a subdivision representing a Delaunay
     * triangulation, and fixes the affected edges so that the result
     * is still a Delaunay triangulation.
     * <p>
     *
     * @return a quadedge containing the inserted vertex
     */
    quadedge::QuadEdge& insertSite(const quadedge::Vertex& v);
};

} //namespace geos.triangulate
} //namespace goes

#endif //GEOS_TRIANGULATE_QUADEDGE_INCREMENTALDELAUNAYTRIANGULATOR_H

