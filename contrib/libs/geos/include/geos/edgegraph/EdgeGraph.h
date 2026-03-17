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

#include <geos/edgegraph/HalfEdge.h>

#include <geos/export.h>
#include <string>
#include <cassert>
#include <map>
#include <array>
#include <memory>
#include <vector>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
}

#undef EDGEGRAPH_HEAPHACK

namespace geos {
namespace edgegraph { // geos.edgegraph


/**
 * A graph comprised of {@link HalfEdge}s.
 * It supports tracking the vertices in the graph
 * via edges incident on them,
 * to allow efficient lookup of edges and vertices.
 *
 * This class may be subclassed to use a
 * different subclass of HalfEdge,
 * by overriding {@link createEdge}.
 * If additional logic is required to initialize
 * edges then {@link addEdge}
 * can be overridden as well.
 *
 * @author Martin Davis
 *
 */

class GEOS_DLL EdgeGraph {

private:

    std::deque<HalfEdge> edges;
    std::map<geom::Coordinate, HalfEdge*> vertexMap;

    HalfEdge* create(const geom::Coordinate& p0, const geom::Coordinate& p1);


protected:

    /**
    * Creates a single HalfEdge.
    * Override to use a different HalfEdge subclass.
    *
    * @param orig the origin location
    * @return a new HalfEdge with the given origin
    */
    HalfEdge* createEdge(const geom::Coordinate& orig);

    /**
    * Inserts an edge not already present into the graph.
    *
    * @param orig the edge origin location
    * @param dest the edge destination location
    * @param eAdj an existing edge with same orig (if any)
    * @return the created edge
    */
    HalfEdge* insert(const geom::Coordinate& orig, const geom::Coordinate& dest, HalfEdge* eAdj);


public:

    /**
    * Initialized
    */
    EdgeGraph() {};

    /**
    * Adds an edge between the coordinates orig and dest
    * to this graph.
    * Only valid edges can be added (in particular, zero-length segments cannot be added)
    *
    * @param orig the edge origin location
    * @param dest the edge destination location.
    * @return the created edge
    * @return null if the edge was invalid and not added
    *
    * @see isValidEdge(Coordinate, Coordinate)
    */
    HalfEdge* addEdge(const geom::Coordinate& orig, const geom::Coordinate& dest);

    /**
    * Tests if the given coordinates form a valid edge (with non-zero length).
    *
    * @param orig the start coordinate
    * @param dest the end coordinate
    * @return true if the edge formed is valid
    */
    static bool isValidEdge(const geom::Coordinate& orig, const geom::Coordinate& dest);

    void getVertexEdges(std::vector<const HalfEdge*>& edgesOut);

    /**
    * Finds an edge in this graph with the given origin
    * and destination, if one exists.
    *
    * @param orig the origin location
    * @param dest the destination location.
    * @return an edge with the given orig and dest, or null if none exists
    */
    HalfEdge* findEdge(const geom::Coordinate& orig, const geom::Coordinate& dest);






};


} // namespace geos.edgegraph
} // namespace geos



