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

#include <geos/export.h>
#include <geos/operation/overlayng/OverlayEdge.h>
#include <geos/operation/overlayng/OverlayLabel.h>
#include <geos/geom/CoordinateSequence.h>

#include <map>
#include <vector>
#include <deque>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
namespace operation {
namespace overlayng {
class Edge;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/**
 * A planar graph of {@link OverlayEdge}, representing
 * the topology resulting from an overlay operation.
 * Each source {@link Edge} is represented
 * by two OverlayEdges, with opposite orientation.
 * A single {@link OverlayLabel} is created for each symmetric pair of OverlayEdges.
 *
 * @author mdavis
 *
 */
class GEOS_DLL OverlayGraph {

private:

    // Members
    std::map<Coordinate, OverlayEdge*> nodeMap;
    std::vector<OverlayEdge*> edges;

    // Locally store the OverlayEdge and OverlayLabel
    std::deque<OverlayEdge> ovEdgeQue;
    std::deque<OverlayLabel> ovLabelQue;

    std::vector<std::unique_ptr<const geom::CoordinateSequence>> csQue;

    // Methods

    /**
    * Create and add HalfEdge pairs to map and vector containers,
    * using local std::deque storage for objects.
    */
    OverlayEdge* createEdgePair(const CoordinateSequence* pts, OverlayLabel* lbl);

    /**
    * Create a single OverlayEdge in local std::deque storage, and return the
    * pointer.
    */
    OverlayEdge* createOverlayEdge(const CoordinateSequence* pts, OverlayLabel* lbl, bool direction);

    void insert(OverlayEdge* e);



public:

    /**
    * Creates a new graph for a set of noded, labelled {@link Edge}s.
    */
    OverlayGraph();

    OverlayGraph(const OverlayGraph& g) = delete;
    OverlayGraph& operator=(const OverlayGraph& g) = delete;

    /**
    * Adds an edge between the coordinates orig and dest
    * to this graph.
    * Only valid edges can be added (in particular, zero-length segments cannot be added)
    *
    */
    OverlayEdge* addEdge(Edge* edge);

    /**
    * Gets the set of edges in this graph.
    * Only one of each symmetric pair of OverlayEdges is included.
    * The opposing edge can be found by using {@link OverlayEdge#sym()}.
    */
    std::vector<OverlayEdge*>& getEdges();

    /**
    * Gets the collection of edges representing the nodes in this graph.
    * For each star of edges originating at a node
    * a single representative edge is included.
    * The other edges around the node can be found by following the next and prev links.
    */
    std::vector<OverlayEdge*> getNodeEdges();

    /**
    * Gets an edge originating at the given node point.
    */
    OverlayEdge* getNodeEdge(const Coordinate& nodePt) const;

    /**
    * Gets the representative edges marked as being in the result area.
    */
    std::vector<OverlayEdge*> getResultAreaEdges();

    /**
    * Create a single OverlayLabel in local std::deque storage
    * and return a pointer to the stored object.
    */
    OverlayLabel* createOverlayLabel(const Edge* edge);

    friend std::ostream& operator<<(std::ostream& os, const OverlayGraph& og);

};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

