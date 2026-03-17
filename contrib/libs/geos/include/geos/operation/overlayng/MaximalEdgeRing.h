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

#include <geos/operation/overlayng/OverlayEdgeRing.h>

#include <vector>
#include <memory>
#include <geos/export.h>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class CoordinateArray;
class GeometryFactory;
}
namespace operation {
namespace overlayng {
class OverlayEdge;
class EdgeRing;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

class GEOS_DLL MaximalEdgeRing {

private:

    // Constants
    static constexpr int STATE_FIND_INCOMING = 1;
    static constexpr int STATE_LINK_OUTGOING = 2;

    // Members
    OverlayEdge* startEdge;

    // Methods
    void attachEdges(OverlayEdge* startEdge);
    void linkMinimalRings();

    /**
    * Links the edges of a {@link MaximalEdgeRing} around this node
    * into minimal edge rings ({@link OverlayEdgeRing}s).
    * Minimal ring edges are linked in the opposite orientation (CW)
    * to the maximal ring.
    * This changes self-touching rings into a two or more separate rings,
    * as per the OGC SFS polygon topology semantics.
    * This relinking must be done to each max ring separately,
    * rather than all the node result edges, since there may be
    * more than one max ring incident at the node.
    *
    * @param nodeEdge an edge originating at this node
    * @param maxRing the maximal ring to link
    */
    static void linkMinRingEdgesAtNode(OverlayEdge* nodeEdge, MaximalEdgeRing* maxRing);

    /**
    * Tests if an edge of the maximal edge ring is already linked into
    * a minimal {@link OverlayEdgeRing}.  If so, this node has already been processed
    * earlier in the maximal edgering linking scan.
    *
    * @param edge an edge of a maximal edgering
    * @param maxRing the maximal edgering
    * @return true if the edge has already been linked into a minimal edgering.
    */
    static bool isAlreadyLinked(OverlayEdge* edge, MaximalEdgeRing* maxRing);

    static OverlayEdge* selectMaxOutEdge(OverlayEdge* currOut, MaximalEdgeRing* maxEdgeRing);
    static OverlayEdge* linkMaxInEdge(OverlayEdge* currOut, OverlayEdge* currMaxRingOut, MaximalEdgeRing* maxEdgeRing);


public:

    MaximalEdgeRing(OverlayEdge* e)
        : startEdge(e)
    {
        attachEdges(e);
    };

    std::vector<std::unique_ptr<OverlayEdgeRing>> buildMinimalRings(const GeometryFactory* geometryFactory);

    /**
    * Traverses the star of edges originating at a node
    * and links consecutive result edges together
    * into maximal edge rings.
    * To link two edges the resultNextMax< pointer
    * for an incoming< result edge
    * is set to the next outgoing result edge.
    *
    * Edges are linked when:
    * - they belong to an area (i.e. they have sides)
    * - they are marked as being in the result
    *
    * Edges are linked in CCW order
    * (which is the order they are linked in the underlying graph).
    * This means that rings have their face on the Right
    * (in other words,
    * the topological location of the face is given by the RHS label of the DirectedEdge).
    * This produces rings with CW orientation.
    *
    * PRECONDITIONS:
    * - This edge is in the result
    * - This edge is not yet linked
    * - The edge and its sym are NOT both marked as being in the result
    */
    static void linkResultAreaMaxRingAtNode(OverlayEdge* nodeEdge);

    friend std::ostream& operator<<(std::ostream& os, const MaximalEdgeRing& mer);

};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

