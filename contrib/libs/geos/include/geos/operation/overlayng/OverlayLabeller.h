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
#include <geos/geom/Location.h>
#include <geos/operation/overlayng/OverlayGraph.h>

#include <vector>
#include <deque>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
namespace operation {
namespace overlayng {
class OverlayLabel;
class OverlayGraph;
class OverlayEdge;
class InputGeometry;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


class GEOS_DLL OverlayLabeller {

private:

    // Members
    OverlayGraph* graph;
    InputGeometry* inputGeometry;
    std::vector<OverlayEdge*>& edges;

    /**
    * Finds a boundary edge for this geom, if one exists.
    */
    static OverlayEdge* findPropagationStartEdge(OverlayEdge* nodeEdge, int geomIndex);

    /**
    * At this point collapsed edges with unknown location
    * must be disconnected from the boundary edges of the parent
    * (because otherwise the location would have
    * been propagated from them).
    * This can occur with a collapsed hole or shell.
    * The edges can be labeled based on their parent ring role (shell or hole).
    * (This cannot be done earlier, because the location
    * based on the boundary edges must take precedence.
    * There are situations where a collapsed edge has a location
    * which is different to its ring role -
    * e.g. a narrow gore in a polygon, which is in
    * the interior of the reduced polygon, but whose
    * ring role would imply the location EXTERIOR.)
    *
    * Note that collapsed edges can NOT have location determined via a PIP location check,
    * because that is done against the unreduced input geometry,
    * which may give an invalid result due to topology collapse.
    *
    * The labeling is propagated to other connected linear edges,
    * since there may be NOT_PART edges which are connected,
    * and they can be labeled in the same way.
    * (These would get labeled anyway during subsequent disconnected labeling pass,
    * but may be more efficient and accurate to do it here.)
    */
    void labelCollapsedEdges();
    void labelCollapsedEdge(OverlayEdge* edge, int geomIndex);

    /**
    * There can be edges which have unknown location
    * but are connected to a Line edge with known location.
    * In this case line location is propagated to the connected edges.
    */
    void labelConnectedLinearEdges();
    void propagateLinearLocations(int geomIndex);
    static void propagateLinearLocationAtNode(OverlayEdge* eNode, int geomIndex, bool isInputLine, std::deque<OverlayEdge*>& edgeStack);

    /**
    * Finds all OverlayEdges which are linear
    * (i.e. line or collapsed) and have a known location
    * for the given input geometry.
    */
    static std::vector<OverlayEdge*> findLinearEdgesWithLocation(std::vector<OverlayEdge*>& edges, int geomIndex);

    /**
    * At this point there may still be edges which have unknown location
    * relative to an input geometry.
    * This must be because they are NOT_PART edges for that geometry,
    * and are disconnected from any edges of that geometry.
    * An example of this is rings of one geometry wholly contained
    * in another geometry.
    * The location must be fully determined to compute a
    * correct result for all overlay operations.
    *
    * If the input geometry is an Area the edge location can
    * be determined via a PIP test.
    * If the input is not an Area the location is EXTERIOR.
    */
    void labelDisconnectedEdges();

    /**
    * Determines the location of an edge relative to a target input geometry.
    * The edge has no location information
    * because it is disconnected from other
    * edges that would provide that information.
    * The location is determined by checking
    * if the edge lies inside the target geometry area (if any).
    */
    void labelDisconnectedEdge(OverlayEdge* edge, int geomIndex);

    /**
    * Determines the {@link Location} for an edge within an Area geometry
    * via point-in-polygon location.
    *
    * NOTE this is only safe to use for disconnected edges,
    * since the test is carried out against the original input geometry,
    * and precision reduction may cause incorrect results for edges
    * which are close enough to a boundary to become connected.
    */
    geom::Location locateEdge(int geomIndex, OverlayEdge* edge);

    /**
    * Determines the {@link Location} for an edge within an Area geometry
    * via point-in-polygon location,
    * by checking that both endpoints are interior to the target geometry.
    * Checking both endpoints ensures correct results in the presence of topology collapse.
    * <p>
    * NOTE this is only safe to use for disconnected edges,
    * since the test is carried out against the original input geometry,
    * and precision reduction may cause incorrect results for edges
    * which are close enough to a boundary to become connected.
    */
    geom::Location locateEdgeBothEnds(int geomIndex, OverlayEdge* edge);

    /**
    * Labels edges around nodes based on the arrangement
    * of incident area boundary edges.
    * Also propagates the labelling to connected linear edges.
    */
    void labelAreaNodeEdges(std::vector<OverlayEdge*>& nodes);



public:

    OverlayLabeller(OverlayGraph* p_graph, InputGeometry* p_inputGeometry)
        : graph(p_graph)
        , inputGeometry(p_inputGeometry)
        , edges(p_graph->getEdges())
    {}

    /**
    * Computes the topological labelling for the edges in the graph->
    */
    void computeLabelling();

    /**
    * Scans around a node CCW, propagating the side labels
    * for a given area geometry to all edges (and their sym)
    * with unknown locations for that geometry.
    */
    void propagateAreaLocations(OverlayEdge* nodeEdge, int geomIndex);

    void markResultAreaEdges(int overlayOpCode);

    /**
    * Marks an edge which forms part of the boundary of the result area.
    * This is determined by the overlay operation being executed,
    * and the location of the edge.
    * The relevant location is either the right side of a boundary edge,
    * or the line location of a non-boundary edge.
    */
    void markInResultArea(OverlayEdge* e, int overlayOpCode);

    /**
    * Unmarks result area edges where the sym edge
    * is also marked as in the result.
    * This has the effect of merging edge-adjacent result areas,
    * as required by polygon validity rules.
    */
    void unmarkDuplicateEdgesFromResultArea();


};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

