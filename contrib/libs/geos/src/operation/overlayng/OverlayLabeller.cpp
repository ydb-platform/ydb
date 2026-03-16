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

#include <geos/operation/overlayng/OverlayLabeller.h>
#include <geos/operation/overlayng/OverlayEdge.h>
#include <geos/operation/overlayng/OverlayNG.h>
#include <geos/operation/overlayng/InputGeometry.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Position.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/util/Assert.h>
#include <geos/util/TopologyException.h>


namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


using namespace geos::geom;

/*public*/
void
OverlayLabeller::computeLabelling()
{
    std::vector<OverlayEdge*> nodes = graph->getNodeEdges();
    labelAreaNodeEdges(nodes);
    labelConnectedLinearEdges();

    //TODO: is there a way to avoid scanning all edges in these steps?
    /**
     * At this point collapsed edges labeled with location UNKNOWN
     * must be disconnected from the area edges of the parent.
     * They can be located based on their parent ring role (shell or hole).
     */
    labelCollapsedEdges();
    labelConnectedLinearEdges();

    labelDisconnectedEdges();
}


/*private*/
void
OverlayLabeller::labelAreaNodeEdges(std::vector<OverlayEdge*>& nodes)
{
    for (OverlayEdge* nodeEdge : nodes) {
        propagateAreaLocations(nodeEdge, 0);
        if (inputGeometry->hasEdges(1)) {
            propagateAreaLocations(nodeEdge, 1);
        }
    }
}

/*public*/
void
OverlayLabeller::propagateAreaLocations(OverlayEdge* nodeEdge, int geomIndex)
{
    /*
     * Only propagate for area geometries
     */
    if (!inputGeometry->isArea(geomIndex))
        return;

    /**
     * No need to propagate if node has only one edge.
     * This handles dangling edges created by overlap limiting
     */
    if (nodeEdge->degree() == 1)
        return;

    OverlayEdge* eStart = findPropagationStartEdge(nodeEdge, geomIndex);
    // no labelled edge found, so nothing to propagate
    if (eStart == nullptr)
        return;

    // initialize currLoc to location of L side
    Location currLoc = eStart->getLocation(geomIndex, Position::LEFT);
    OverlayEdge* e = eStart->oNextOE();

    do {
        OverlayLabel* label = e->getLabel();
        if (!label->isBoundary(geomIndex)) {
            /**
            * If this is not a Boundary edge for this input area,
            * its location is now known relative to this input area
            */
            label->setLocationLine(geomIndex, currLoc);
        }
        else {
            util::Assert::isTrue(label->hasSides(geomIndex));
            /**
             *  This is a boundary edge for the input area geom.
             *  Update the current location from its labels.
             *  Also check for topological consistency.
             */
            Location locRight = e->getLocation(geomIndex, Position::RIGHT);
            if (locRight != currLoc) {
                throw util::TopologyException("side location conflict", e->getCoordinate());
            }
            Location locLeft = e->getLocation(geomIndex, Position::LEFT);
            if (locLeft == Location::NONE) {
                util::Assert::shouldNeverReachHere("found single null side");
            }
            currLoc = locLeft;
        }
        e = e->oNextOE();
    } while (e != eStart);
}

/*private*/
OverlayEdge*
OverlayLabeller::findPropagationStartEdge(OverlayEdge* nodeEdge, int geomIndex)
{
    OverlayEdge* eStart = nodeEdge;
    do {
        const OverlayLabel* label = eStart->getLabel();
        if (label->isBoundary(geomIndex)) {
            util::Assert::isTrue(label->hasSides(geomIndex));
            return eStart;
        }
        eStart = static_cast<OverlayEdge*>(eStart->oNext());
    } while (eStart != nodeEdge);
    return nullptr;
}

/*private*/
void
OverlayLabeller::labelCollapsedEdges()
{
    for (OverlayEdge* edge : edges) {
        if (edge->getLabel()->isLineLocationUnknown(0)) {
            labelCollapsedEdge(edge, 0);
        }
        if (edge->getLabel()->isLineLocationUnknown(1)) {
            labelCollapsedEdge(edge, 1);
        }
    }
}

/*private*/
void
OverlayLabeller::labelCollapsedEdge(OverlayEdge* edge, int geomIndex)
{
    OverlayLabel* label = edge->getLabel();
    if (! label->isCollapse(geomIndex))
        return;
    /**
    * This must be a collapsed edge which is disconnected
    * from any area edges (e.g. a fully collapsed shell or hole).
    * It can be labeled according to its parent source ring role.
    */
    label->setLocationCollapse(geomIndex);
}

/*private*/
void
OverlayLabeller::labelConnectedLinearEdges()
{
    //TODO: can these be merged to avoid two scans?
    propagateLinearLocations(0);
    if (inputGeometry->hasEdges(1)) {
        propagateLinearLocations(1);
    }
}

/*private*/
void
OverlayLabeller::propagateLinearLocations(int geomIndex)
{
    std::vector<OverlayEdge*> linearEdges = findLinearEdgesWithLocation(edges, geomIndex);
    if (linearEdges.size() <= 0) return;

    std::deque<OverlayEdge*> edgeStack;
    edgeStack.insert(edgeStack.begin(), linearEdges.begin(), linearEdges.end());
    bool isInputLine = inputGeometry->isLine(geomIndex);
    // traverse connected linear edges, labeling unknown ones
    while (! edgeStack.empty()) {
        OverlayEdge* lineEdge = edgeStack.front();
        edgeStack.pop_front();

        // for any edges around origin with unknown location for this geomIndex,
        // add those edges to stack to continue traversal
        propagateLinearLocationAtNode(lineEdge, geomIndex, isInputLine, edgeStack);
    }
}

/*private static*/
void
OverlayLabeller::propagateLinearLocationAtNode(OverlayEdge* eNode,
    int geomIndex, bool isInputLine,
    std::deque<OverlayEdge*>& edgeStack)
{
    Location lineLoc = eNode->getLabel()->getLineLocation(geomIndex);
    /**
    * If the parent geom is a Line
    * then only propagate EXTERIOR locations.
    */
    if (isInputLine && lineLoc != Location::EXTERIOR)
        return;

    OverlayEdge* e = eNode->oNextOE();
    do {
        OverlayLabel* label = e->getLabel();
        if (label->isLineLocationUnknown(geomIndex)) {
            /**
             * If edge is not a boundary edge,
             * its location is now known for this area
             */
            label->setLocationLine(geomIndex, lineLoc);
            /**
             * Add sym edge to stack for graph traversal
             * (Don't add e itself, since e origin node has now been scanned)
             */
            edgeStack.push_front(e->symOE());
      }
      e = e->oNextOE();
    }
    while (e != eNode);
}

/*private static*/
std::vector<OverlayEdge*>
OverlayLabeller::findLinearEdgesWithLocation(std::vector<OverlayEdge*>& edges, int geomIndex)
{
    std::vector<OverlayEdge*> linearEdges;
    for (OverlayEdge* edge : edges) {
        OverlayLabel* lbl = edge->getLabel();
        // keep if linear with known location
        if (lbl->isLinear(geomIndex) && !lbl->isLineLocationUnknown(geomIndex)) {
            linearEdges.push_back(edge);
        }
    }
    return linearEdges;
}

/*private*/
void
OverlayLabeller::labelDisconnectedEdges()
{
    for (OverlayEdge* edge : edges) {
        if (edge->getLabel()->isLineLocationUnknown(0)) {
            labelDisconnectedEdge(edge, 0);
        }
        if (edge->getLabel()->isLineLocationUnknown(1)) {
            labelDisconnectedEdge(edge, 1);
        }
    }
}

/*private*/
void
OverlayLabeller::labelDisconnectedEdge(OverlayEdge* edge, int geomIndex)
{
    OverlayLabel* label = edge->getLabel();

    /**
    * if target geom is not an area then
    * edge must be EXTERIOR, since to be
    * INTERIOR it would have been labelled
    * when it was created.
    */
    if (!inputGeometry->isArea(geomIndex)) {
        label->setLocationAll(geomIndex, Location::EXTERIOR);
        return;
    };

    /**
    * Locate edge in input area using a Point-In-Poly check.
    * This should be safe even with precision reduction,
    * because since the edge has remained disconnected
    * its interior-exterior relationship
    * can be determined relative to the original input geometry.
    */
    Location edgeLoc = locateEdgeBothEnds(geomIndex, edge);
    label->setLocationAll(geomIndex, edgeLoc);
}

/*private*/
Location
OverlayLabeller::locateEdge(int geomIndex, OverlayEdge* edge)
{
    Location loc = inputGeometry->locatePointInArea(geomIndex, edge->orig());
    Location edgeLoc = loc != Location::EXTERIOR ? Location::INTERIOR : Location::EXTERIOR;
    return edgeLoc;
}

/*private*/
Location
OverlayLabeller::locateEdgeBothEnds(int geomIndex, OverlayEdge* edge)
{
    /*
    * To improve the robustness of the point location,
    * check both ends of the edge.
    * Edge is only labelled INTERIOR if both ends are.
    */
    Location locOrig = inputGeometry->locatePointInArea(geomIndex, edge->orig());
    Location locDest = inputGeometry->locatePointInArea(geomIndex, edge->dest());
    bool isInt = locOrig != Location::EXTERIOR && locDest != Location::EXTERIOR;
    Location edgeLoc = isInt ? Location::INTERIOR : Location::EXTERIOR;
    return edgeLoc;
}

/*public*/
void
OverlayLabeller::markResultAreaEdges(int overlayOpCode)
{
    for (OverlayEdge* edge : edges) {
        markInResultArea(edge, overlayOpCode);
    }
}

/*public*/
void
OverlayLabeller::markInResultArea(OverlayEdge* e, int overlayOpCode)
{
    const OverlayLabel* label = e->getLabel();
    if (label->isBoundaryEither() && OverlayNG::isResultOfOp(overlayOpCode,
        label->getLocationBoundaryOrLine(0, Position::RIGHT, e->isForward()),
        label->getLocationBoundaryOrLine(1, Position::RIGHT, e->isForward())))
    {
        e->markInResultArea();
    }
}

/*public*/
void
OverlayLabeller::unmarkDuplicateEdgesFromResultArea()
{
    for (OverlayEdge* edge : edges) {
        if (edge->isInResultAreaBoth()) {
            edge->unmarkFromResultAreaBoth();
        }
    }
}






} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
