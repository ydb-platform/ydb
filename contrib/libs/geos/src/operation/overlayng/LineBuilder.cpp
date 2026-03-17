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

#include <geos/operation/overlayng/LineBuilder.h>

#include <geos/operation/overlayng/OverlayEdge.h>
#include <geos/operation/overlayng/OverlayLabel.h>
#include <geos/operation/overlayng/OverlayNG.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/CoordinateSequence.h>



namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/*public*/
std::vector<std::unique_ptr<LineString>>
LineBuilder::getLines()
{
    markResultLines();
    addResultLines();

    // Transfer ownership of all the lines to the
    // caller
    return std::move(lines);
}

/*private*/
void
LineBuilder::markResultLines()
{
    std::vector<OverlayEdge*>& edges = graph->getEdges();
    for (OverlayEdge* edge : edges) {
        /**
        * If the edge linework is already marked as in the result,
        * it is not included as a line.
        * This occurs when an edge either is in a result area
        * or has already been included as a line.
        */
        if (edge->isInResultEither()) {
            continue;
        }
        if (isResultLine(edge->getLabel())) {
            edge->markInResultLine();
        }
    }
}


/*private*/
bool
LineBuilder::isResultLine(const OverlayLabel* lbl) const
{
    /**
    * Omit edge which is a boundary of a single geometry
    * (i.e. not a collapse or line edge as well).
    * These are only included if part of a result area.
    * This is a short-circuit for the most common area edge case
    */
    if (lbl->isBoundarySingleton())
        return false;

    /**
    * Omit edge which is a collapse along a boundary.
    * I.e a result line edge must be from a input line
    * OR two coincident area boundaries.
    * This logic is only used if not including collapse lines in result.
    */
    if (!isAllowCollapseLines && lbl->isBoundaryCollapse())
        return false;

    /**
     * Omit edge which is a collapse interior to its parent area.
     * (E.g. a narrow gore, or spike off a hole)
     */
    if (lbl->isInteriorCollapse())
        return false;

    /**
    * For ops other than Intersection, omit a line edge
    * if it is interior to the other area.
    *
    * For Intersection, a line edge interior to an area is included.
    */
    if (opCode != OverlayNG::INTERSECTION) {
        /**
        * Omit collapsed edge in other area interior.
        */
        if (lbl->isCollapseAndNotPartInterior())
            return false;

        /**
        * If there is a result area, omit line edge inside it.
        * It is sufficient to check against the input area rather
        * than the result area,
        * because if line edges are present then there is only one input area,
        * and the result area must be the same as the input area.
        */
        if (hasResultArea && lbl->isLineInArea(inputAreaIndex))
            return false;
    }

    /**
    * Include line edge formed by touching area boundaries,
    * if enabled.
    */
    if (isAllowMixedResult &&
        opCode == OverlayNG::INTERSECTION &&
        lbl->isBoundaryTouch()) {
        return true;
    }

    /**
    * Finally, determine included line edge
    * according to overlay op boolean logic.
    */
    Location aLoc = effectiveLocation(lbl, 0);
    Location bLoc = effectiveLocation(lbl, 1);
    bool inResult = OverlayNG::isResultOfOp(opCode, aLoc, bLoc);
    return inResult;
}

/*private*/
Location
LineBuilder::effectiveLocation(const OverlayLabel* lbl, int geomIndex) const
{
    if (lbl->isCollapse(geomIndex)) {
        return Location::INTERIOR;
    }
    if (lbl->isLine(geomIndex)) {
        return Location::INTERIOR;
    }
    return lbl->getLineLocation(geomIndex);
}


/*private*/
void
LineBuilder::addResultLinesMerged()
{
    addResultLinesForNodes();
    addResultLinesRings();
}

/*private*/
void
LineBuilder::addResultLines()
{
    std::vector<OverlayEdge*>& edges = graph->getEdges();

    for (OverlayEdge* edge : edges) {
        if (! edge->isInResultLine())
            continue;
        if (edge->isVisited())
            continue;

        lines.push_back(toLine(edge));
        edge->markVisitedBoth();
    }
}

std::unique_ptr<LineString>
LineBuilder::toLine(OverlayEdge* edge)
{
    // bool isForward = edge->isForward();
    std::unique_ptr<CoordinateArraySequence> pts(new CoordinateArraySequence());
    pts->add(edge->orig(), false);
    edge->addCoordinates(pts.get());
    return geometryFactory->createLineString(std::move(pts));
}

/**
* FUTURE: To implement a strategy preserving input lines,
* the label must carry an id for each input LineString.
* The ids are zeroed out whenever two input edges are merged.
* Additional result nodes are created where there are changes in id
* at degree-2 nodes.
* (degree>=3 nodes must be kept as nodes to ensure
* output linework is fully noded.
*/

/*private*/
void
LineBuilder::addResultLinesForNodes()
{
    std::vector<OverlayEdge*>& edges = graph->getEdges();
    for (OverlayEdge* edge : edges) {
        if (! edge->isInResultLine())
            continue;
        if (edge->isVisited())
            continue;

        /**
        * Choose line start point as a node.
        * Nodes in the line graph are degree-1 or degree >= 3 edges.
        * This will find all lines originating at nodes
        */
        if (degreeOfLines(edge) != 2) {
            std::unique_ptr<LineString> line = buildLine(edge);
            lines.push_back(std::move(line));
        }
    }
}

/*private*/
void
LineBuilder::addResultLinesRings()
{
    // TODO: an ordering could be imposed on the endpoints to make this more repeatable
    // TODO: preserve input LinearRings if possible?  Would require marking them as such
    std::vector<OverlayEdge*>& edges = graph->getEdges();
    for (OverlayEdge* edge : edges) {
    if (! edge->isInResultLine())
        continue;
    if (edge->isVisited())
        continue;

    lines.push_back(buildLine(edge));
    }
}

/*private*/
std::unique_ptr<LineString>
LineBuilder::buildLine(OverlayEdge* node)
{
    // assert: edgeStart degree = 1
    // assert: edgeStart direction = forward
    std::unique_ptr<CoordinateArraySequence> pts(new CoordinateArraySequence());
    pts->add(node->orig(), false);

    bool isNodeForward = node->isForward();

    OverlayEdge* e = node;
    do {
        e->markVisitedBoth();
        e->addCoordinates(pts.get());

        // end line if next vertex is a node
        if (degreeOfLines(e->symOE()) != 2) {
            break;
        }
        e = nextLineEdgeUnvisited(e->symOE());
        // e will be nullptr if next edge has been visited, which indicates a ring
    }
    while (e != nullptr);
    // reverse coordinates before constructing
    if(!isNodeForward) {
        CoordinateSequence::reverse(pts.get());
    }

    return geometryFactory->createLineString(std::move(pts));
}

/*private*/
OverlayEdge*
LineBuilder::nextLineEdgeUnvisited(OverlayEdge* node) const
{
    OverlayEdge* e = node;
    do {
        e = e->oNextOE();
        if (e->isVisited()) {
            continue;
        }
        if (e->isInResultLine()) {
            return e;
        }
    }
    while (e != node);
    return nullptr;
}

/*private*/
int
LineBuilder::degreeOfLines(OverlayEdge* node) const
{
    int degree = 0;
    OverlayEdge* e = node;
    do {
        if (e->isInResultLine()) {
            degree++;
        }
        e = e->oNextOE();
    }
    while (e != node);
    return degree;
}






} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
