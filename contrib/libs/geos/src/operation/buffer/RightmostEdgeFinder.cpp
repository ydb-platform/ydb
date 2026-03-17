/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/RightmostEdgeFinder.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/algorithm/Orientation.h>
#include <geos/operation/buffer/RightmostEdgeFinder.h>
#include <geos/geomgraph/DirectedEdge.h>
#include <geos/geomgraph/DirectedEdgeStar.h>
#include <geos/geom/Position.h>
#include <geos/geomgraph/Node.h>
#include <geos/geomgraph/Edge.h>
#include <geos/util/TopologyException.h>
#include <geos/util.h>

#include <vector>
#include <cassert>

using namespace geos::algorithm; // Orientation
using namespace geos::geom;
using namespace geos::geomgraph; // DirectedEdge, Position

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/*public*/
RightmostEdgeFinder::RightmostEdgeFinder()
    :
    minIndex(-1), // FIXME: don't use -1 as a sentinel, or we won't be
    // able to use an unsigned int here
    minCoord(Coordinate::getNull()),
    minDe(nullptr),
    orientedDe(nullptr)
{
}

/*public*/
void
RightmostEdgeFinder::findEdge(std::vector<DirectedEdge*>* dirEdgeList)
{

#ifndef NDEBUG
    size_t checked = 0;
#endif

    /*
     * Check all forward DirectedEdges only.  This is still general,
     * because each edge has a forward DirectedEdge.
     */
    size_t dirEdgeListSize = dirEdgeList->size();
    for(size_t i = 0; i < dirEdgeListSize; ++i) {
        DirectedEdge* de = (*dirEdgeList)[i];
        assert(de);
        if(!de->isForward()) {
            continue;
        }
        checkForRightmostCoordinate(de);
#ifndef NDEBUG
        ++checked;
#endif
    }

    if(! minDe) {
        // I don't know why, but it looks like this can happen
        // (invalid PlanarGraph, I think)
        // See http://trac.osgeo.org/geos/ticket/605#comment:17
        //
        throw util::TopologyException("No forward edges found in buffer subgraph");
    }

#ifndef NDEBUG
    assert(checked > 0);
    assert(minIndex >= 0);
    assert(minDe);
#endif

    /*
     * If the rightmost point is a node, we need to identify which of
     * the incident edges is rightmost.
     */
    assert(minIndex != 0 || minCoord == minDe->getCoordinate());
    // inconsistency in rightmost processing

    if(minIndex == 0) {
        findRightmostEdgeAtNode();
    }
    else {
        findRightmostEdgeAtVertex();
    }

    /*
     * now check that the extreme side is the R side.
     * If not, use the sym instead.
     */
    orientedDe = minDe;
    int rightmostSide = getRightmostSide(minDe, minIndex);
    if(rightmostSide == Position::LEFT) {
        orientedDe = minDe->getSym();
    }
}

/*private*/
void
RightmostEdgeFinder::findRightmostEdgeAtNode()
{
    Node* node = minDe->getNode();
    assert(node);

    DirectedEdgeStar* star = detail::down_cast<DirectedEdgeStar*>(node->getEdges());

    // Warning! NULL could be returned if the star is empty!
    minDe = star->getRightmostEdge();
    assert(minDe);

    // the DirectedEdge returned by the previous call is not
    // necessarily in the forward direction. Use the sym edge if it isn't.
    if(!minDe->isForward()) {
        minDe = minDe->getSym();

        const Edge* minEdge = minDe->getEdge();
        assert(minEdge);

        const CoordinateSequence* minEdgeCoords =
            minEdge->getCoordinates();
        assert(minEdgeCoords);

        minIndex = (int)(minEdgeCoords->getSize()) - 1;
        assert(minIndex >= 0);
    }
}

/*private*/
void
RightmostEdgeFinder::findRightmostEdgeAtVertex()
{
    /*
     * The rightmost point is an interior vertex, so it has
     * a segment on either side of it.
     * If these segments are both above or below the rightmost
     * point, we need to determine their relative orientation
     * to decide which is rightmost.
     */

    Edge* minEdge = minDe->getEdge();
    assert(minEdge);
    const CoordinateSequence* pts = minEdge->getCoordinates();
    assert(pts);

    // rightmost point expected to be interior vertex of edge
    assert(minIndex > 0);
    assert((size_t)minIndex < pts->getSize());

    const Coordinate& pPrev = pts->getAt(minIndex - 1);
    const Coordinate& pNext = pts->getAt(minIndex + 1);
    int orientation = Orientation::index(
                          minCoord,
                          pNext,
                          pPrev);
    bool usePrev = false;

    // both segments are below min point
    if(pPrev.y < minCoord.y && pNext.y < minCoord.y
            && orientation == Orientation::COUNTERCLOCKWISE) {
        usePrev = true;
    }
    else if(pPrev.y > minCoord.y && pNext.y > minCoord.y
            && orientation == Orientation::CLOCKWISE) {
        usePrev = true;
    }

    // if both segments are on the same side, do nothing - either is safe
    // to select as a rightmost segment
    if(usePrev) {
        minIndex = minIndex - 1;
    }
}

/*private*/
void
RightmostEdgeFinder::checkForRightmostCoordinate(DirectedEdge* de)
{
    const Edge* deEdge = de->getEdge();
    assert(deEdge);

    const CoordinateSequence* coord = deEdge->getCoordinates();
    assert(coord);

    // only check vertices which are the starting point of
    // a non-horizontal segment
    size_t n = coord->getSize() - 1;
    for(size_t i = 0; i < n; i++) {
        // only check vertices which are the start or end point
        // of a non-horizontal segment
        // <FIX> MD 19 Sep 03 - NO!  we can test all vertices,
        // since the rightmost must have a non-horiz segment adjacent to it
        if(minCoord.isNull() ||
                coord->getAt(i).x > minCoord.x) {
            minDe = de;
            minIndex = (int)i;
            minCoord = coord->getAt(i);
        }
    }
}

/*private*/
int
RightmostEdgeFinder::getRightmostSide(DirectedEdge* de, int index)
{
    int side = getRightmostSideOfSegment(de, index);

    if(side < 0) {
        side = getRightmostSideOfSegment(de, index - 1);
    }

    if(side < 0) {
        // reaching here can indicate that segment is horizontal
        // Assert::shouldNeverReachHere(
        //	"problem with finding rightmost side of segment");

        minCoord = Coordinate::getNull();
        checkForRightmostCoordinate(de);
    }

    return side;
}

/*private*/
int
RightmostEdgeFinder::getRightmostSideOfSegment(DirectedEdge* de, int i)
{
    assert(de);

    const Edge* e = de->getEdge();
    assert(e);

    const CoordinateSequence* coord = e->getCoordinates();
    assert(coord);

    if(i < 0 || i + 1 >= (int)coord->getSize()) {
        return -1;
    }

    // indicates edge is parallel to x-axis
    if(coord->getAt(i).y == coord->getAt(i + 1).y) {
        return -1;
    }

    int pos = Position::LEFT;
    if(coord->getAt(i).y < coord->getAt(i + 1).y) {
        pos = Position::RIGHT;
    }
    return pos;
}

} // namespace geos.operation.buffer
} // namespace geos.operation
} // namespace geos
