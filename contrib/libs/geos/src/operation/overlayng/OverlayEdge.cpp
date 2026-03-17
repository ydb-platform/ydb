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

#include <geos/operation/overlayng/MaximalEdgeRing.h>
#include <geos/operation/overlayng/OverlayEdge.h>
#include <geos/operation/overlayng/OverlayLabel.h>
#include <geos/operation/overlayng/OverlayEdgeRing.h>
#include <geos/geom/Location.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateArraySequence.h>

namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/*public*/
bool
OverlayEdge::isForward() const
{
    return direction;
}

/*public*/
const Coordinate&
OverlayEdge::directionPt() const
{
    return dirPt;
}

/*public*/
OverlayLabel*
OverlayEdge::getLabel() const
{
    return label;
}

/*public*/
Location
OverlayEdge::getLocation(int index, int position) const
{
    return label->getLocation(index, position, direction);
}

/*public*/
const Coordinate&
OverlayEdge::getCoordinate() const
{
    return orig();
}

/*public*/
const CoordinateSequence*
OverlayEdge::getCoordinatesRO() const
{
    return pts;
}

/*public*/
std::unique_ptr<CoordinateSequence>
OverlayEdge::OverlayEdge::getCoordinates()
{
    // return a copy of pts
    return pts->clone();
}

/*public*/
std::unique_ptr<CoordinateSequence>
OverlayEdge::getCoordinatesOriented()
{
    if (direction) {
        return pts->clone();
    }
    std::unique_ptr<CoordinateSequence> ptsCopy = pts->clone();
    CoordinateSequence::reverse(ptsCopy.get());
    return ptsCopy;
}

/**
* Adds the coordinates of this edge to the given list,
* in the direction of the edge.
* Duplicate coordinates are removed
* (which means that this is safe to use for a path
* of connected edges in the topology graph).
*
* @param coords the coordinate list to add to
*/
/*public*/
void
OverlayEdge::addCoordinates(CoordinateArraySequence* coords)
{
    bool isFirstEdge = coords->size() > 0;
    if (direction) {
        int startIndex = 1;
        if (isFirstEdge) {
            startIndex = 0;
        }
        for (std::size_t i = startIndex, sz = pts->size(); i < sz; i++) {
            coords->add(pts->getAt(i), false);
        }
    }
    else { // is backward
        int startIndex = (int)(pts->size()) - 2;
        if (isFirstEdge) {
            startIndex = (int)(pts->size()) - 1;
        }
        for (int i = startIndex; i >= 0; i--) {
            coords->add(pts->getAt(i), false);
        }
    }
}

/*public*/
OverlayEdge*
OverlayEdge::symOE() const
{
    return static_cast<OverlayEdge*>(sym());
}

/*public*/
OverlayEdge*
OverlayEdge::oNextOE() const
{
    return static_cast<OverlayEdge*>(oNext());
}

/*public*/
bool
OverlayEdge::isInResultArea() const
{
    return m_isInResultArea;
}

/*public*/
bool
OverlayEdge::isInResultAreaBoth() const
{
    return m_isInResultArea && symOE()->m_isInResultArea;
}

/*public*/
bool
OverlayEdge::isInResultEither() const
{
    return isInResult() || symOE()->isInResult();
}

/*public*/
void
OverlayEdge::unmarkFromResultAreaBoth()
{
    m_isInResultArea = false;
    symOE()->m_isInResultArea = false;
}

/*public*/
void
OverlayEdge::markInResultArea()
{
    m_isInResultArea  = true;
}

/*public*/
void
OverlayEdge::markInResultAreaBoth()
{
    m_isInResultArea  = true;
    symOE()->m_isInResultArea = true;
}

/*public*/
bool
OverlayEdge::isInResultLine() const
{
    return m_isInResultLine;
}

/*public*/
void
OverlayEdge::markInResultLine()
{
    m_isInResultLine  = true;
    symOE()->m_isInResultLine = true;
}

/*public*/
bool
OverlayEdge::isInResult() const
{
    return m_isInResultArea || m_isInResultLine;
}

void
OverlayEdge::setNextResult(OverlayEdge* e)
{
    // Assert: e.orig() == this.dest();
    nextResultEdge = e;
}

/*public*/
OverlayEdge*
OverlayEdge::nextResult() const
{
    return nextResultEdge;
}

/*public*/
bool
OverlayEdge::isResultLinked() const
{
    return nextResultEdge != nullptr;
}

void
OverlayEdge::setNextResultMax(OverlayEdge* e)
{
    // Assert: e.orig() == this.dest();
    nextResultMaxEdge = e;
}

/*public*/
OverlayEdge*
OverlayEdge::nextResultMax() const
{
    return nextResultMaxEdge;
}

/*public*/
bool
OverlayEdge::isResultMaxLinked() const
{
    return nextResultMaxEdge != nullptr;
}

/*public*/
bool
OverlayEdge::isVisited() const
{
    return m_isVisited;
}

/*private*/
void
OverlayEdge::markVisited()
{
    m_isVisited = true;
}

/*public*/
void
OverlayEdge::markVisitedBoth()
{
    markVisited();
    symOE()->markVisited();
}

/*public*/
void
OverlayEdge::setEdgeRing(const OverlayEdgeRing* p_edgeRing)
{
    edgeRing = p_edgeRing;
}

/*public*/
const OverlayEdgeRing*
OverlayEdge::getEdgeRing() const
{
    return edgeRing;
}

/*public*/
const MaximalEdgeRing*
OverlayEdge::getEdgeRingMax() const
{
    return maxEdgeRing;
}

/*public*/
void
OverlayEdge::setEdgeRingMax(const MaximalEdgeRing* p_maximalEdgeRing)
{
    maxEdgeRing = p_maximalEdgeRing;
}

/*public friend*/
std::ostream&
operator<<(std::ostream& os, const OverlayEdge& oe)
{
    os << "OE( " << oe.orig();
    if (oe.pts->size() > 2) {
        os << ", " << oe.directionPt();
    }
    os << " .. " << oe.dest() << " ) ";
    oe.label->toString(oe.direction, os);
    os << oe.resultSymbol();
    os << " / Sym: ";
    oe.symOE()->getLabel()->toString(oe.symOE()->direction, os);
    os << oe.symOE()->resultSymbol();
    return os;
}

/*public*/
std::string
OverlayEdge::resultSymbol() const
{
    if (isInResultArea()) return std::string(" resA");
    if (isInResultLine()) return std::string(" resL");
    return std::string("");
}



} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
