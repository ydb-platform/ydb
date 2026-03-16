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

#include <geos/operation/overlayng/Edge.h>
#include <geos/operation/overlayng/EdgeSourceInfo.h>
#include <geos/geom/Dimension.h>
#include <geos/geom/Location.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LineString.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/util/GEOSException.h>
#include <geos/io/WKBWriter.h>


namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;
using geos::util::GEOSException;

/*public*/
Edge::Edge(CoordinateSequence* p_pts, const EdgeSourceInfo* info)
    : aDim(OverlayLabel::DIM_UNKNOWN)
    , aDepthDelta(0)
    , aIsHole(false)
    , bDim(OverlayLabel::DIM_UNKNOWN)
    , bDepthDelta(0)
    , bIsHole(false)
    , pts(p_pts)
{
    copyInfo(info);
}

/*public static*/
bool
Edge::isCollapsed(const CoordinateSequence* pts) {
    std::size_t sz = pts->size();
    if (sz < 2)
        return true;
    // zero-length line
    if (pts->getAt(0).equals2D(pts->getAt(1)))
        return true;
    // TODO: is pts > 2 with equal points ever expected?
    if (sz > 2) {
        if (pts->getAt(sz-1).equals2D(pts->getAt(sz - 2)))
            return true;
    }
    return false;
}

/*public*/
const CoordinateSequence*
Edge::getCoordinatesRO() const
{
    return pts.get();
}

/*public*/
std::unique_ptr<CoordinateSequence>
Edge::getCoordinates()
{
    // std::unique_ptr<CoordinateSequence> tmp = std::move(pts);
    // pts.reset(nullptr);
    return pts->clone();
}

/*public*/
geom::CoordinateSequence*
Edge::releaseCoordinates()
{
    CoordinateSequence* cs = pts.release();
    pts.reset(nullptr);
    return cs;
}


/*public*/
const Coordinate&
Edge::getCoordinate(size_t index)  const
{
    return pts->getAt(index);
}

/*public*/
std::size_t
Edge::size() const
{
    return pts->size();
}

/*public*/
bool
Edge::direction() const
{
    if (pts->size() < 2) {
        throw GEOSException("Edge must have >= 2 points");
    }

    const Coordinate& p0 = pts->getAt(0);
    const Coordinate& p1 = pts->getAt(1);
    const Coordinate& pn0 = pts->getAt(pts->size() - 1);
    const Coordinate& pn1 = pts->getAt(pts->size() - 2);

    int cmp = 0;
    int cmp0 = p0.compareTo(pn0);
    if (cmp0 != 0) cmp = cmp0;

    if (cmp == 0) {
        int cmp1 = p1.compareTo(pn1);
        if (cmp1 != 0) cmp = cmp1;
    }

    if (cmp == 0) {
        throw GEOSException("Edge direction cannot be determined because endpoints are equal");
    }

    return cmp == -1 ? true : false;
}

/**
* Compares two coincident edges to determine
* whether they have the same or opposite direction.
*/
/*public*/
bool
Edge::relativeDirection(const Edge* edge2) const
{
    // assert: the edges match (have the same coordinates up to direction)
    if (!getCoordinate(0).equals2D(edge2->getCoordinate(0))) {
      return false;
    }
    if (!getCoordinate(1).equals2D(edge2->getCoordinate(1))) {
      return false;
    }
    return true;
}

/*public*/
int
Edge::dimension(int geomIndex) const
{
    if (geomIndex == 0) return aDim;
    return bDim;
}


/**
* Populates the label for an edge resulting from an input geometry.
*  - If the edge is not part of the input, the label is left as NOT_PART
*  - If input is an Area and the edge is on the boundary
* (which may include some collapses),
* edge is marked as an AREA edge and side locations are assigned
*  - If input is an Area and the edge is collapsed
* (depth delta = 0),
* the label is set to COLLAPSE.
* The location will be determined later
* by evaluating the final graph topology.
*  - If input is a Line edge is set to a LINE edge.
* For line edges the line location is not significant
* (since there is no parent area for which to determine location).
*/
/*private*/
void
Edge::initLabel(OverlayLabel& lbl, int geomIndex, int dim, int depthDelta, bool p_isHole) const
{
    int dimLabel = labelDim(dim, depthDelta);

    switch (dimLabel) {
        case OverlayLabel::DIM_NOT_PART: {
            lbl.initNotPart(geomIndex);
            break;
        }
        case OverlayLabel::DIM_BOUNDARY: {
            lbl.initBoundary(geomIndex, locationLeft(depthDelta), locationRight(depthDelta), p_isHole);
            break;
        }
        case OverlayLabel::DIM_COLLAPSE: {
            lbl.initCollapse(geomIndex, p_isHole);
            break;
        }
        case OverlayLabel::DIM_LINE: {
            lbl.initLine(geomIndex);
            break;
        }
    }
}

/*private*/
int
Edge::labelDim(int dim, int depthDelta) const
{
    if (dim == Dimension::False)
        return OverlayLabel::DIM_NOT_PART;

    if (dim == Dimension::L)
        return OverlayLabel::DIM_LINE;

    // assert: dim is A
    bool isCollapse = (depthDelta == 0);
    if (isCollapse)
        return OverlayLabel::DIM_COLLAPSE;

    return OverlayLabel::DIM_BOUNDARY;
}

/*private*/
bool
Edge::isHole(int index) const
{
    if (index == 0)
        return aIsHole;
    return bIsHole;
}


/*private*/
bool
Edge::isBoundary(int geomIndex) const
{
    if (geomIndex == 0)
        return aDim == OverlayLabel::DIM_BOUNDARY;
    return bDim == OverlayLabel::DIM_BOUNDARY;
}

/**
* Tests whether the edge is part of a shell in the given geometry.
* This is only the case if the edge is a boundary.
*/
/*private*/
bool
Edge::isShell(int geomIndex) const
{
    if (geomIndex == 0) {
        return (aDim == OverlayLabel::DIM_BOUNDARY && ! aIsHole);
    }
    return (bDim == OverlayLabel::DIM_BOUNDARY && ! bIsHole);
}

/*private*/
Location
Edge::locationRight(int depthDelta) const
{
    int sgn = delSign(depthDelta);
    switch (sgn) {
        case 0: return Location::NONE;
        case 1: return Location::INTERIOR;
        case -1: return Location::EXTERIOR;
    }
    return Location::NONE;
}

/*private*/
Location
Edge::locationLeft(int depthDelta) const
{
    // TODO: is it always safe to ignore larger depth deltas?
    int sgn = delSign(depthDelta);
    switch (sgn) {
        case 0: return Location::NONE;
        case 1: return Location::EXTERIOR;
        case -1: return Location::INTERIOR;
    }
    return Location::NONE;
}

/*private*/
int
Edge::delSign(int depthDel) const
{
    if (depthDel > 0) return 1;
    if (depthDel < 0) return -1;
    return 0;
}

/*private*/
void
Edge::copyInfo(const EdgeSourceInfo* info)
{
    if (info->getIndex() == 0) {
        aDim = info->getDimension();
        aIsHole = info->isHole();
        aDepthDelta = info->getDepthDelta();
    }
    else {
        bDim = info->getDimension();
        bIsHole = info->isHole();
        bDepthDelta = info->getDepthDelta();
    }
}

/**
* Merges an edge into this edge,
* updating the topology info accordingly.
*/
/*public*/
void
Edge::merge(const Edge* edge)
{
    /**
     * Marks this
     * as a shell edge if any contributing edge is a shell.
     * Update hole status first, since it depends on edge dim
     */
    aIsHole = isHoleMerged(0, this, edge);
    bIsHole = isHoleMerged(1, this, edge);

    if (edge->aDim > aDim) aDim = edge->aDim;
    if (edge->bDim > bDim) bDim = edge->bDim;

    bool relDir = relativeDirection(edge);
    int flipFactor = relDir ? 1 : -1;
    aDepthDelta += flipFactor * edge->aDepthDelta;
    bDepthDelta += flipFactor * edge->bDepthDelta;
}

/*private*/
bool
Edge::isHoleMerged(int geomIndex, const Edge* edge1, const Edge* edge2) const
{
    // TOD: this might be clearer with tri-state logic for isHole?
    bool isShell1 = edge1->isShell(geomIndex);
    bool isShell2 = edge2->isShell(geomIndex);
    bool isShellMerged = isShell1 || isShell2;
    // flip since isHole is stored
    return !isShellMerged;
}

/*public*/
void
Edge::populateLabel(OverlayLabel &lbl) const
{
    initLabel(lbl, 0, aDim, aDepthDelta, aIsHole);
    initLabel(lbl, 1, bDim, bDepthDelta, bIsHole);
    return;
}


/*public friend*/
std::ostream&
operator<<(std::ostream& os, const Edge& e)
{
    auto gf = GeometryFactory::create();
    auto cs = e.getCoordinatesRO();
    auto line = gf->createLineString(cs->clone());
    io::WKBWriter w;
    w.writeHEX(*line, os);
    return os;
}


bool EdgeComparator(const Edge* a, const Edge* b)
{
    return a->compareTo(*b);
}


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
