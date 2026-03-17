/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/geomgraph/index/SweepLineSegment.h>
#include <geos/geomgraph/index/SegmentIntersector.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Coordinate.h>
#include <geos/geomgraph/Edge.h>

using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph
namespace index { // geos.geomgraph.index

SweepLineSegment::SweepLineSegment(Edge* newEdge, size_t newPtIndex):
    edge(newEdge),
    pts(newEdge->getCoordinates()),
    ptIndex(newPtIndex)
{
    //pts=newEdge->getCoordinates();
    //edge=newEdge;
    //ptIndex=newPtIndex;
}

double
SweepLineSegment::getMinX()
{
    double x1 = pts->getAt(ptIndex).x;
    double x2 = pts->getAt(ptIndex + 1).x;
    return x1 < x2 ? x1 : x2;
}

double
SweepLineSegment::getMaxX()
{
    double x1 = pts->getAt(ptIndex).x;
    double x2 = pts->getAt(ptIndex + 1).x;
    return x1 > x2 ? x1 : x2;
}

void
SweepLineSegment::computeIntersections(SweepLineSegment* ss,
                                       SegmentIntersector* si)
{
    si->addIntersections(edge, ptIndex, ss->edge, ss->ptIndex);
}

} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos
