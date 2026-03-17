/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: geomgraph/DirectedEdge.java r428 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOMGRAPH_DIRECTEDEDGE_INL
#define GEOS_GEOMGRAPH_DIRECTEDEDGE_INL

#include <geos/geomgraph/DirectedEdge.h>

namespace geos {
namespace geomgraph { // geos::geomgraph

//INLINE Edge*
//DirectedEdge::getEdge() { return edge; }

INLINE void
DirectedEdge::setInResult(bool v)
{
    isInResultVar = v;
}

INLINE bool
DirectedEdge::isInResult()
{
    return isInResultVar;
}

INLINE bool
DirectedEdge::isVisited()
{
    return isVisitedVar;
}

INLINE void
DirectedEdge::setVisited(bool v)
{
    isVisitedVar = v;
}

INLINE void
DirectedEdge::setEdgeRing(EdgeRing* er)
{
    edgeRing = er;
}

INLINE EdgeRing*
DirectedEdge::getEdgeRing()
{
    return edgeRing;
}

INLINE void
DirectedEdge::setMinEdgeRing(EdgeRing* mer)
{
    minEdgeRing = mer;
}

INLINE EdgeRing*
DirectedEdge::getMinEdgeRing()
{
    return minEdgeRing;
}

INLINE int
DirectedEdge::getDepth(int position)
{
    return depth[position];
}

INLINE DirectedEdge*
DirectedEdge::getSym()
{
    return sym;
}

INLINE bool
DirectedEdge::isForward()
{
    return isForwardVar;
}

INLINE void
DirectedEdge::setSym(DirectedEdge* de)
{
    sym = de;
}

INLINE DirectedEdge*
DirectedEdge::getNext()
{
    return next;
}

INLINE void
DirectedEdge::setNext(DirectedEdge* newNext)
{
    next = newNext;
}

INLINE DirectedEdge*
DirectedEdge::getNextMin()
{
    return nextMin;
}

INLINE void
DirectedEdge::setNextMin(DirectedEdge* nm)
{
    nextMin = nm;
}


} // namespace geos::geomgraph
} // namespace geos

#endif // GEOS_GEOMGRAPH_DIRECTEDEDGE_INL
