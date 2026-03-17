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

#ifndef GEOS_GEOMGRAPH_INDEX_SEGMENTINTERSECTOR_INL
#define GEOS_GEOMGRAPH_INDEX_SEGMENTINTERSECTOR_INL

namespace geos {
namespace geomgraph {
namespace index {

INLINE bool
SegmentIntersector::isAdjacentSegments(size_t i1, size_t i2)
{
    return (i1 > i2 ? i1 - i2 : i2 - i1) == 1;
}

INLINE void
SegmentIntersector::setBoundaryNodes(std::vector<Node*>* bdyNodes0,
                                     std::vector<Node*>* bdyNodes1)
{
    bdyNodes[0] = bdyNodes0;
    bdyNodes[1] = bdyNodes1;
}

/*
* @return the proper intersection point, or <code>null</code>
* if none was found
*/
INLINE geom::Coordinate&
SegmentIntersector::getProperIntersectionPoint()
{
    return properIntersectionPoint;
}

INLINE bool
SegmentIntersector::hasIntersection()
{
    return hasIntersectionVar;
}

INLINE void
SegmentIntersector::setIsDoneIfProperInt(bool idwpi)
{
    isDoneWhenProperInt = idwpi;
}

INLINE bool
SegmentIntersector::getIsDone()
{
    return isDone;
}

/*
 * A proper intersection is an intersection which is interior to at least two
 * line segments.  Note that a proper intersection is not necessarily
 * in the interior of the entire Geometry, since another edge may have
 * an endpoint equal to the intersection, which according to SFS semantics
 * can result in the point being on the Boundary of the Geometry.
 */
INLINE bool
SegmentIntersector::hasProperIntersection()
{
    return hasProper;
}

/*
 * A proper interior intersection is a proper intersection which is <b>not</b>
 * contained in the set of boundary nodes set for this SegmentIntersector.
 */
INLINE bool
SegmentIntersector::hasProperInteriorIntersection()
{
    return hasProperInterior;
}

/*private*/
INLINE bool
SegmentIntersector::isBoundaryPoint(algorithm::LineIntersector* p_li,
                                    std::array<std::vector<Node*>*, 2>& tstBdyNodes)
{
    return isBoundaryPoint(p_li, tstBdyNodes[0]) || isBoundaryPoint(p_li, tstBdyNodes[1]);
}


}
}
}

#endif
