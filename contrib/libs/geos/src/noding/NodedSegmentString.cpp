/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/NodedSegmentString.java r320 (JTS-1.12)
 *
 **********************************************************************/


#include <geos/noding/NodedSegmentString.h>
#include <geos/noding/Octant.h>
#include <geos/algorithm/LineIntersector.h>

using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace noding { // geos::noding

const SegmentNodeList&
NodedSegmentString::getNodeList() const
{
    return nodeList;
}

SegmentNodeList&
NodedSegmentString::getNodeList()
{
    return nodeList;
}

/*static private*/
int
NodedSegmentString::safeOctant(const Coordinate& p0, const Coordinate& p1)
{
    if(p0.equals2D(p1)) {
        return 0;
    }
    return Octant::octant(p0, p1);
}


/*public*/
int
NodedSegmentString::getSegmentOctant(size_t index) const
{
    if(index >= size() - 1) {
        return -1;
    }
    return safeOctant(getCoordinate(index), getCoordinate(index + 1));
    //return Octant::octant(getCoordinate(index), getCoordinate(index+1));
}

/*public*/
void
NodedSegmentString::addIntersections(LineIntersector* li,
                                     size_t segmentIndex, size_t geomIndex)
{
    for(size_t i = 0, n = li->getIntersectionNum(); i < n; ++i) {
        addIntersection(li, segmentIndex, geomIndex, i);
    }
}

/*public*/
void
NodedSegmentString::addIntersection(LineIntersector* li,
                                    size_t segmentIndex,
                                    size_t geomIndex, size_t intIndex)
{
    ::geos::ignore_unused_variable_warning(geomIndex);

    const Coordinate& intPt = li->getIntersection(intIndex);
    addIntersection(intPt, segmentIndex);
}

/*public*/
void
NodedSegmentString::addIntersection(const Coordinate& intPt,
                                    size_t segmentIndex)
{
    size_t normalizedSegmentIndex = segmentIndex;

    if(segmentIndex > size() - 2) {
        throw util::IllegalArgumentException("SegmentString::addIntersection: SegmentIndex out of range");
    }

    // normalize the intersection point location
    auto nextSegIndex = normalizedSegmentIndex + 1;
    if(nextSegIndex < size()) {
        const Coordinate& nextPt = pts->getAt(nextSegIndex);

        // Normalize segment index if intPt falls on vertex
        // The check for point equality is 2D only -
        // Z values are ignored
        if(intPt.equals2D(nextPt)) {
            normalizedSegmentIndex = nextSegIndex;
        }
    }

    /*
     * Add the intersection point to edge intersection list
     * (unless the node is already known)
     */
    nodeList.add(intPt, normalizedSegmentIndex);
}

/* public static */
void
NodedSegmentString::getNodedSubstrings(
    const SegmentString::NonConstVect& segStrings,
    SegmentString::NonConstVect* resultEdgeList)
{
    assert(resultEdgeList);
    for(SegmentString::NonConstVect::const_iterator
            i = segStrings.begin(), iEnd = segStrings.end();
            i != iEnd; ++i) {
        NodedSegmentString* ss = dynamic_cast<NodedSegmentString*>(*i);
        assert(ss);
        ss->getNodeList().addSplitEdges(resultEdgeList);
    }
}

/* public */
std::unique_ptr<std::vector<Coordinate>>
NodedSegmentString::getNodedCoordinates() {
    return nodeList.getSplitCoordinates();
}


/* public static */
SegmentString::NonConstVect*
NodedSegmentString::getNodedSubstrings(
    const SegmentString::NonConstVect& segStrings)
{
    SegmentString::NonConstVect* resultEdgelist = \
            new SegmentString::NonConstVect();
    getNodedSubstrings(segStrings, resultEdgelist);
    return resultEdgelist;
}

/* virtual public */
const geom::Coordinate&
NodedSegmentString::getCoordinate(size_t i) const
{
    return pts->getAt(i);
}

/* virtual public */
geom::CoordinateSequence*
NodedSegmentString::getCoordinates() const
{
    return pts.get();
}

/* virtual public */
bool
NodedSegmentString::isClosed() const
{
    return pts->getAt(0) == pts->getAt(size() - 1);
}

/* public virtual */
std::ostream&
NodedSegmentString::print(std::ostream& os) const
{
    os << "NodedSegmentString: " << std::endl;
    os << " LINESTRING" << *(pts) << ";" << std::endl;
    os << " Nodes: " << nodeList.size() << std::endl;

    return os;
}


} // geos::noding
} // geos
