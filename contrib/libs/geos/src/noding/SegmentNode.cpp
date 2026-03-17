/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006      Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/SegmentNode.java 4667170ea (JTS-1.17)
 *
 **********************************************************************/

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#include <iostream>
#include <sstream>
#include <iomanip>

#include <geos/noding/SegmentNode.h>
#include <geos/noding/SegmentPointComparator.h>
#include <geos/noding/NodedSegmentString.h>
#include <geos/geom/Coordinate.h>

using namespace std;
using namespace geos::geom;

namespace geos {
namespace noding { // geos.noding


/*public*/
SegmentNode::SegmentNode(const NodedSegmentString& ss, const Coordinate& nCoord,
                         size_t nSegmentIndex, int nSegmentOctant)
    :
    segString(ss),
    segmentOctant(nSegmentOctant),
    coord(nCoord),
    segmentIndex(nSegmentIndex)
{
    // Number of points in NodedSegmentString is one-more number of segments
    assert(segmentIndex < segString.size());

    isInteriorVar = \
                    !coord.equals2D(segString.getCoordinate(segmentIndex));

}


bool
SegmentNode::isEndPoint(unsigned int maxSegmentIndex) const
{
    if(segmentIndex == 0 && ! isInteriorVar) {
        return true;
    }
    if(segmentIndex == maxSegmentIndex) {
        return true;
    }
    return false;
}

/**
 * @return -1 this EdgeIntersection is located before the argument location
 * @return 0 this EdgeIntersection is at the argument location
 * @return 1 this EdgeIntersection is located after the argument location
 */
int
SegmentNode::compareTo(const SegmentNode& other)
{
    if(segmentIndex < other.segmentIndex) {
        return -1;
    }
    if(segmentIndex > other.segmentIndex) {
        return 1;
    }

#if GEOS_DEBUG
    cerr << setprecision(17) << "compareTo: " << *this << ", " << other << endl;
#endif

    if(coord.equals2D(other.coord)) {

#if GEOS_DEBUG
        cerr << " Coordinates equal!" << endl;
#endif

        return 0;
    }

#if GEOS_DEBUG
    cerr << " Coordinates do not equal!" << endl;
#endif

    // an exterior node is the segment start point,
    // so always sorts first
    // this guards against a robustness problem
    // where the octants are not reliable
    if (! isInteriorVar) return -1;
    if (! other.isInteriorVar) return 1;

    return SegmentPointComparator::compare(segmentOctant, coord,
                                           other.coord);
}

ostream&
operator<< (ostream& os, const SegmentNode& n)
{
    return os << n.coord << " seg#=" << n.segmentIndex << " octant#=" << n.segmentOctant << endl;
}

} // namespace geos.noding
} // namespace geos

