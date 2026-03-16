/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006      Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/SegmentNode.java 4667170ea (JTS-1.17)
 *
 **********************************************************************/

#ifndef GEOS_NODING_SEGMENTNODE_H
#define GEOS_NODING_SEGMENTNODE_H

#include <geos/export.h>

#include <vector>
#include <iostream>

#include <geos/inline.h>

#include <geos/geom/Coordinate.h>

// Forward declarations
namespace geos {
namespace noding {
class NodedSegmentString;
}
}

namespace geos {
namespace noding { // geos.noding

/**
 * \brief
 * Represents an intersection point between two NodedSegmentString.
 *
 * Final class.
 */
class GEOS_DLL SegmentNode {
private:
    const NodedSegmentString& segString;

    int segmentOctant;

    bool isInteriorVar;

    // Declare type as noncopyable
    SegmentNode(const SegmentNode& other) = delete;
    SegmentNode& operator=(const SegmentNode& rhs) = delete;

public:
    friend std::ostream& operator<< (std::ostream& os, const SegmentNode& n);

    /// the point of intersection (own copy)
    geom::Coordinate coord;

    /// the index of the containing line segment in the parent edge
    size_t segmentIndex;

    /// Construct a node on the given NodedSegmentString
    ///
    /// @param ss the parent NodedSegmentString
    ///
    /// @param nCoord the coordinate of the intersection, will be copied
    ///
    /// @param nSegmentIndex the index of the segment on parent
    ///                      NodedSegmentString
    ///        where the Node is located.
    ///
    /// @param nSegmentOctant
    ///
    SegmentNode(const NodedSegmentString& ss,
                const geom::Coordinate& nCoord,
                size_t nSegmentIndex, int nSegmentOctant);

    ~SegmentNode() {}

    /// \brief
    /// Return true if this Node is *internal* (not on the boundary)
    /// of the corresponding segment. Currently only the *first*
    /// segment endpoint is checked, actually.
    ///
    bool
    isInterior() const
    {
        return isInteriorVar;
    }

    bool isEndPoint(unsigned int maxSegmentIndex) const;

    /**
     * @return -1 this EdgeIntersection is located before
     *            the argument location
     * @return 0 this EdgeIntersection is at the argument location
     * @return 1 this EdgeIntersection is located after the
     *           argument location
     */
    int compareTo(const SegmentNode& other);

    //string print() const;
};

std::ostream& operator<< (std::ostream& os, const SegmentNode& n);

struct GEOS_DLL  SegmentNodeLT {
    bool
    operator()(SegmentNode* s1, SegmentNode* s2) const
    {
        return s1->compareTo(*s2) < 0;
    }
};


} // namespace geos.noding
} // namespace geos

#endif // GEOS_NODING_SEGMENTNODE_H
