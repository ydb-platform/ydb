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
 *  Last port: noding/SegmentPointComparator.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_NODING_SEGMENTPOINTCOMPARATOR_H
#define GEOS_NODING_SEGMENTPOINTCOMPARATOR_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>
#include <cassert>

namespace geos {
namespace noding { // geos.noding

/** \brief
 * Implements a robust method of comparing the relative position of two
 * points along the same segment.
 *
 * The coordinates are assumed to lie "near" the segment.
 * This means that this algorithm will only return correct results
 * if the input coordinates
 * have the same precision and correspond to rounded values
 * of exact coordinates lying on the segment.
 *
 */
class GEOS_DLL SegmentPointComparator {

public:

    /** \brief
     * Compares two Coordinates for their relative position along a
     * segment lying in the specified Octant.
     *
     * @return -1 node0 occurs first
     * @return 0 the two nodes are equal
     * @return 1 node1 occurs first
     */
    static int
    compare(int octant, const geom::Coordinate& p0,
            const geom::Coordinate& p1)
    {
        // nodes can only be equal if their coordinates are equal
        if(p0.equals2D(p1)) {
            return 0;
        }

        int xSign = relativeSign(p0.x, p1.x);
        int ySign = relativeSign(p0.y, p1.y);

        switch(octant) {
        case 0:
            return compareValue(xSign, ySign);
        case 1:
            return compareValue(ySign, xSign);
        case 2:
            return compareValue(ySign, -xSign);
        case 3:
            return compareValue(-xSign, ySign);
        case 4:
            return compareValue(-xSign, -ySign);
        case 5:
            return compareValue(-ySign, -xSign);
        case 6:
            return compareValue(-ySign, xSign);
        case 7:
            return compareValue(xSign, -ySign);
        }
        assert(0); // invalid octant value
        return 0;

    }

    static int
    relativeSign(double x0, double x1)
    {
        if(x0 < x1) {
            return -1;
        }
        if(x0 > x1) {
            return 1;
        }
        return 0;
    }

    static int
    compareValue(int compareSign0, int compareSign1)
    {
        if(compareSign0 < 0) {
            return -1;
        }
        if(compareSign0 > 0) {
            return 1;
        }
        if(compareSign1 < 0) {
            return -1;
        }
        if(compareSign1 > 0) {
            return 1;
        }
        return 0;
    }

};

} // namespace geos.noding
} // namespace geos

#endif // GEOS_NODING_SEGMENTPOINTCOMPARATOR_H
