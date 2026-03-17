/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 *
 **********************************************************************/

#ifndef GEOS_NODING_SEGMENTSETMUTUALINTERSECTOR_H
#define GEOS_NODING_SEGMENTSETMUTUALINTERSECTOR_H

#include <geos/noding/SegmentString.h>
#include <geos/noding/SegmentIntersector.h>

namespace geos {
namespace noding { // geos::noding

/** \brief
 * An intersector for the red-blue intersection problem.
 *
 * In this class of line arrangement problem,
 * two disjoint sets of linestrings are provided.
 * It is assumed that within
 * each set, no two linestrings intersect except possibly at their endpoints.
 * Implementations can take advantage of this fact to optimize processing.
 *
 * @author Martin Davis
 * @version 1.10
 */
class SegmentSetMutualIntersector {
public:

    SegmentSetMutualIntersector()
        : segInt(nullptr)
    {}

    virtual
    ~SegmentSetMutualIntersector() {}

    /**
     * Sets the SegmentIntersector to use with this intersector.
     * The SegmentIntersector will either rocord or add intersection nodes
     * for the input segment strings.
     *
     * @param si the segment intersector to use
     */
    void
    setSegmentIntersector(SegmentIntersector* si)
    {
        segInt = si;
    }

    /**
     *
     * @param segStrings a collection of [SegmentStrings](@ref SegmentString) to node
     */
    virtual void setBaseSegments(SegmentString::ConstVect* segStrings) = 0;

    /**
     * Computes the intersections for two collections of [SegmentStrings](@ref SegmentString).
     *
     * @param segStrings a collection of [SegmentStrings](@ref SegmentString) to node
     */
    virtual void process(SegmentString::ConstVect* segStrings) = 0;

protected:

    SegmentIntersector* segInt;

};

} // geos::noding
} // geos

#endif // GEOS_NODING_SEGMENTSETMUTUALINTERSECTOR_H
