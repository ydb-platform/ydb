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

#ifndef GEOS_NODING_NODABLESEGMENTSTRING_H
#define GEOS_NODING_NODABLESEGMENTSTRING_H

#include <geos/export.h>
#include <geos/noding/SegmentString.h> // for inheritance

namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace noding { // geos::noding

/** \brief
 * An interface for classes which support adding nodes to
 * a segment string.
 *
 * @author Martin Davis
 */
class GEOS_DLL NodableSegmentString : public SegmentString {
private:
protected:
public:
    NodableSegmentString(const void* newContext)
        :
        SegmentString(newContext)
    { }

    /**
     * Adds an intersection node for a given point and segment to this segment string.
     *
     * @param intPt the location of the intersection
     * @param segmentIndex the index of the segment containing the intersection
     */
    //virtual void addIntersection( const geom::Coordinate * intPt, int segmentIndex) =0;
};

} // namespace geos::noding
} // namespace geos

#endif // GEOS_NODING_NODABLESEGMENTSTRING_H
