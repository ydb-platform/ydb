/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/BasicSegmentString.java rev. 1.1 (JTS-1.9)
 *
 **********************************************************************/

#ifndef GEOS_NODING_BASICSEGMENTSTRING_H
#define GEOS_NODING_BASICSEGMENTSTRING_H

#include <geos/export.h>
#include <geos/noding/SegmentString.h> // for inheritance
#include <geos/geom/CoordinateSequence.h> // for inlines (size())

#include <geos/inline.h>

#include <vector>

// Forward declarations

namespace geos {
namespace noding { // geos.noding

/** \brief
 * Represents a list of contiguous line segments, and supports noding the segments.
 *
 * The line segments are represented by an array of [Coordinates](@ref geom::Coordinate).
 * Intended to optimize the noding of contiguous segments by reducing the number
 * of allocated objects. SegmentStrings can carry a context object, which is
 * useful for preserving topological or parentage information.
 * All noded substrings are initialized with the same context object.
 */
class GEOS_DLL BasicSegmentString : public SegmentString {

public:

    /// \brief Construct a BasicSegmentString.
    ///
    /// @param newPts CoordinateSequence representing the string, externally owned
    /// @param newContext the context associated to this SegmentString
    ///
    BasicSegmentString(geom::CoordinateSequence* newPts,
                       const void* newContext)
        :
        SegmentString(newContext),
        pts(newPts)
    {}

    ~BasicSegmentString() override
    {}

    // see dox in SegmentString.h
    size_t
    size() const override
    {
        return pts->size();
    }

    // see dox in SegmentString.h
    const geom::Coordinate& getCoordinate(size_t i) const override;

    /// @see SegmentString::getCoordinates() const
    geom::CoordinateSequence* getCoordinates() const override;

    // see dox in SegmentString.h
    bool isClosed() const override;

    // see dox in SegmentString.h
    std::ostream& print(std::ostream& os) const override;

    /** \brief
     * Gets the octant of the segment starting at vertex index.
     *
     * @param index the index of the vertex starting the segment.
     *        Must not be the last index in the vertex list
     * @return the octant of the segment at the vertex
     */
    int getSegmentOctant(size_t index) const;

private:

    geom::CoordinateSequence* pts;

};

} // namespace geos.noding
} // namespace geos

#ifdef GEOS_INLINE
#include <geos/noding/BasicSegmentString.inl>
#endif

#endif // ndef GEOS_NODING_BASICSEGMENTSTRING_H
