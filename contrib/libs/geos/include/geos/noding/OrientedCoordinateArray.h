/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009    Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/OrientedCoordinateArray.java rev. 1.1 (JTS-1.9)
 *
 **********************************************************************/

#ifndef GEOS_NODING_ORIENTEDCOORDINATEARRAY_H
#define GEOS_NODING_ORIENTEDCOORDINATEARRAY_H

#include <geos/export.h>

#include <cstddef>

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
}
namespace noding {
//class SegmentString;
}
}

namespace geos {
namespace noding { // geos.noding

/** \brief
 * Allows comparing {@link geom::CoordinateSequence}s
 * in an orientation-independent way.
 */
class GEOS_DLL OrientedCoordinateArray {
public:

    /**
     * Creates a new {@link OrientedCoordinateArray}
     * for the given {@link geom::CoordinateSequence}.
     *
     * @param p_pts the coordinates to orient
     */
    OrientedCoordinateArray(const geom::CoordinateSequence& p_pts)
        :
        pts(&p_pts),
        orientationVar(orientation(p_pts))
    {
    }

    /** \brief
     * Compares two {@link OrientedCoordinateArray}s for their
     * relative order
     *
     * @return -1 this one is smaller
     * @return 0 the two objects are equal
     * @return 1 this one is greater
     *
     * In JTS, this is used automatically by ordered lists.
     * In C++, operator< would be used instead....
     */
    int compareTo(const OrientedCoordinateArray& o1) const;

    bool operator==(const OrientedCoordinateArray& other) const;

    struct GEOS_DLL HashCode {
        size_t operator()(const OrientedCoordinateArray & oca) const;
    };

private:

    static int compareOriented(const geom::CoordinateSequence& pts1,
                               bool orientation1,
                               const geom::CoordinateSequence& pts2,
                               bool orientation2);


    /**
     * Computes the canonical orientation for a coordinate array.
     *
     * @param pts the array to test
     * @return <code>true</code> if the points are oriented forwards
     * @return <code>false</code> if the points are oriented in reverse
     */
    static bool orientation(const geom::CoordinateSequence& pts);

    /// Externally owned
    const geom::CoordinateSequence* pts;

    bool orientationVar;

};

/// Strict weak ordering operator for OrientedCoordinateArray
///
/// This is the C++ equivalent of JTS's compareTo
inline bool
operator< (const OrientedCoordinateArray& oca1,
           const OrientedCoordinateArray& oca2)
{
    return oca1.compareTo(oca2) < 0;
}

} // namespace geos.noding
} // namespace geos


#endif // GEOS_NODING_ORIENTEDCOORDINATEARRAY_H

