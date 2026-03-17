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
 * Last port: noding/FastNodingValidator.java rev. ??? (JTS-1.8)
 *
 **********************************************************************/

#ifndef GEOS_NODING_FASTNODINGVALIDATOR_H
#define GEOS_NODING_FASTNODINGVALIDATOR_H

#include <geos/noding/NodingIntersectionFinder.h> // for composition
#include <geos/algorithm/LineIntersector.h> // for composition

#include <memory>
#include <string>
#include <cassert>

// Forward declarations
namespace geos {
namespace noding {
class SegmentString;
}
}

namespace geos {
namespace noding { // geos.noding

/** \brief
 * Validates that a collection of {@link SegmentString}s is correctly noded.
 *
 * Indexing is used to improve performance.
 * By default validation stops after a single
 * non-noded intersection is detected.
 * Alternatively, it can be requested to detect all intersections by
 * using `setFindAllIntersections(boolean)`.
 *
 * The validator does not check for topology collapse situations
 * (e.g. where two segment strings are fully co-incident).
 *
 * The validator checks for the following situations which indicated incorrect noding:
 *
 * - Proper intersections between segments (i.e. the intersection is interior to both segments)
 * - Intersections at an interior vertex (i.e. with an endpoint or another interior vertex)
 *
 * The client may either test the {@link #isValid()} condition,
 * or request that a suitable [TopologyException](@ref util::TopologyException) be thrown.
 *
 */
class FastNodingValidator {

public:

    FastNodingValidator(std::vector<noding::SegmentString*>& newSegStrings)
        :
        li(), // robust...
        segStrings(newSegStrings),
        segInt(),
        isValidVar(true)
    {
    }

    /** \brief
     * Checks for an intersection and
     * reports if one is found.
     *
     * @return true if the arrangement contains an interior intersection
     */
    bool
    isValid()
    {
        execute();
        return isValidVar;
    }

    /** \brief
     * Returns an error message indicating the segments containing
     * the intersection.
     *
     * @return an error message documenting the intersection location
     */
    std::string getErrorMessage() const;

    /** \brief
     * Checks for an intersection and throws
     * a TopologyException if one is found.
     *
     * @throws TopologyException if an intersection is found
     */
    void checkValid();

private:

    geos::algorithm::LineIntersector li;

    std::vector<noding::SegmentString*>& segStrings;

    std::unique_ptr<NodingIntersectionFinder> segInt;

    bool isValidVar;

    void
    execute()
    {
        if(segInt.get() != nullptr) {
            return;
        }
        checkInteriorIntersections();
    }

    void checkInteriorIntersections();

    // Declare type as noncopyable
    FastNodingValidator(const FastNodingValidator& other) = delete;
    FastNodingValidator& operator=(const FastNodingValidator& rhs) = delete;
};

} // namespace geos.noding
} // namespace geos

#endif // GEOS_NODING_FASTNODINGVALIDATOR_H
