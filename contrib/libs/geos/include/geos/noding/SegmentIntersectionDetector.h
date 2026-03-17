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

#ifndef GEOS_GEOM_PREP_SEGMENTINTERSECTIONDETECTOR_H
#define GEOS_GEOM_PREP_SEGMENTINTERSECTIONDETECTOR_H

#include <cstddef>
#include <geos/noding/SegmentIntersector.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/noding/SegmentString.h>

namespace geos {
namespace noding { // geos::noding

/** \brief
 * Detects and records an intersection between two {@link SegmentString}s,
 * if one exists.
 *
 * This strategy can be configured to search for proper intersections.
 * In this case, the presence of any intersection will still be recorded,
 * but searching will continue until either a proper intersection has been found
 * or no intersections are detected.
 *
 * Only a single intersection is recorded.
 *
 * @version 1.7
 */
class SegmentIntersectionDetector : public SegmentIntersector {
private:
    algorithm::LineIntersector* li;

    bool findProper;
    bool findAllTypes;

    bool _hasIntersection;
    bool _hasProperIntersection;
    bool _hasNonProperIntersection;

    const geom::Coordinate* intPt;
    geom::CoordinateArraySequence* intSegments;

protected:
public:
    SegmentIntersectionDetector(algorithm::LineIntersector* p_li)
        :
        li(p_li),
        findProper(false),
        findAllTypes(false),
        _hasIntersection(false),
        _hasProperIntersection(false),
        _hasNonProperIntersection(false),
        intPt(nullptr),
        intSegments(nullptr)
    { }

    ~SegmentIntersectionDetector() override
    {
        //delete intPt;
        delete intSegments;
    }


    void
    setFindProper(bool p_findProper)
    {
        this->findProper = p_findProper;
    }

    void
    setFindAllIntersectionTypes(bool p_findAllTypes)
    {
        this->findAllTypes = p_findAllTypes;
    }

    /** \brief
     * Tests whether an intersection was found.
     *
     * @return true if an intersection was found
     */
    bool
    hasIntersection() const
    {
        return _hasIntersection;
    }

    /** \brief
     * Tests whether a proper intersection was found.
     *
     * @return `true` if a proper intersection was found
     */
    bool
    hasProperIntersection() const
    {
        return _hasProperIntersection;
    }

    /** \brief
     * Tests whether a non-proper intersection was found.
     *
     * @return true if a non-proper intersection was found
     */
    bool
    hasNonProperIntersection() const
    {
        return _hasNonProperIntersection;
    }

    /** \brief
    * Gets the computed location of the intersection.
    * Due to round-off, the location may not be exact.
    *
    * @return the coordinate for the intersection location
    */
    const geom::Coordinate*
    getIntersection()  const
    {
        return intPt;
    }


    /** \brief
     * Gets the endpoints of the intersecting segments.
     *
     * @return an array of the segment endpoints (p00, p01, p10, p11)
     */
    const geom::CoordinateSequence*
    getIntersectionSegments() const
    {
        return intSegments;
    }

    bool
    isDone() const override
    {
        // If finding all types, we can stop
        // when both possible types have been found.
        if(findAllTypes) {
            return _hasProperIntersection && _hasNonProperIntersection;
        }

        // If searching for a proper intersection, only stop if one is found
        if(findProper) {
            return _hasProperIntersection;
        }

        return _hasIntersection;
    }

    /** \brief
     * This method is called by clients of the SegmentIntersector class to process
     * intersections for two segments of the {@link SegmentString}s being intersected.
     *
     * @note Some clients (such as `MonotoneChains`) may optimize away
     *       this call for segment pairs which they have determined do not intersect
     *       (e.g. by an disjoint envelope test).
     */
    void processIntersections(noding::SegmentString* e0, size_t segIndex0,
                              noding::SegmentString* e1, size_t segIndex1) override;

};

} // namespace geos::noding
} // namespace geos

#endif // GEOS_GEOM_PREP_SEGMENTINTERSECTIONDETECTOR_H
