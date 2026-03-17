/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: simplify/TaggedLineStringSimplifier.java r536 (JTS-1.12+)
 *
 **********************************************************************
 *
 * NOTES: This class can be optimized to work with vector<Coordinate*>
 *        rather then with CoordinateSequence
 *
 **********************************************************************/

#ifndef GEOS_SIMPLIFY_TAGGEDLINESTRINGSIMPLIFIER_H
#define GEOS_SIMPLIFY_TAGGEDLINESTRINGSIMPLIFIER_H

#include <geos/export.h>
#include <cstddef>
#include <vector>
#include <memory>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace algorithm {
class LineIntersector;
}
namespace geom {
class CoordinateSequence;
class LineSegment;
}
namespace simplify {
class TaggedLineSegment;
class TaggedLineString;
class LineSegmentIndex;
}
}

namespace geos {
namespace simplify { // geos::simplify


/** \brief
 * Simplifies a TaggedLineString, preserving topology
 * (in the sense that no new intersections are introduced).
 * Uses the recursive Douglas-Peucker algorithm.
 *
 */
class GEOS_DLL TaggedLineStringSimplifier {

public:

    TaggedLineStringSimplifier(LineSegmentIndex* inputIndex,
                               LineSegmentIndex* outputIndex);

    /** \brief
     * Sets the distance tolerance for the simplification.
     *
     * All vertices in the simplified geometry will be within this
     * distance of the original geometry.
     *
     * @param d the approximation tolerance to use
     */
    void setDistanceTolerance(double d);

    /**
     * Simplifies the given {@link TaggedLineString}
     * using the distance tolerance specified.
     *
     * @param line the linestring to simplify
     */
    void simplify(TaggedLineString* line);


private:

    // externally owned
    LineSegmentIndex* inputIndex;

    // externally owned
    LineSegmentIndex* outputIndex;

    std::unique_ptr<algorithm::LineIntersector> li;

    /// non-const as segments are possibly added to it
    TaggedLineString* line;

    const geom::CoordinateSequence* linePts;

    double distanceTolerance;

    void simplifySection(std::size_t i, std::size_t j,
                         std::size_t depth);

    static std::size_t findFurthestPoint(
        const geom::CoordinateSequence* pts,
        std::size_t i, std::size_t j,
        double& maxDistance);

    bool hasBadIntersection(const TaggedLineString* parentLine,
                            const std::pair<std::size_t, std::size_t>& sectionIndex,
                            const geom::LineSegment& candidateSeg);

    bool hasBadInputIntersection(const TaggedLineString* parentLine,
                                 const std::pair<std::size_t, std::size_t>& sectionIndex,
                                 const geom::LineSegment& candidateSeg);

    bool hasBadOutputIntersection(const geom::LineSegment& candidateSeg);

    bool hasInteriorIntersection(const geom::LineSegment& seg0,
                                 const geom::LineSegment& seg1) const;

    std::unique_ptr<TaggedLineSegment> flatten(
        std::size_t start, std::size_t end);

    /** \brief
     * Tests whether a segment is in a section of a TaggedLineString
     *
     * @param parentLine
     * @param sectionIndex
     * @param seg
     * @return
     */
    static bool isInLineSection(
        const TaggedLineString* parentLine,
        const std::pair<std::size_t, std::size_t>& sectionIndex,
        const TaggedLineSegment* seg);

    /** \brief
     * Remove the segs in the section of the line
     *
     * @param line
     * @param start
     * @param end
     */
    void remove(const TaggedLineString* line,
                std::size_t start,
                std::size_t end);

};

inline void
TaggedLineStringSimplifier::setDistanceTolerance(double d)
{
    distanceTolerance = d;
}

} // namespace geos::simplify
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_SIMPLIFY_TAGGEDLINESTRINGSIMPLIFIER_H

