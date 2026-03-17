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
 * Last port: noding/snapround/MCIndexSnapRounder.java r486 (JTS-1.12+)
 *
 **********************************************************************/

#ifndef GEOS_NODING_SNAPROUND_MCINDEXSNAPROUNDER_H
#define GEOS_NODING_SNAPROUND_MCINDEXSNAPROUNDER_H

#include <geos/export.h>

#include <geos/noding/Noder.h> // for inheritance
#include <geos/noding/NodedSegmentString.h> // for inlines
#include <geos/noding/snapround/MCIndexPointSnapper.h> // for inines
#include <geos/algorithm/LineIntersector.h> // for composition
#include <geos/geom/Coordinate.h> // for use in vector
#include <geos/geom/PrecisionModel.h> // for inlines

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace algorithm {
class LineIntersector;
}
namespace noding {
class SegmentString;
class MCIndexNoder;
}
}

namespace geos {
namespace noding { // geos::noding
namespace snapround { // geos::noding::snapround


/** \brief
 * Uses Snap Rounding to compute a rounded,
 * fully noded arrangement from a set of SegmentString
 *
 * Implements the Snap Rounding technique described in Hobby, Guibas & Marimont,
 * and Goodrich et al.
 *
 * Snap Rounding assumes that all vertices lie on a uniform grid
 * (hence the precision model of the input must be fixed precision,
 * and all the input vertices must be rounded to that precision).
 *
 * This implementation uses a monotone chains and a spatial index to
 * speed up the intersection tests.
 *
 * This implementation appears to be fully robust using an integer
 * precision model.
 *
 * It will function with non-integer precision models, but the
 * results are not 100% guaranteed to be correctly noded.
 */
class GEOS_DLL MCIndexSnapRounder: public Noder { // implments Noder

public:

    MCIndexSnapRounder(const geom::PrecisionModel& nPm)
        :
        pm(nPm),
        scaleFactor(nPm.getScale()),
        pointSnapper(nullptr)
    {
        li.setPrecisionModel(&pm);
    }

    std::vector<SegmentString*>*
    getNodedSubstrings() const override
    {
        return NodedSegmentString::getNodedSubstrings(*nodedSegStrings);
    }

    void computeNodes(std::vector<SegmentString*>* segStrings) override;

    /**
     * Computes nodes introduced as a result of
     * snapping segments to vertices of other segments
     *
     * @param edges the list of segment strings to snap together
     *        NOTE: they *must* be instances of NodedSegmentString, or
     * 	            an assertion will fail.
     */
    void computeVertexSnaps(std::vector<SegmentString*>& edges);

private:

    /// externally owned
    const geom::PrecisionModel& pm;

    algorithm::LineIntersector li;

    double scaleFactor;

    std::vector<SegmentString*>* nodedSegStrings;

    std::unique_ptr<MCIndexPointSnapper> pointSnapper;

    void snapRound(MCIndexNoder& noder, std::vector<SegmentString*>* segStrings);


    /**
     * Computes all interior intersections in the collection of SegmentStrings,
     * and push their Coordinate to the provided vector.
     *
     * Does NOT node the segStrings.
     *
     */
    void findInteriorIntersections(MCIndexNoder& noder,
                                   std::vector<SegmentString*>* segStrings,
                                   std::vector<geom::Coordinate>& intersections);

    /**
     * Computes nodes introduced as a result of snapping
     * segments to snap points (hot pixels)
     */
    void computeIntersectionSnaps(std::vector<geom::Coordinate>& snapPts);

    /**
     * Performs a brute-force comparison of every segment in each {@link SegmentString}.
     * This has n^2 performance.
     */
    void computeVertexSnaps(NodedSegmentString* e);

    void checkCorrectness(std::vector<SegmentString*>& inputSegmentStrings);

    // Declare type as noncopyable
    MCIndexSnapRounder(const MCIndexSnapRounder& other) = delete;
    MCIndexSnapRounder& operator=(const MCIndexSnapRounder& rhs) = delete;
};

} // namespace geos::noding::snapround
} // namespace geos::noding
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_NODING_SNAPROUND_MCINDEXSNAPROUNDER_H
