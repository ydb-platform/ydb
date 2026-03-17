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
 * Last port: noding/snapround/SnapRoundingNoder.java r320 (JTS-1.12)
 *
 **********************************************************************/

#pragma once

#include <geos/export.h>

#include <geos/geom/Coordinate.h>
#include <geos/geom/Envelope.h>
#include <geos/noding/snapround/HotPixelIndex.h>
#include <geos/noding/Noder.h>


// Forward declarations
namespace geos {
namespace geom {
class Envelope;
class PrecisionModel;
}
namespace noding {
class NodedSegmentString;
}
}

namespace geos {
namespace noding { // geos::noding
namespace snapround { // geos::noding::snapround

/**
 * Uses Snap Rounding to compute a rounded,
 * fully noded arrangement from a set of {@link noding::SegmentString}s,
 * in a performant way, and avoiding unnecessary noding.
 *
 * Implements the Snap Rounding technique described in
 * the papers by Hobby, Guibas &amp; Marimont, and Goodrich et al.
 * Snap Rounding enforces that all output vertices lie on a uniform grid,
 * which is determined by the provided {@link geom::PrecisionModel}.
 *
 * Input vertices do not have to be rounded to the grid beforehand;
 * this is done during the snap-rounding process.
 * In fact, rounding cannot be done a priori,
 * since rounding vertices by themselves can distort the rounded topology
 * of the arrangement (i.e. by moving segments away from hot pixels
 * that would otherwise intersect them, or by moving vertices
 * across segments).
 *
 * To minimize the number of introduced nodes,
 * the Snap-Rounding Noder avoids creating nodes
 * at edge vertices if there is no intersection or snap at that location.
 * However, if two different input edges contain identical segments,
 * each of the segment vertices will be noded.
 * This still provides fully-noded output.
 * This is the same behaviour provided by other noders,
 * such as {@link noding::MCIndexNoder} and {@link noding::snap::SnappingNoder}.
 */
class GEOS_DLL SnapRoundingNoder : public Noder {

private:

    // Members
    const geom::PrecisionModel* pm;
    noding::snapround::HotPixelIndex pixelIndex;
    std::vector<SegmentString*> snappedResult;

    // Methods
    void snapRound(std::vector<SegmentString*>& inputSegStrings, std::vector<SegmentString*>& resultNodedSegments);

    /**
    * Creates HotPixels for each vertex in the input segStrings.
    * The HotPixels are not marked as nodes, since they will
    * only be nodes in the final line arrangement
    * if they interact with other segments (or they are already
    * created as intersection nodes).
    */
    void addVertexPixels(std::vector<SegmentString*>& segStrings);

    /**
    * Detects interior intersections in the collection of {@link SegmentString}s,
    * and adds nodes for them to the segment strings.
    * Also creates HotPixel nodes for the intersection points.
    */
    void addIntersectionPixels(std::vector<SegmentString*>& segStrings);

    void round(const geom::Coordinate& pt, geom::Coordinate& ptOut);

    /**
    * Gets a list of the rounded coordinates.
    * Duplicate (collapsed) coordinates are removed.
    *
    * @param pts the coordinates to round
    * @return array of rounded coordinates
    */
    std::unique_ptr<std::vector<geom::Coordinate>> round(const std::vector<geom::Coordinate>& pts);

    /**
    * Computes new segment strings which are rounded and contain
    * intersections added as a result of snapping segments to snap points (hot pixels).
    *
    * @param segStrings segments to snap
    * @return the snapped segment strings
    */
    void computeSnaps(const std::vector<SegmentString*>& segStrings, std::vector<SegmentString*>& snapped);
    NodedSegmentString* computeSegmentSnaps(NodedSegmentString* ss);

    /**
    * Snaps a segment in a segmentString to HotPixels that it intersects.
    *
    * @param p0 the segment start coordinate
    * @param p1 the segment end coordinate
    * @param ss the segment string to add intersections to
    * @param segIndex the index of the segment
    */
    void snapSegment(geom::Coordinate& p0, geom::Coordinate& p1, NodedSegmentString* ss, size_t segIndex);

    /**
    * Add nodes for any vertices in hot pixels that were
    * added as nodes during segment noding.
    */
    void addVertexNodeSnaps(NodedSegmentString* ss);

    void snapVertexNode(const geom::Coordinate& p0, NodedSegmentString* ss, size_t segIndex);

public:

    SnapRoundingNoder(const geom::PrecisionModel* p_pm)
        : pm(p_pm)
        , pixelIndex(p_pm)
        {}

    /**
    * @return a Collection of NodedSegmentStrings representing the substrings
    */
    std::vector<SegmentString*>* getNodedSubstrings() const override;

    /**
    * Computes the nodes in the snap-rounding line arrangement.
    * The nodes are added to the {@link NodedSegmentString}s provided as the input.
    */
    void computeNodes(std::vector<SegmentString*>* inputSegStrings) override; //override

};


} // namespace geos::noding::snapround
} // namespace geos::noding
} // namespace geos



