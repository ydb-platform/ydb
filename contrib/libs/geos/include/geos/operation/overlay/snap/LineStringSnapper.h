/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009-2010  Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/snap/LineStringSnapper.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_OVERLAY_SNAP_LINESTRINGSNAPPER_H
#define GEOS_OP_OVERLAY_SNAP_LINESTRINGSNAPPER_H

#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateList.h>

#include <memory>

// Forward declarations
namespace geos {
namespace geom {
//class PrecisionModel;
//class CoordinateSequence;
class CoordinateList;
class Geometry;
}
}

namespace geos {
namespace operation { // geos::operation
namespace overlay { // geos::operation::overlay
namespace snap { // geos::operation::overlay::snap

/** \brief
 * Snaps the vertices and segments of a LineString to a set
 * of target snap vertices.
 *
 * A snapping distance tolerance is used to control where snapping is performed.
 *
 */
class GEOS_DLL LineStringSnapper {

public:

    /**
     * Creates a new snapper using the given points
     * as source points to be snapped.
     *
     * @param nSrcPts the points to snap
     * @param nSnapTol the snap tolerance to use
     */
    LineStringSnapper(const geom::Coordinate::Vect& nSrcPts,
                      double nSnapTol)
        :
        srcPts(nSrcPts),
        snapTolerance(nSnapTol),
        allowSnappingToSourceVertices(false)
    {
        size_t s = srcPts.size();
        isClosed = s < 2 ? false : srcPts[0].equals2D(srcPts[s - 1]);
    }

    // Snap points are assumed to be all distinct points (a set would be better, uh ?)
    std::unique_ptr<geom::Coordinate::Vect> snapTo(const geom::Coordinate::ConstVect& snapPts);

    void
    setAllowSnappingToSourceVertices(bool allow)
    {
        allowSnappingToSourceVertices = allow;
    }

private:

    const geom::Coordinate::Vect& srcPts;

    double snapTolerance;

    bool allowSnappingToSourceVertices;
    bool isClosed;


    // Modifies first arg
    void snapVertices(geom::CoordinateList& srcCoords,
                      const geom::Coordinate::ConstVect& snapPts);


    // Returns snapPts.end() if no snap point is close enough (within snapTol distance)
    geom::Coordinate::ConstVect::const_iterator findSnapForVertex(const geom::Coordinate& pt,
            const geom::Coordinate::ConstVect& snapPts);

    /** \brief
     * Snap segments of the source to nearby snap vertices.
     *
     * Source segments are "cracked" at a snap vertex.
     * A single input segment may be snapped several times
     * to different snap vertices.
     *
     * For each distinct snap vertex, at most one source segment
     * is snapped to.  This prevents "cracking" multiple segments
     * at the same point, which would likely cause
     * topology collapse when being used on polygonal linework.
     *
     * @param srcCoords the coordinates of the source linestring to be snapped
     *                  the object will be modified (coords snapped)
     * @param snapPts the target snap vertices                                       */
    void snapSegments(geom::CoordinateList& srcCoords,
                      const geom::Coordinate::ConstVect& snapPts);

    /// \brief
    /// Finds a src segment which snaps to (is close to) the given snap
    /// point.
    ///
    /// Only a single segment is selected for snapping.
    /// This prevents multiple segments snapping to the same snap vertex,
    /// which would almost certainly cause invalid geometry
    /// to be created.
    /// (The heuristic approach to snapping used here
    /// is really only appropriate when
    /// snap pts snap to a unique spot on the src geometry.)
    ///
    /// Also, if the snap vertex occurs as a vertex in the src
    /// coordinate list, no snapping is performed (may be changed
    /// using setAllowSnappingToSourceVertices).
    ///
    /// @param from
    ///        an iterator to first point of first segment to be checked
    ///
    /// @param too_far
    ///        an iterator to last point of last segment to be checked
    ///
    /// @returns an iterator to the snapped segment or
    ///          too_far if no segment needs snapping
    ///          (either none within snapTol distance,
    ///           or one found on the snapPt)
    ///
    geom::CoordinateList::iterator findSegmentToSnap(
        const geom::Coordinate& snapPt,
        geom::CoordinateList::iterator from,
        geom::CoordinateList::iterator too_far);

    geom::CoordinateList::iterator findVertexToSnap(
        const geom::Coordinate& snapPt,
        geom::CoordinateList::iterator from,
        geom::CoordinateList::iterator too_far);

    // Declare type as noncopyable
    LineStringSnapper(const LineStringSnapper& other) = delete;
    LineStringSnapper& operator=(const LineStringSnapper& rhs) = delete;
};


} // namespace geos::operation::overlay::snap
} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_OVERLAY_SNAP_LINESTRINGSNAPPER_H

