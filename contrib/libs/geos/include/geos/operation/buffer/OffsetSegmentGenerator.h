/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011  Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/OffsetSegmentGenerator.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_BUFFER_OFFSETSEGMENTGENERATOR_H
#define GEOS_OP_BUFFER_OFFSETSEGMENTGENERATOR_H

#include <geos/export.h>

#include <vector>

#include <geos/algorithm/LineIntersector.h> // for composition
#include <geos/geom/Coordinate.h> // for composition
#include <geos/geom/LineSegment.h> // for composition
#include <geos/operation/buffer/BufferParameters.h> // for composition
#include <geos/operation/buffer/OffsetSegmentString.h> // for composition

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
class PrecisionModel;
}
}

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/**
 * Generates segments which form an offset curve.
 * Supports all end cap and join options
 * provided for buffering.
 * Implements various heuristics to
 * produce smoother, simpler curves which are
 * still within a reasonable tolerance of the
 * true curve.
 *
 * @author Martin Davis
 *
 */
class GEOS_DLL OffsetSegmentGenerator {

public:

    /*
     * @param nBufParams buffer parameters, this object will
     *                   keep a reference to the passed parameters
     *                   so caller must make sure the object is
     *                   kept alive for the whole lifetime of
     *                   the buffer builder.
     */
    OffsetSegmentGenerator(const geom::PrecisionModel* newPrecisionModel,
                           const BufferParameters& bufParams, double distance);

    /**
     * Tests whether the input has a narrow concave angle
     * (relative to the offset distance).
     * In this case the generated offset curve will contain self-intersections
     * and heuristic closing segments.
     * This is expected behaviour in the case of buffer curves.
     * For pure offset curves,
     * the output needs to be further treated
     * before it can be used.
     *
     * @return true if the input has a narrow concave angle
     */
    bool
    hasNarrowConcaveAngle() const
    {
        return _hasNarrowConcaveAngle;
    }

    void initSideSegments(const geom::Coordinate& nS1,
                          const geom::Coordinate& nS2, int nSide);

    /// Get coordinates by taking ownership of them
    ///
    /// After this call, the coordinates reference in
    /// this object are dropped. Calling twice will
    /// segfault...
    ///
    /// FIXME: refactor memory management of this
    ///
    void
    getCoordinates(std::vector<geom::CoordinateSequence*>& to)
    {
        to.push_back(segList.getCoordinates());
    }

    void
    closeRing()
    {
        segList.closeRing();
    }

    /// Adds a CW circle around a point
    void createCircle(const geom::Coordinate& p, double distance);

    /// Adds a CW square around a point
    void createSquare(const geom::Coordinate& p, double distance);

    /// Add first offset point
    void
    addFirstSegment()
    {
        segList.addPt(offset1.p0);
    }

    /// Add last offset point
    void
    addLastSegment()
    {
        segList.addPt(offset1.p1);
    }

    void addNextSegment(const geom::Coordinate& p, bool addStartPoint);

    /// \brief
    /// Add an end cap around point p1, terminating a line segment
    /// coming from p0
    void addLineEndCap(const geom::Coordinate& p0,
                       const geom::Coordinate& p1);

    void
    addSegments(const geom::CoordinateSequence& pts, bool isForward)
    {
        segList.addPts(pts, isForward);
    }

private:

    /**
     * Factor which controls how close offset segments can be to
     * skip adding a filler or mitre.
     */
    static const double OFFSET_SEGMENT_SEPARATION_FACTOR; // 1.0E-3;

    /**
     * Factor which controls how close curve vertices on inside turns
     * can be to be snapped
     */
    static const double INSIDE_TURN_VERTEX_SNAP_DISTANCE_FACTOR; // 1.0E-3;

    /**
     * Factor which controls how close curve vertices can be to be snapped
     */
    static const double CURVE_VERTEX_SNAP_DISTANCE_FACTOR; //  1.0E-6;

    /**
     * Factor which determines how short closing segs can be for round buffers
     */
    static const int MAX_CLOSING_SEG_LEN_FACTOR = 80;

    /** \brief
     * the max error of approximation (distance) between a quad segment and
     * the true fillet curve
     */
    double maxCurveSegmentError; // 0.0

    /** \brief
     * The angle quantum with which to approximate a fillet curve
     * (based on the input # of quadrant segments)
     */
    double filletAngleQuantum;

    /// The Closing Segment Factor controls how long "closing
    /// segments" are.  Closing segments are added at the middle of
    /// inside corners to ensure a smoother boundary for the buffer
    /// offset curve.  In some cases (particularly for round joins
    /// with default-or-better quantization) the closing segments
    /// can be made quite short.  This substantially improves
    /// performance (due to fewer intersections being created).
    ///
    /// A closingSegFactor of 0 results in lines to the corner vertex.
    /// A closingSegFactor of 1 results in lines halfway
    /// to the corner vertex.
    /// A closingSegFactor of 80 results in lines 1/81 of the way
    /// to the corner vertex (this option is reasonable for the very
    /// common default situation of round joins and quadrantSegs >= 8).
    ///
    /// The default is 1.
    ///
    int closingSegLengthFactor; // 1;

    /// Owned by this object, destroyed by dtor
    ///
    /// This actually gets created multiple times
    /// and each of the old versions is pushed
    /// to the ptLists std::vector to ensure all
    /// created CoordinateSequences are properly
    /// destroyed.
    ///
    OffsetSegmentString segList;

    double distance;

    const geom::PrecisionModel* precisionModel;

    const BufferParameters& bufParams;

    algorithm::LineIntersector li;

    geom::Coordinate s0, s1, s2;

    geom::LineSegment seg0;

    geom::LineSegment seg1;

    geom::LineSegment offset0;

    geom::LineSegment offset1;

    int side;

    bool _hasNarrowConcaveAngle; // =false

    void addCollinear(bool addStartPoint);

    /// The mitre will be beveled if it exceeds the mitre ratio limit.
    ///
    /// @param offset0 the first offset segment
    /// @param offset1 the second offset segment
    /// @param distance the offset distance
    ///
    void addMitreJoin(const geom::Coordinate& p,
                      const geom::LineSegment& offset0,
                      const geom::LineSegment& offset1,
                      double distance);

    /// Adds a limited mitre join connecting the two reflex offset segments.
    ///
    /// A limited mitre is a mitre which is beveled at the distance
    /// determined by the mitre ratio limit.
    ///
    /// @param offset0 the first offset segment
    /// @param offset1 the second offset segment
    /// @param distance the offset distance
    /// @param mitreLimit the mitre limit ratio
    ///
    void addLimitedMitreJoin(
        const geom::LineSegment& offset0,
        const geom::LineSegment& offset1,
        double distance, double mitreLimit);

    /// \brief
    /// Adds a bevel join connecting the two offset segments
    /// around a reflex corner.
    ///
    /// @param offset0 the first offset segment
    /// @param offset1 the second offset segment
    ///
    void addBevelJoin(const geom::LineSegment& offset0,
                      const geom::LineSegment& offset1);

    static const double PI; //  3.14159265358979

    // Not in JTS, used for single-sided buffers
    int endCapIndex;

    void init(double newDistance);

    /**
     * Use a value which results in a potential distance error which is
     * significantly less than the error due to
     * the quadrant segment discretization.
     * For QS = 8 a value of 100 is reasonable.
     * This should produce a maximum of 1% distance error.
     */
    static const double SIMPLIFY_FACTOR; // 100.0;

    /// Adds the offset points for an outside (convex) turn
    ///
    /// @param orientation
    /// @param addStartPoint
    ///
    void addOutsideTurn(int orientation, bool addStartPoint);

    /// Adds the offset points for an inside (concave) turn
    ///
    /// @param orientation
    /// @param addStartPoint
    ///
    void addInsideTurn(int orientation, bool addStartPoint);

    /** \brief
     * Compute an offset segment for an input segment on a given
     * side and at a given distance.
     *
     * The offset points are computed in full double precision,
     * for accuracy.
     *
     * @param seg the segment to offset
     * @param side the side of the segment the offset lies on
     * @param distance the offset distance
     * @param offset the points computed for the offset segment
     */
    void computeOffsetSegment(const geom::LineSegment& seg,
                              int side, double distance,
                              geom::LineSegment& offset);

    /**
     * Adds points for a circular fillet around a reflex corner.
     *
     * Adds the start and end points
     *
     * @param p base point of curve
     * @param p0 start point of fillet curve
     * @param p1 endpoint of fillet curve
     * @param direction the orientation of the fillet
     * @param radius the radius of the fillet
     */
    void addDirectedFillet(const geom::Coordinate& p, const geom::Coordinate& p0,
                   const geom::Coordinate& p1,
                   int direction, double radius);

    /**
     * Adds points for a circular fillet arc between two specified angles.
     *
     * The start and end point for the fillet are not added -
     * the caller must add them if required.
     *
     * @param direction is -1 for a CW angle, 1 for a CCW angle
     * @param radius the radius of the fillet
     */
    void addDirectedFillet(const geom::Coordinate& p, double startAngle,
                   double endAngle, int direction, double radius);
private:
    // An OffsetSegmentGenerator cannot be copied because of member "const BufferParameters& bufParams"
    // Not declaring these functions triggers MSVC warning C4512: "assignment operator could not be generated"
    OffsetSegmentGenerator(const OffsetSegmentGenerator&);
    void operator=(const OffsetSegmentGenerator&);

};

} // namespace geos::operation::buffer
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_BUFFER_OFFSETSEGMENTGENERATOR_H

