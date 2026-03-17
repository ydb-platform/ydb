/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/RobustLineIntersector.java r785 (JTS-1.13+)
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_LINEINTERSECTOR_H
#define GEOS_ALGORITHM_LINEINTERSECTOR_H

#include <geos/export.h>
#include <string>

#include <geos/geom/Coordinate.h>

// Forward declarations
namespace geos {
namespace geom {
class PrecisionModel;
}
}

namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * A LineIntersector is an algorithm that can both test whether
 * two line segments intersect and compute the intersection point
 * if they do.
 *
 * The intersection point may be computed in a precise or non-precise manner.
 * Computing it precisely involves rounding it to an integer.  (This assumes
 * that the input coordinates have been made precise by scaling them to
 * an integer grid.)
 *
 */
class GEOS_DLL LineIntersector {
public:

    /// \brief
    /// Return a Z value being the interpolation of Z from p0 and p1 at
    /// the given point p
    static double interpolateZ(const geom::Coordinate& p, const geom::Coordinate& p0, const geom::Coordinate& p1);


    /// Computes the "edge distance" of an intersection point p in an edge.
    ///
    /// The edge distance is a metric of the point along the edge.
    /// The metric used is a robust and easy to compute metric function.
    /// It is <b>not</b> equivalent to the usual Euclidean metric.
    /// It relies on the fact that either the x or the y ordinates of the
    /// points in the edge are unique, depending on whether the edge is longer in
    /// the horizontal or vertical direction.
    ///
    /// NOTE: This function may produce incorrect distances
    ///  for inputs where p is not precisely on p1-p2
    /// (E.g. p = (139,9) p1 = (139,10), p2 = (280,1) produces distanct
    /// 0.0, which is incorrect.
    ///
    /// My hypothesis is that the function is safe to use for points which are the
    /// result of <b>rounding</b> points which lie on the line,
    /// but not safe to use for <b>truncated</b> points.
    ///
    static double computeEdgeDistance(const geom::Coordinate& p, const geom::Coordinate& p0, const geom::Coordinate& p1);

    static double nonRobustComputeEdgeDistance(const geom::Coordinate& p, const geom::Coordinate& p1,
            const geom::Coordinate& p2);

    LineIntersector(const geom::PrecisionModel* initialPrecisionModel = nullptr)
        :
        precisionModel(initialPrecisionModel),
        result(0),
        intPt{},
        isProperVar(false)
    {}

    ~LineIntersector() {}

    /** \brief
     * Tests whether either intersection point is an interior point of
     * one of the input segments.
     *
     * @return <code>true</code> if either intersection point is in
     * the interior of one of the input segments
     */
    bool isInteriorIntersection();

    /** \brief
     * Tests whether either intersection point is an interior point
     * of the specified input segment.
     *
     * @return <code>true</code> if either intersection point is in
     * the interior of the input segment
     */
    bool isInteriorIntersection(size_t inputLineIndex);

    /// Force computed intersection to be rounded to a given precision model.
    ///
    /// No getter is provided, because the precision model is not required
    /// to be specified.
    /// @param newPM the PrecisionModel to use for rounding
    ///
    void
    setPrecisionModel(const geom::PrecisionModel* newPM)
    {
        precisionModel = newPM;
    }

    /// Compute the intersection of a point p and the line p1-p2.
    ///
    /// This function computes the boolean value of the hasIntersection test.
    /// The actual value of the intersection (if there is one)
    /// is equal to the value of <code>p</code>.
    ///
    void computeIntersection(const geom::Coordinate& p, const geom::Coordinate& p1, const geom::Coordinate& p2);

    /// Same as above but doesn't compute intersection point. Faster.
    static bool hasIntersection(const geom::Coordinate& p, const geom::Coordinate& p1, const geom::Coordinate& p2);

    enum intersection_type : uint8_t {
        /// Indicates that line segments do not intersect
        NO_INTERSECTION = 0,

        /// Indicates that line segments intersect in a single point
        POINT_INTERSECTION = 1,

        /// Indicates that line segments intersect in a line segment
        COLLINEAR_INTERSECTION = 2
    };

    /// Computes the intersection of the lines p1-p2 and p3-p4
    void computeIntersection(const geom::Coordinate& p1, const geom::Coordinate& p2,
                             const geom::Coordinate& p3, const geom::Coordinate& p4);

    std::string toString() const;

    /**
     * Tests whether the input geometries intersect.
     *
     * @return true if the input geometries intersect
     */
    bool
    hasIntersection() const
    {
        return result != NO_INTERSECTION;
    }

    /// Returns the number of intersection points found.
    ///
    /// This will be either 0, 1 or 2.
    ///
    size_t
    getIntersectionNum() const
    {
        return result;
    }


    /// Returns the intIndex'th intersection point
    ///
    /// @param intIndex is 0 or 1
    ///
    /// @return the intIndex'th intersection point
    ///
    const geom::Coordinate&
    getIntersection(size_t intIndex) const
    {
        return intPt[intIndex];
    }

    /// Returns false if both numbers are zero.
    ///
    /// @return true if both numbers are positive or if both numbers are negative.
    ///
    static bool isSameSignAndNonZero(double a, double b);

    /** \brief
     * Test whether a point is a intersection point of two line segments.
     *
     * Note that if the intersection is a line segment, this method only tests for
     * equality with the endpoints of the intersection segment.
     * It does <b>not</b> return true if
     * the input point is internal to the intersection segment.
     *
     * @return true if the input point is one of the intersection points.
     */
    bool isIntersection(const geom::Coordinate& pt) const;

    /** \brief
     * Tests whether an intersection is proper.
     *
     * The intersection between two line segments is considered proper if
     * they intersect in a single point in the interior of both segments
     * (e.g. the intersection is a single point and is not equal to any of the
     * endpoints).
     *
     * The intersection between a point and a line segment is considered proper
     * if the point lies in the interior of the segment (e.g. is not equal to
     * either of the endpoints).
     *
     * @return true if the intersection is proper
     */
    bool
    isProper() const
    {
        return hasIntersection() && isProperVar;
    }

    /** \brief
     * Computes the intIndex'th intersection point in the direction of
     * a specified input line segment
     *
     * @param segmentIndex is 0 or 1
     * @param intIndex is 0 or 1
     *
     * @return the intIndex'th intersection point in the direction of the
     *         specified input line segment
     */
    const geom::Coordinate& getIntersectionAlongSegment(size_t segmentIndex, size_t intIndex);

    /** \brief
     * Computes the index of the intIndex'th intersection point in the direction of
     * a specified input line segment
     *
     * @param segmentIndex is 0 or 1
     * @param intIndex is 0 or 1
     *
     * @return the index of the intersection point along the segment (0 or 1)
     */
    size_t getIndexAlongSegment(size_t segmentIndex, size_t intIndex);

    /** \brief
     * Computes the "edge distance" of an intersection point along the specified
     * input line segment.
     *
     * @param geomIndex is 0 or 1
     * @param intIndex is 0 or 1
     *
     * @return the edge distance of the intersection point
     */
    double getEdgeDistance(size_t geomIndex, size_t intIndex) const;

private:

    /**
     * If makePrecise is true, computed intersection coordinates
     * will be made precise using Coordinate#makePrecise
     */
    const geom::PrecisionModel* precisionModel;

    size_t result;

    const geom::Coordinate* inputLines[2][2];

    /**
     * We store real Coordinates here because
     * we must compute the Z of intersection point.
     */
    geom::Coordinate intPt[2];

    /**
     * The indexes of the endpoints of the intersection lines, in order along
     * the corresponding line
     */
    size_t intLineIndex[2][2];

    bool isProperVar;
    //Coordinate &pa;
    //Coordinate &pb;

    bool
    isCollinear() const
    {
        return result == COLLINEAR_INTERSECTION;
    }

    uint8_t computeIntersect(const geom::Coordinate& p1, const geom::Coordinate& p2,
                             const geom::Coordinate& q1, const geom::Coordinate& q2);

    bool
    isEndPoint() const
    {
        return hasIntersection() && !isProperVar;
    }

    void computeIntLineIndex();

    void computeIntLineIndex(size_t segmentIndex);

    uint8_t computeCollinearIntersection(const geom::Coordinate& p1, const geom::Coordinate& p2,
                                         const geom::Coordinate& q1, const geom::Coordinate& q2);

    /** \brief
     * This method computes the actual value of the intersection point.
     *
     * To obtain the maximum precision from the intersection calculation,
     * the coordinates are normalized by subtracting the minimum
     * ordinate values (in absolute value).  This has the effect of
     * removing common significant digits from the calculation to
     * maintain more bits of precision.
     */
    geom::Coordinate intersection(const geom::Coordinate& p1,
                                  const geom::Coordinate& p2,
                                  const geom::Coordinate& q1,
                                  const geom::Coordinate& q2) const;

    /**
     * Test whether a point lies in the envelopes of both input segments.
     * A correctly computed intersection point should return true
     * for this test.
     * Since this test is for debugging purposes only, no attempt is
     * made to optimize the envelope test.
     *
     * @return true if the input point lies within both
     *         input segment envelopes
     */
    bool isInSegmentEnvelopes(const geom::Coordinate& intPt) const;


    /**
     * Computes a segment intersection.
     * Round-off error can cause the raw computation to fail,
     * (usually due to the segments being approximately parallel).
     * If this happens, a reasonable approximation is computed instead.
     *
     * @param p1 a segment endpoint
     * @param p2 a segment endpoint
     * @param q1 a segment endpoint
     * @param q2 a segment endpoint
     * @return the computed intersection point is stored there
     */
    geom::Coordinate intersectionSafe(const geom::Coordinate& p1, const geom::Coordinate& p2,
                                      const geom::Coordinate& q1, const geom::Coordinate& q2) const;

    /**
     * Finds the endpoint of the segments P and Q which
     * is closest to the other segment.
     * This is a reasonable surrogate for the true
     * intersection points in ill-conditioned cases
     * (e.g. where two segments are nearly coincident,
     * or where the endpoint of one segment lies almost on the other segment).
     * <p>
     * This replaces the older CentralEndpoint heuristic,
     * which chose the wrong endpoint in some cases
     * where the segments had very distinct slopes
     * and one endpoint lay almost on the other segment.
     *
     * @param p1 an endpoint of segment P
     * @param p2 an endpoint of segment P
     * @param q1 an endpoint of segment Q
     * @param q2 an endpoint of segment Q
     * @return the nearest endpoint to the other segment
     */
    static geom::Coordinate nearestEndpoint(const geom::Coordinate& p1,
                                            const geom::Coordinate& p2,
                                            const geom::Coordinate& q1,
                                            const geom::Coordinate& q2);

    static double zGet(const geom::Coordinate& p, const geom::Coordinate& q);

    static double zGetOrInterpolate(const geom::Coordinate& p,
                                    const geom::Coordinate& p0,
                                    const geom::Coordinate& p1);

    static geom::Coordinate zGetOrInterpolateCopy(const geom::Coordinate& p,
                                                  const geom::Coordinate& p0,
                                                  const geom::Coordinate& p1);

    /// \brief
    /// Return a Z value being the interpolation of Z from p0 to p1 at
    /// the given point p
    static double zInterpolate(const geom::Coordinate& p,
                               const geom::Coordinate& p0,
                               const geom::Coordinate& p1);

    static double zInterpolate(const geom::Coordinate& p,
                               const geom::Coordinate& p1,
                               const geom::Coordinate& p2,
                               const geom::Coordinate& q1,
                               const geom::Coordinate& q2);

};


} // namespace geos::algorithm
} // namespace geos


#endif // GEOS_ALGORITHM_LINEINTERSECTOR_H

