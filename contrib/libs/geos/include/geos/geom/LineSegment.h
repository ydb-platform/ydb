/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: geom/LineSegment.java r18 (JTS-1.11)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_LINESEGMENT_H
#define GEOS_GEOM_LINESEGMENT_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h> // for composition

#include <geos/inline.h>

#include <array>
#include <iostream> // for ostream
#include <functional> // for std::hash
#include <memory> // for unique_ptr

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
class GeometryFactory;
class LineString;
}
}

namespace geos {
namespace geom { // geos::geom

/**
 * Represents a line segment defined by two Coordinate.
 * Provides methods to compute various geometric properties
 * and relationships of line segments.
 *
 * This class is designed to be easily mutable (to the extent of
 * having its contained points public).
 * This supports a common pattern of reusing a single LineSegment
 * object as a way of computing segment properties on the
 * segments defined by arrays or lists of {@link Coordinate}s.
 *
 * TODO: have this class keep pointers rather then real Coordinates ?
 */
class GEOS_DLL LineSegment {
public:

    friend std::ostream& operator<< (std::ostream& o, const LineSegment& l);

    /// Computes the midpoint of the segment
    static Coordinate midPoint(const Coordinate& pt0, const Coordinate& pt1);

    Coordinate p0; /// Segment start

    Coordinate p1; /// Segment end

    LineSegment();

    /// Constructs a LineSegment with the given start and end Coordinates.
    LineSegment(const Coordinate& c0, const Coordinate& c1);

    LineSegment(double x0, double y0, double x1, double y1);

    void setCoordinates(const Coordinate& c0, const Coordinate& c1);

    // obsoleted, use operator[] instead
    //const Coordinate& getCoordinate(std::size_t i) const;

    const Coordinate& operator[](std::size_t i) const;
    Coordinate& operator[](std::size_t i);

    void setCoordinates(const LineSegment& ls);

    /// Computes the length of the line segment.
    double getLength() const;

    /// Tests whether the segment is horizontal.
    ///
    /// @return <code>true</code> if the segment is horizontal
    ///
    bool isHorizontal() const;

    /// Tests whether the segment is vertical.
    ///
    /// @return <code>true</code> if the segment is vertical
    ///
    bool isVertical() const;

    /**
     * Determines the orientation of a LineSegment relative to this segment.
     * The concept of orientation is specified as follows:
     * Given two line segments A and L,
     * <ul>
     * <li>A is to the left of a segment L if A lies wholly in the
     * closed half-plane lying to the left of L
     * <li>A is to the right of a segment L if A lies wholly in the
     * closed half-plane lying to the right of L
     * <li>otherwise, A has indeterminate orientation relative to L.
     *     This happens if A is collinear with L or if A crosses
     *     the line determined by L.
     * </ul>
     *
     * @param seg the LineSegment to compare
     *
     * @return 1 if seg is to the left of this segment
     * @return -1 if seg is to the right of this segment
     * @return 0 if seg has indeterminate orientation relative
     *         to this segment
     */
    int orientationIndex(const LineSegment& seg) const;

    // TODO: deprecate this
    int orientationIndex(const LineSegment* seg) const;

    /** \brief
     * Determines the orientation index of a Coordinate
     * relative to this segment.
     *
     * The orientation index is as defined in
     * Orientation::index.
     *
     * @param p the Coordinate to compare
     *
     * @return 1 if <code>p</code> is to the left of this segment
     * @return -1 if <code>p</code> is to the right of this segment
     * @return 0 if <code>p</code> is collinear with this segment
     *
     * @see Orientation::index(Coordinate, Coordinate,
     *                                       Coordinate)
     */
    int orientationIndex(const Coordinate& p) const;

    /// Reverses the direction of the line segment.
    void reverse();

    /// Puts the line segment into a normalized form.
    //
    /// This is useful for using line segments in maps and indexes when
    /// topological equality rather than exact equality is desired.
    ///
    void normalize();

    /// @return the angle this segment makes with the x-axis (in radians)
    double angle() const;

    /// Computes the midpoint of the segment
    Coordinate midPoint() const;

    /// Computes the distance between this line segment and another one.
    double distance(const LineSegment& ls) const;

    /// Computes the distance between this line segment and a point.
    double distance(const Coordinate& p) const;

    /** \brief
     * Computes the perpendicular distance between the (infinite)
     * line defined by this line segment and a point.
     */
    double distancePerpendicular(const Coordinate& p) const;

    /** \brief
     * Computes the Coordinate that lies a given
     * fraction along the line defined by this segment.
     *
     * A fraction of <code>0.0</code> returns the start point of
     * the segment; a fraction of <code>1.0</code> returns the end
     * point of the segment.
     * If the fraction is < 0.0 or > 1.0 the point returned
     * will lie before the start or beyond the end of the segment.
     *
     * @param segmentLengthFraction the fraction of the segment length
     *        along the line
     * @param ret will be set to the point at that distance
     */
    void pointAlong(double segmentLengthFraction, Coordinate& ret) const;

    /** \brief
     * Computes the {@link Coordinate} that lies a given
     * fraction along the line defined by this segment and offset from
     * the segment by a given distance.
     *
     * A fraction of <code>0.0</code> offsets
     * from the start point of the segment;
     * a fraction of <code>1.0</code> offsets
     * from the end point of the segment.
     *
     * The computed point is offset to the left of the line
     * if the offset distance is positive, to the right if negative.
     *
     * @param segmentLengthFraction the fraction of the segment
     *                              length along the line
     *
     * @param offsetDistance the distance the point is offset
     *        from the segment
     *         (positive is to the left, negative is to the right)
     *
     * @param ret will be set to the point at that distance and offset
     *
     * @throws IllegalStateException if the segment has zero length
     */
    void pointAlongOffset(double segmentLengthFraction,
                          double offsetDistance,
                          Coordinate& ret) const;

    /** \brief
     * Compute the projection factor for the projection of the point p
     * onto this LineSegment.
     *
     * The projection factor is the constant r
     * by which the vector for this segment must be multiplied to
     * equal the vector for the projection of p on the line
     * defined by this segment.
     *
     * The projection factor returned will be in the range
     * (-inf, +inf)
     *
     * @param p the point to compute the factor for
     *
     * @return the projection factor for the point
     *
     */
    double projectionFactor(const Coordinate& p) const;

    /** \brief
     * Computes the fraction of distance (in <tt>[0.0, 1.0]</tt>)
     * that the projection of a point occurs along this line segment.
     *
     * If the point is beyond either ends of the line segment,
     * the closest fractional value (<tt>0.0</tt> or <tt>1.0</tt>)
     * is returned.
     *
     * Essentially, this is the {@link #projectionFactor} clamped to
     * the range <tt>[0.0, 1.0]</tt>.
     *
     * @param inputPt the point
     * @return the fraction along the line segment the projection
     *         of the point occurs
     */
    double segmentFraction(const Coordinate& inputPt) const;

    /** \brief
     * Compute the projection of a point onto the line determined
     * by this line segment.
     *
     * Note that the projected point
     * may lie outside the line segment.  If this is the case,
     * the projection factor will lie outside the range [0.0, 1.0].
     */
    void project(const Coordinate& p, Coordinate& ret) const;

    /** \brief
     * Project a line segment onto this line segment and return the resulting
     * line segment.
     *
     * The returned line segment will be a subset of
     * the target line line segment.  This subset may be null, if
     * the segments are oriented in such a way that there is no projection.
     *
     * Note that the returned line may have zero length (i.e. the same endpoints).
     * This can happen for instance if the lines are perpendicular to one another.
     *
     * @param seg the line segment to project
     * @param ret the projected line segment
     * @return true if there is an overlap, false otherwise
     */
    bool project(const LineSegment& seg, LineSegment& ret) const;

    /// Computes the closest point on this line segment to another point.
    //
    /// @param p the point to find the closest point to
    /// @param ret the Coordinate to which the closest point on the line segment
    ///            to the point p will be written
    ///
    void closestPoint(const Coordinate& p, Coordinate& ret) const;

    /** \brief
     * Compares this object with the specified object for order.
     *
     * Uses the standard lexicographic ordering for the points in the LineSegment.
     *
     * @param  other  the LineSegment with which this LineSegment
     *            is being compared
     * @return a negative integer, zero, or a positive integer as this
     *         LineSegment is less than, equal to, or greater than the
     *         specified LineSegment
     */
    int compareTo(const LineSegment& other) const;

    /** \brief
     *  Returns <code>true</code> if <code>other</code> is
     *  topologically equal to this LineSegment (e.g. irrespective
     *  of orientation).
     *
     * @param  other  a <code>LineSegment</code> with which to do the comparison.
     * @return true if other is a LineSegment
     *      with the same values for the x and y ordinates.
     */
    bool equalsTopo(const LineSegment& other) const;

    /**
     * Computes the closest points on two line segments.
     * @param line the line segment to find the closest points to
     * @return a pair of Coordinates which are the closest points on
     * the line segments.
     */
    std::array<Coordinate, 2> closestPoints(const LineSegment& line);

    std::array<Coordinate, 2> closestPoints(const LineSegment* line);

    /**
     * Computes an intersection point between two segments,
     * if there is one.
     * There may be 0, 1 or many intersection points between two segments.
     * If there are 0, null is returned. If there is 1 or more, a single
     * one is returned (chosen at the discretion of the algorithm).
     * If more information is required about the details of the
     * intersection, the LineIntersector class should be used.
     *
     * @param line
     * @return intersection if found, setNull() otherwise
     */
    Coordinate intersection(const LineSegment& line) const;

    /** \brief
     * Computes the intersection point of the lines defined
     * by two segments, if there is one.
     *
     * There may be 0, 1 or an infinite number of intersection points
     * between two lines.
     * If there is a unique intersection point, it is returned.
     * Otherwise, <tt>null</tt> is returned.
     * If more information is required about the details of the
     * intersection, the algorithms::LineIntersector class should
     * be used.
     *
     * @param line a line segment defining a straight line
     * @return intersection if found, setNull() otherwise
     *
     */
    Coordinate lineIntersection(const LineSegment& line) const;

    /**
     * Creates a LineString with the same coordinates as this segment
     *
     * @param gf the geometery factory to use
     * @return a LineString with the same geometry as this segment
     */
    std::unique_ptr<LineString> toGeometry(const GeometryFactory& gf) const;

    struct HashCode {
        size_t operator()(const LineSegment & s) const {
            size_t h = std::hash<double>{}(s.p0.x);
            h ^= (std::hash<double>{}(s.p0.y) << 1);
            h ^= (std::hash<double>{}(s.p1.x) << 1);
            return h ^ (std::hash<double>{}(s.p1.y) << 1);
        }
    };

private:
    void project(double factor, Coordinate& ret) const;

};

std::ostream& operator<< (std::ostream& o, const LineSegment& l);

/// Checks if two LineSegment are equal (2D only check)
bool operator==(const LineSegment& a, const LineSegment& b);


} // namespace geos::geom
} // namespace geos

#ifdef GEOS_INLINE
# include "geos/geom/LineSegment.inl"
#endif

#endif // ndef GEOS_GEOM_LINESEGMENT_H
