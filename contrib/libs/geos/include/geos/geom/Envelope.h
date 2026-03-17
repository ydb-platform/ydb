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
 **********************************************************************
 *
 * Last port: geom/Envelope.java rev 1.46 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_ENVELOPE_H
#define GEOS_GEOM_ENVELOPE_H


#include <geos/export.h>
#include <geos/inline.h>
#include <geos/geom/Coordinate.h>

#include <string>
#include <vector>
#include <ostream> // for operator<<
#include <memory>

namespace geos {
namespace geom { // geos::geom

class Envelope;

/// Output operator
GEOS_DLL std::ostream& operator<< (std::ostream& os, const Envelope& o);

class Coordinate;

/**
 * \class Envelope geom.h geos.h
 *
 * \brief
 * An Envelope defines a rectangulare region of the 2D coordinate plane.
 *
 * It is often used to represent the bounding box of a Geometry,
 * e.g. the minimum and maximum x and y values of the Coordinates.
 *
 * Note that Envelopes support infinite or half-infinite regions, by using
 * the values of `Double_POSITIVE_INFINITY` and `Double_NEGATIVE_INFINITY`.
 *
 * When Envelope objects are created or initialized, the supplies extent
 * values are automatically sorted into the correct order.
 *
 */
class GEOS_DLL Envelope {

public:

    friend std::ostream& operator<< (std::ostream& os, const Envelope& o);

    typedef std::unique_ptr<Envelope> Ptr;

    /** \brief
     * Creates a null Envelope.
     */
    Envelope();

    /** \brief
     * Creates an Envelope for a region defined by maximum and minimum values.
     *
     * @param  x1  the first x-value
     * @param  x2  the second x-value
     * @param  y1  the first y-value
     * @param  y2  the second y-value
     */
    Envelope(double x1, double x2, double y1, double y2);

    /** \brief
     * Creates an Envelope for a region defined by two Coordinates.
     *
     * @param  p1  the first Coordinate
     * @param  p2  the second Coordinate
     */
    Envelope(const Coordinate& p1, const Coordinate& p2);

    /** \brief
     * Creates an Envelope for a region defined by a single Coordinate.
     *
     * @param  p  the Coordinate
     */
    Envelope(const Coordinate& p);

    /// Copy constructor
    Envelope(const Envelope& env);

    /// Assignment operator
    Envelope& operator=(const Envelope& e);

    /** \brief
     * Create an Envelope from an Envelope string representation produced
     * by Envelope::toString()
     */
    Envelope(const std::string& str);

    /** \brief
     * Test the point `q` to see whether it intersects the Envelope
     * defined by `p1-p2`.
     *
     * @param p1 one extremal point of the envelope
     * @param p2 another extremal point of the envelope
     * @param q the point to test for intersection
     * @return `true` if q intersects the envelope p1-p2
     */
    static bool intersects(const Coordinate& p1, const Coordinate& p2,
                           const Coordinate& q);

    /** \brief
     * Test the envelope defined by `p1-p2` for intersection
     * with the envelope defined by `q1-q2`.
     *
     * @param p1 one extremal point of the envelope P
     * @param p2 another extremal point of the envelope P
     * @param q1 one extremal point of the envelope Q
     * @param q2 another extremal point of the envelope Q
     *
     * @return `true` if Q intersects P
     */
    static bool intersects(const Coordinate& p1, const Coordinate& p2,
                           const Coordinate& q1, const Coordinate& q2);

    /** \brief
     * Check if the extent defined by two extremal points intersects
     * the extent of this Envelope.
     *
     * @param a a point
     * @param b another point
     * @return `true` if the extents intersect
     */
    bool intersects(const Coordinate& a, const Coordinate& b) const;

    /** \brief
     *  Initialize to a null Envelope.
     */
    void init(void);

    /** \brief
     * Initialize an Envelope for a region defined by maximum and minimum values.
     *
     * @param  x1  the first x-value
     * @param  x2  the second x-value
     * @param  y1  the first y-value
     * @param  y2  the second y-value
     */
    void init(double x1, double x2, double y1, double y2);

    /** \brief
     * Initialize an Envelope to a region defined by two Coordinates.
     *
     * @param  p1  the first Coordinate
     * @param  p2  the second Coordinate
     */
    void init(const Coordinate& p1, const Coordinate& p2);

    /** \brief
     * Initialize an Envelope to a region defined by a single Coordinate.
     *
     * @param  p  the Coordinate
     */
    void init(const Coordinate& p);

    // use assignment operator instead
    //void init(Envelope env);

    /** \brief
     * Makes this `Envelope` a "null" envelope, that is, the envelope
     * of the empty geometry.
     */
    void setToNull(void);

    /** \brief
     * Returns `true` if this Envelope is a "null" envelope.
     *
     * @return `true` if this Envelope is uninitialized or is the
     *                envelope of the empty geometry.
     */
    bool isNull(void) const;

    /** \brief
     * Returns the difference between the maximum and minimum x values.
     *
     * @return  `max x - min x`, or 0 if this is a null Envelope
     */
    double getWidth(void) const;

    /** \brief
     * Returns the difference between the maximum and minimum y values.
     *
     * @return `max y - min y`, or 0 if this is a null Envelope
     */
    double getHeight(void) const;

    /** \brief
     * Gets the area of this envelope.
     *
     * @return the area of the envelope
     * @return 0.0 if the envelope is null
     */
    double
    getArea() const
    {
        return getWidth() * getHeight();
    }

    /** \brief
     * Returns the Envelope maximum y-value. `min y > max y` indicates
     * that this is a null Envelope.
     */
    double getMaxY() const;

    /** \brief
     * Returns the Envelope maximum x-value. `min x > max x` indicates
     * that this is a null Envelope.
     */
    double getMaxX() const;

    /** \brief
     * Returns the Envelope minimum y-value. `min y > max y` indicates
     * that this is a null Envelope.
     */
    double getMinY() const;

    /** \brief
     * Returns the Envelope minimum x-value. `min x > max x` indicates
     * that this is a null Envelope.
     */
    double getMinX() const;

    /** \brief
     * Computes the coordinate of the centre of this envelope
     * (as long as it is non-null).
     *
     * @param centre The coordinate to write results into
     * @return `false` if the center could not be found (null envelope).
     */
    bool centre(Coordinate& centre) const;

    /** \brief
     * Computes the intersection of two [Envelopes](@ref Envelope).
     *
     * @param env the envelope to intersect with
     * @param result the envelope representing the intersection of
     *               the envelopes (this will be the null envelope
     *               if either argument is null, or they do not intersect)
     * @return false if not intersection is found
     */
    bool intersection(const Envelope& env, Envelope& result) const;

    /** \brief
     * Translates this envelope by given amounts in the X and Y direction.
     *
     * @param transX the amount to translate along the X axis
     * @param transY the amount to translate along the Y axis
     */
    void translate(double transX, double transY);

    /** \brief
     * Expands this envelope by a given distance in all directions.
     * Both positive and negative distances are supported.
     *
     * @param deltaX the distance to expand the envelope along the X axis
     * @param deltaY the distance to expand the envelope along the Y axis
     */
    void expandBy(double deltaX, double deltaY);

    /** \brief
     * Expands this envelope by a given distance in all directions.
     *
     * Both positive and negative distances are supported.
     *
     * @param p_distance the distance to expand the envelope
     */
    void
    expandBy(double p_distance)
    {
        expandBy(p_distance, p_distance);
    }


    /** \brief
     * Enlarges the boundary of the Envelope so that it contains p. Does
     * nothing if p is already on or within the boundaries.
     *
     * @param  p the Coordinate to include
     */
    void expandToInclude(const Coordinate& p);

    /** \brief
     * Enlarges the boundary of the Envelope so that it contains (x,y).
     *
     * Does nothing if (x,y) is already on or within the boundaries.
     *
     * @param  x  the value to lower the minimum x
     *            to or to raise the maximum x to
     * @param  y  the value to lower the minimum y
     *            to or to raise the maximum y to
     */
    void expandToInclude(double x, double y);

    /** \brief
     * Enlarges the boundary of the Envelope so that it contains `other`.
     *
     * Does nothing if other is wholly on or within the boundaries.
     *
     * @param other the Envelope to merge with
     */
    void expandToInclude(const Envelope* other);
    void expandToInclude(const Envelope& other);

    /** \brief
     * Tests if the Envelope `other` lies wholly inside this Envelope
     * (inclusive of the boundary).
     *
     * Note that this is **not** the same definition as the SFS `contains`,
     * which would exclude the envelope boundary.
     *
     * @param other the Envelope to check
     * @return `true` if `other` is contained in this Envelope
     *
     * @see covers(Envelope)
     */
    bool
    contains(const Envelope& other) const
    {
        return covers(other);
    }

    bool
    contains(const Envelope* other) const
    {
        return contains(*other);
    }

    /** \brief
     * Returns `true` if the given point lies in or on the envelope.
     *
     * @param p the point which this Envelope is being checked for containing
     * @return `true` if the point lies in the interior or on the boundary
     *         of this Envelope.
     */
    bool
    contains(const Coordinate& p) const
    {
        return covers(p.x, p.y);
    }

    /** \brief
     * Returns `true` if the given point lies in or on the envelope.
     *
     * @param x the x-coordinate of the point which this Envelope is
     *          being checked for containing
     * @param y the y-coordinate of the point which this Envelope is being
     *          checked for containing
     * @return `true` if `(x, y)` lies in the interior or on the boundary
     *         of this Envelope.
     */
    bool
    contains(double x, double y) const
    {
        return covers(x, y);
    }

    /** \brief
     * Check if the point p intersects (lies inside) the region of this Envelope.
     *
     * @param  p the Coordinate to be tested
     * @return true if the point intersects this Envelope
     */
    bool intersects(const Coordinate& p) const;

    /** \brief
     *  Check if the point (x, y) intersects (lies inside) the region of this Envelope.
     *
     * @param x the x-ordinate of the point
     * @param y the y-ordinate of the point
     * @return `true` if the point intersects this Envelope
     */
    bool intersects(double x, double y) const;

    /** \brief
     * Check if the region defined by other Envelope intersects the region of this Envelope.
     *
     * @param other the Envelope which this Envelope is being checked for intersection
     * @return true if the Envelopes intersects
     */
    bool intersects(const Envelope* other) const;

    bool intersects(const Envelope& other) const;

    /**
    * Tests if the region defined by other
    * is disjoint from the region of this Envelope
    *
    * @param other  the Envelope being checked for disjointness
    * @return  true if the Envelopes are disjoint
    */
    bool disjoint(const Envelope* other) const;

    bool disjoint(const Envelope& other) const;

    /** \brief
     * Tests if the given point lies in or on the envelope.
     *
     * @param x the x-coordinate of the point which this Envelope is being checked for containing
     * @param y the y-coordinate of the point which this Envelope is being checked for containing
     * @return `true` if `(x, y)` lies in the interior or on the boundary of this Envelope.
     */
    bool covers(double x, double y) const;

    /** \brief
     * Tests if the given point lies in or on the envelope.
     *
     * @param p the point which this Envelope is being checked for containing
     * @return `true` if the point lies in the interior or on the boundary of this Envelope.
     */
    bool covers(const Coordinate* p) const;

    /** \brief
     * Tests if the Envelope `other` lies wholly inside this Envelope (inclusive of the boundary).
     *
     * @param other the Envelope to check
     * @return true if this Envelope covers the `other`
     */
    bool covers(const Envelope& other) const;

    bool
    covers(const Envelope* other) const
    {
        return covers(*other);
    }


    /** \brief
     * Returns `true` if the Envelope `other` spatially equals this Envelope.
     *
     * @param  other the Envelope which this Envelope is being checked for equality
     * @return `true` if this and `other` Envelope objects are spatially equal
     */
    bool equals(const Envelope* other) const;

    /** \brief
     * Returns a `string` of the form `Env[minx:maxx,miny:maxy]`.
     *
     * @return a `string` of the form `Env[minx:maxx,miny:maxy]`
     */
    std::string toString(void) const;

    /** \brief
     * Computes the distance between this and another Envelope.
     *
     * The distance between overlapping Envelopes is 0. Otherwise, the
     * distance is the Euclidean distance between the closest points.
     */
    double distance(const Envelope& env) const;

    /** \brief
     * Computes the square of the distance between this and another Envelope.
     *
     * The distance between overlapping Envelopes is 0. Otherwise, the
     * distance is the Euclidean distance between the closest points.
     */
    double distanceSquared(const Envelope& env) const;

    /** \brief
     * Computes the distance between one Coordinate and an Envelope
     * defined by two other Coordinates. The order of the Coordinates
     * used to define the envelope is not significant.
     *
     * @param c the coordinate to from which distance should be found
     * @param p1 first coordinate defining an envelope
     * @param p2 second coordinate defining an envelope.
     */
    static double distanceToCoordinate(const Coordinate & c, const Coordinate & p1, const Coordinate & p2);

    /** \brief
     * Computes the squared distance between one Coordinate and an Envelope
     * defined by two other Coordinates. The order of the Coordinates
     * used to define the envelope is not significant.
     *
     * @param c the coordinate to from which distance should be found
     * @param p1 first coordinate defining an envelope
     * @param p2 second coordinate defining an envelope.
     */
    static double distanceSquaredToCoordinate(const Coordinate & c, const Coordinate & p1, const Coordinate & p2);

    size_t hashCode() const;

private:

    /** \brief
     * Splits a string into parts based on the supplied delimiters.
     *
     * This is a generic function that really belongs in a utility
     * file somewhere
     */
    std::vector<std::string> split(const std::string& str,
                                   const std::string& delimiters = " ");

    static double distance(double x0, double y0, double x1, double y1);

    /// the minimum x-coordinate
    double minx;

    /// the maximum x-coordinate
    double maxx;

    /// the minimum y-coordinate
    double miny;

    /// the maximum y-coordinate
    double maxy;
};

/// Checks if two Envelopes are equal (2D only check)
GEOS_DLL bool operator==(const Envelope& a, const Envelope& b);

} // namespace geos::geom
} // namespace geos

#ifdef GEOS_INLINE
# include "geos/geom/Envelope.inl"
#endif

#endif // ndef GEOS_GEOM_ENVELOPE_H
