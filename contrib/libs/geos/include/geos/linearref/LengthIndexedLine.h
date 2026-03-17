/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: linearref/LengthIndexedLine.java r463
 *
 **********************************************************************/

#ifndef GEOS_LINEARREF_LENGTHINDEXEDLINE_H
#define GEOS_LINEARREF_LENGTHINDEXEDLINE_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/linearref/LinearLocation.h>

namespace geos {
namespace linearref { // geos::linearref

/** \brief
 * Supports linear referencing along a linear geom::Geometry
 * using the length along the line as the index.
 *
 * Negative length values are taken as measured in the reverse direction
 * from the end of the geometry.
 * Out-of-range index values are handled by clamping
 * them to the valid range of values.
 * Non-simple lines (i.e. which loop back to cross or touch
 * themselves) are supported.
 */

class GEOS_DLL LengthIndexedLine {
private:
    const geom::Geometry* linearGeom;
    LinearLocation locationOf(double index) const;
    LinearLocation locationOf(double index, bool resolveLower) const;
    double positiveIndex(double index) const;

public:

    /** \brief
     * Constructs an object which allows a linear [Geometry](@ref geom::Geometry)
     * to be linearly referenced using length as an index.
     *
     * @param linearGeom the linear geometry to reference along
     */

    LengthIndexedLine(const geom::Geometry* linearGeom);

    /** \brief
     * Computes the [Coordinate](@ref geom::Coordinate) for the point
     * on the line at the given index.
     *
     * If the index is out of range the first or last point on the
     * line will be returned.
     * The Z-ordinate of the computed point will be interpolated from
     * the Z-ordinates of the line segment containing it, if they exist.
     *
     * @param index the index of the desired point
     * @return the Coordinate at the given index
     */
    geom::Coordinate extractPoint(double index) const;


    /**
     * \brief
     * Computes the [Coordinate](@ref geom::Coordinate) for the point
     * on the line at the given index, offset by the given distance.
     *
     * If the index is out of range the first or last point on the
     * line will be returned.
     * The computed point is offset to the left of the line if the
     * offset distance is positive, to the right if negative.
     *
     * The Z-ordinate of the computed point will be interpolated from
     * the Z-ordinates of the line segment containing it, if they exist.
     *
     * @param index the index of the desired point
     * @param offsetDistance the distance the point is offset from the segment
     *                       (positive is to the left, negative is to the right)
     * @return the Coordinate at the given index
     */
    geom::Coordinate extractPoint(double index, double offsetDistance) const;

    /** \brief
     * Computes the [LineString](@ref geom::LineString) for the interval
     * on the line between the given indices.
     *
     * If the endIndex lies before the startIndex,
     * the computed geometry is reversed.
     *
     * @param startIndex the index of the start of the interval
     * @param endIndex the index of the end of the interval
     * @return the linear interval between the indices
     */
    std::unique_ptr<geom::Geometry> extractLine(double startIndex, double endIndex) const;


    /** \brief
     * Computes the minimum index for a point on the line.
     *
     * If the line is not simple (i.e. loops back on itself)
     * a single point may have more than one possible index.
     * In this case, the smallest index is returned.
     *
     * The supplied point does not *necessarily* have to lie precisely
     * on the line, but if it is far from the line the accuracy and
     * performance of this function is not guaranteed.
     * Use {@link #project} to compute a guaranteed result for points
     * which may be far from the line.
     *
     * @param pt a point on the line
     * @return the minimum index of the point
     *
     * @see project
     */
    double indexOf(const geom::Coordinate& pt) const;

    /** \brief
     * Finds the index for a point on the line which is
     * greater than the given index.
     *
     * If no such index exists, returns `minIndex`.
     * This method can be used to determine all indexes for
     * a point which occurs more than once on a non-simple line.
     * It can also be used to disambiguate cases where the given point lies
     * slightly off the line and is equidistant from two different
     * points on the line.
     *
     * The supplied point does not `*necessarily* have to lie precisely
     * on the line, but if it is far from the line the accuracy and
     * performance of this function is not guaranteed.
     * Use {@link #project} to compute a guaranteed result for points
     * which may be far from the line.
     *
     * @param pt a point on the line
     * @param minIndex the value the returned index must be greater than
     * @return the index of the point greater than the given minimum index
     *
     * @see project
     */
    double indexOfAfter(const geom::Coordinate& pt, double minIndex) const;

    /** \brief
     * Computes the indices for a subline of the line.
     *
     * (The subline must **conform** to the line; that is,
     * all vertices in the subline (except possibly the first and last)
     * must be vertices of the line and occcur in the same order).
     *
     * @param subLine a subLine of the line
     * @return a pair of indices for the start and end of the subline.
     */
    double* indicesOf(const geom::Geometry* subLine) const;


    /** \brief
     * Computes the index for the closest point on the line to the given point.
     *
     * If more than one point has the closest distance the first one along the line
     * is returned.
     * (The point does not necessarily have to lie precisely on the line.)
     *
     * @param pt a point on the line
     * @return the index of the point
     */
    double project(const geom::Coordinate& pt) const;

    /** \brief
     * Returns the index of the start of the line
     * @return the start index
     */
    double getStartIndex() const;

    /** \brief
     * Returns the index of the end of the line
     * @return the end index
     */
    double getEndIndex() const;

    /** \brief
     * Tests whether an index is in the valid index range for the line.
     *
     * @param index the index to test
     * @return `true` if the index is in the valid range
     */
    bool isValidIndex(double index) const;


    /** \brief
     * Computes a valid index for this line by clamping the given index
     * to the valid range of index values.
     *
     * @return a valid index value
     */
    double clampIndex(double index) const;
};
}
}
#endif
