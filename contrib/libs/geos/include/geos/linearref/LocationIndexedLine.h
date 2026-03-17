/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: linearref/LocationIndexedLine.java r466
 *
 **********************************************************************/

#ifndef GEOS_LINEARREF_LOCATIONINDEXEDLINE_H
#define GEOS_LINEARREF_LOCATIONINDEXEDLINE_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/linearref/LinearLocation.h>
#include <geos/linearref/LocationIndexOfPoint.h>
#include <geos/linearref/LocationIndexOfLine.h>
#include <geos/util/IllegalArgumentException.h>

namespace geos {
namespace linearref { // geos::linearref

/**
 * \brief
 * Supports linear referencing along a linear
 * [Geometry](@ref geom::Geometry) using
 * [LinearLocations](@ref LinearLocation) as the index.
 */
class GEOS_DLL LocationIndexedLine {
private:
    const geom::Geometry* linearGeom;

    void
    checkGeometryType()
    {
        if(!linearGeom->isLineal()) {
            throw util::IllegalArgumentException("Input geometry must be linear");
        }
    }

public:

    /**
     * \brief
     * Constructs an object which allows linear referencing along
     * a given linear [Geometry](@ref geom::Geometry).
     *
     * @param p_linearGeom the linear geometry to reference along
     */
    LocationIndexedLine(const geom::Geometry* p_linearGeom)
        : linearGeom(p_linearGeom)
    {
        checkGeometryType();
    }

    /**
     * \brief
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
    geom::Coordinate
    extractPoint(const LinearLocation& index) const
    {
        return index.getCoordinate(linearGeom);
    }


    /**
     * \brief
     * Computes the [Coordinate](@ref geom::Coordinate) for the point
     * on the line at the given index, offset by the given distance.
     *
     * If the index is out of range the first or last point on the
     * line will be returned.
     * The computed point is offset to the left of the line if the offset
     * distance is positive, to the right if negative.
     *
     * The Z-ordinate of the computed point will be interpolated from
     * the Z-ordinates of the line segment containing it, if they exist.
     *
     * @param index the index of the desired point
     * @param offsetDistance the distance the point is offset from the segment
     *                       (positive is to the left, negative is to the right)
     * @return the Coordinate at the given index
     */
    geom::Coordinate
    extractPoint(const LinearLocation& index,
                 double offsetDistance) const
    {
        geom::Coordinate ret;
        index.getSegment(linearGeom)->pointAlongOffset(
            index.getSegmentFraction(), offsetDistance, ret
        );
        return ret;
    }

    /**
     * \brief
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
    std::unique_ptr<geom::Geometry>
    extractLine(const LinearLocation& startIndex,
                const LinearLocation& endIndex) const
    {
        return ExtractLineByLocation::extract(linearGeom, startIndex, endIndex);
    }


    /**
     * \brief
     * Computes the index for a given point on the line.
     *
     * The supplied point does not <i>necessarily</i> have to lie precisely
     * on the line, but if it is far from the line the accuracy and
     * performance of this function is not guaranteed.
     * Use {@link #project} to compute a guaranteed result for points
     * which may be far from the line.
     *
     * @param pt a point on the line
     * @return the index of the point
     *
     * @see project
     */
    LinearLocation
    indexOf(const geom::Coordinate& pt) const
    {
        return LocationIndexOfPoint::indexOf(linearGeom, pt);
    }

    /**
     * \brief
     * Finds the index for a point on the line
     * which is greater than the given index.
     *
     * If no such index exists, returns <tt>minIndex</tt>.
     * This method can be used to determine all indexes for
     * a point which occurs more than once on a non-simple line.
     * It can also be used to disambiguate cases where the given point lies
     * slightly off the line and is equidistant from two different
     * points on the line.
     *
     * The supplied point does not <i>necessarily</i> have to lie precisely
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
    LinearLocation
    indexOfAfter(const geom::Coordinate& pt,
                 const LinearLocation& minIndex) const
    {
        return LocationIndexOfPoint::indexOfAfter(linearGeom, pt, &minIndex);
    }

    /**
     * \brief
     * Computes the indices for a subline of the line.
     *
     * (The subline must <b>conform</b> to the line; that is,
     * all vertices in the subline (except possibly the first and last)
     * must be vertices of the line and occur in the same order).
     *
     * @param subLine a subLine of the line
     * @return a pair of indices for the start and end of the subline.
     */
    LinearLocation*
    indicesOf(const geom::Geometry* subLine) const
    {
        return LocationIndexOfLine::indicesOf(linearGeom, subLine);
    }


    /**
     * \brief
     * Computes the index for the closest point on the line to the given point.
     *
     * If more than one point has the closest distance the first one along
     * the line is returned.
     * (The point does not necessarily have to lie precisely on the line.)
     *
     * @param pt a point on the line
     * @return the index of the point
     */
    LinearLocation
    project(const geom::Coordinate& pt) const
    {
        return LocationIndexOfPoint::indexOf(linearGeom, pt);
    }

    /**
     * \brief
     * Returns the index of the start of the line
     *
     * @return the start index
     */
    LinearLocation
    getStartIndex() const
    {
        return LinearLocation();
    }

    /**
     * \brief
     * Returns the index of the end of the line
     *
     * @return the end index
     */
    LinearLocation
    getEndIndex() const
    {
        return LinearLocation::getEndLocation(linearGeom);
    }

    /**
     * \brief
     * Tests whether an index is in the valid index range for the line.
     *
     * @param index the index to test
     * @return `true` if the index is in the valid range
     */
    bool
    isValidIndex(const LinearLocation& index) const
    {
        return index.isValid(linearGeom);
    }


    /**
     * \brief
     * Computes a valid index for this line
     * by clamping the given index to the valid range of index values
     *
     * @return a valid index value
     */
    LinearLocation
    clampIndex(const LinearLocation& index) const
    {
        LinearLocation loc = index;
        loc.clamp(linearGeom);
        return loc;
    }
};

} // geos::linearref
} // geos
#endif
