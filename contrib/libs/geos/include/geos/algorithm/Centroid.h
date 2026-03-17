/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2013 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/Centroid.java r728 (JTS-0.13+)
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_CENTROID_H
#define GEOS_ALGORITHM_CENTROID_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h> // for composition
#include <memory> // for std::unique_ptr

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class Polygon;
class CoordinateSequence;
}
}


namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Computes the centroid of a [Geometry](@ref geom::Geometry) of any dimension.
 *
 * If the geometry is nomimally of higher dimension, but contains only
 * components having a lower effective dimension (i.e. zero length or area),
 * the centroid will be computed appropriately.
 *
 * ### Algorithm #
 *
 * - **Dimension = 2** - Based on the usual algorithm for calculating
 *     the centroid as a weighted sum of the centroids
 *     of a decomposition of the area into (possibly overlapping) triangles.
 *     The algorithm has been extended to handle holes and multi-polygons.
 *     See http://www.faqs.org/faqs/graphics/algorithms-faq/
 *     for further details of the basic approach.
 * - **Dimension = 1** - Computes the average of the midpoints
 *     of all line segments weighted by the segment length.
 * - **Dimension = 0** - Compute the average coordinate over all points.
 *
 * If the input geometries are empty, a `null` Coordinate is returned.
 *
 */
class GEOS_DLL Centroid {

public:

    /** \brief
     * Computes the centroid point of a geometry.
     *
     * @param geom the geometry to use
     * @param cent will be set to the centroid point, if any
     *
     * @return `true` if a centroid could be computed,
     *         `false` otherwise (empty geom)
     */
    static bool getCentroid(const geom::Geometry& geom, geom::Coordinate& cent);

    /** \brief
     * Creates a new instance for computing the centroid of a geometry
     */
    Centroid(const geom::Geometry& geom)
        :
        areasum2(0.0),
        totalLength(0.0),
        ptCount(0)
    {
        add(geom);
    }

    /** \brief
     * Gets the computed centroid.
     *
     * @param cent will be set to the centroid point, if any
     *
     * @return `true` if a centroid could be computed,
     *         `false` otherwise (empty geom)
     */
    bool getCentroid(geom::Coordinate& cent) const;

private:

    std::unique_ptr<geom::Coordinate> areaBasePt;
    geom::Coordinate triangleCent3;
    geom::Coordinate cg3;
    geom::Coordinate lineCentSum;
    geom::Coordinate ptCentSum;
    double areasum2;
    double totalLength;
    int ptCount;

    /**
     * Adds a Geometry to the centroid total.
     *
     * @param geom the geometry to add
     */
    void add(const geom::Geometry& geom);

    void setAreaBasePoint(const geom::Coordinate& basePt);

    void add(const geom::Polygon& poly);

    void addShell(const geom::CoordinateSequence& pts);

    void addHole(const geom::CoordinateSequence& pts);

    void addTriangle(const geom::Coordinate& p0, const geom::Coordinate& p1, const geom::Coordinate& p2,
                     bool isPositiveArea);

    /**
     * Computes three times the centroid of the triangle p1-p2-p3.
     * The factor of 3 is
     * left in to permit division to be avoided until later.
     */
    static void centroid3(const geom::Coordinate& p1, const geom::Coordinate& p2, const geom::Coordinate& p3,
                          geom::Coordinate& c);

    /**
     * Returns twice the signed area of the triangle p1-p2-p3.
     * The area is positive if the triangle is oriented CCW, and negative if CW.
     */
    static double area2(const geom::Coordinate& p1, const geom::Coordinate& p2, const geom::Coordinate& p3);

    /**
     * Adds the line segments defined by an array of coordinates
     * to the linear centroid accumulators.
     *
     * @param pts an array of {@link Coordinate}s
     */
    void addLineSegments(const geom::CoordinateSequence& pts);

    /**
     * Adds a point to the point centroid accumulator.
     * @param pt a {@link Coordinate}
     */
    void addPoint(const geom::Coordinate& pt);
};

} // namespace geos::algorithm
} // namespace geos

#endif // GEOS_ALGORITHM_CENTROID_H
