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
 * Last port: algorithm/ConvexHull.java r407 (JTS-1.12+)
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_CONVEXHULL_H
#define GEOS_ALGORITHM_CONVEXHULL_H

#include <geos/export.h>
#include <memory>
#include <vector>

// FIXME: avoid using Cordinate:: typedefs to avoid full include
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class GeometryFactory;
}
}

namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Computes the convex hull of a Geometry.
 *
 * The convex hull is the smallest convex Geometry that contains all the
 * points in the input Geometry.
 *
 * Uses the Graham Scan algorithm.
 *
 * Last port: algorithm/ConvexHull.java rev. 1.26 (JTS-1.7)
 *
 */
class GEOS_DLL ConvexHull {
private:
    const geom::GeometryFactory* geomFactory;
    geom::Coordinate::ConstVect inputPts;

    void extractCoordinates(const geom::Geometry* geom);

    /// Create a CoordinateSequence from the Coordinate::ConstVect
    /// This is needed to construct the geometries.
    /// Here coordinate copies happen
    /// The returned object is newly allocated !NO EXCEPTION SAFE!
    std::unique_ptr<geom::CoordinateSequence> toCoordinateSequence(geom::Coordinate::ConstVect& cv);

    void computeOctPts(const geom::Coordinate::ConstVect& src,
                       geom::Coordinate::ConstVect& tgt);

    bool computeOctRing(const geom::Coordinate::ConstVect& src,
                        geom::Coordinate::ConstVect& tgt);

    /**
     * Uses a heuristic to reduce the number of points scanned
     * to compute the hull.
     * The heuristic is to find a polygon guaranteed to
     * be in (or on) the hull, and eliminate all points inside it.
     * A quadrilateral defined by the extremal points
     * in the four orthogonal directions
     * can be used, but even more inclusive is
     * to use an octilateral defined by the points in the
     * 8 cardinal directions.
     *
     * Note that even if the method used to determine the polygon
     * vertices is not 100% robust, this does not affect the
     * robustness of the convex hull.
     *
     * To satisfy the requirements of the Graham Scan algorithm,
     * the resulting array has at least 3 entries.
     *
     * @param pts The vector of const Coordinate pointers
     *            to be reduced (to at least 3 elements)
     *
     * WARNING: the parameter will be modified
     *
     */
    void reduce(geom::Coordinate::ConstVect& pts);

    /// parameter will be modified
    void padArray3(geom::Coordinate::ConstVect& pts);

    /// parameter will be modified
    void preSort(geom::Coordinate::ConstVect& pts);

    /**
     * Given two points p and q compare them with respect to their radial
     * ordering about point o.  First checks radial ordering.
     * If points are collinear, the comparison is based
     * on their distance to the origin.
     *
     * p < q iff
     *
     * - ang(o-p) < ang(o-q) (e.g. o-p-q is CCW)
     * - or ang(o-p) == ang(o-q) && dist(o,p) < dist(o,q)
     *
     * @param o the origin
     * @param p a point
     * @param q another point
     * @return -1, 0 or 1 depending on whether p is less than,
     * equal to or greater than q
     */
    int polarCompare(const geom::Coordinate& o,
                     const geom::Coordinate& p, const geom::Coordinate& q);

    void grahamScan(const geom::Coordinate::ConstVect& c,
                    geom::Coordinate::ConstVect& ps);

    /**
     * @param  vertices  the vertices of a linear ring,
     *                   which may or may not be
     *                   flattened (i.e. vertices collinear)
     *
     * @return           a 2-vertex LineString if the vertices are
     *                   collinear; otherwise, a Polygon with unnecessary
     *                   (collinear) vertices removed
     */
    std::unique_ptr<geom::Geometry> lineOrPolygon(const geom::Coordinate::ConstVect& vertices);

    /**
     * Write in 'cleaned' a version of 'input' with collinear
     * vertexes removed.
     */
    void cleanRing(const geom::Coordinate::ConstVect& input,
                   geom::Coordinate::ConstVect& cleaned);

    /**
     * @return  whether the three coordinates are collinear
     *          and c2 lies between c1 and c3 inclusive
     */
    bool isBetween(const geom::Coordinate& c1, const geom::Coordinate& c2, const geom::Coordinate& c3);

public:

    /**
     * Create a new convex hull construction for the input Geometry.
     */
    ConvexHull(const geom::Geometry* newGeometry);


    ~ConvexHull();

    /**
     * Returns a Geometry that represents the convex hull of
     * the input geometry.
     * The returned geometry contains the minimal number of points
     * needed to represent the convex hull.
     * In particular, no more than two consecutive points
     * will be collinear.
     *
     * @return if the convex hull contains 3 or more points,
     *         a Polygon; 2 points, a LineString;
     *         1 point, a Point; 0 points, an empty GeometryCollection.
     */
    std::unique_ptr<geom::Geometry> getConvexHull();
};

} // namespace geos::algorithm
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#ifdef GEOS_INLINE
# include "geos/algorithm/ConvexHull.inl"
#endif

#endif // GEOS_ALGORITHM_CONVEXHULL_H
