/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/MinimumBoundingCircle.java 2019-01-23
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_MINIMUMBOUNDINGCIRCLE_H
#define GEOS_ALGORITHM_MINIMUMBOUNDINGCIRCLE_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Point.h>
#include <geos/geom/Triangle.h>

#include <vector>

// Forward declarations
// namespace geos {
// 	namespace geom {
// 		class GeometryCollection;
// 	}
// }


namespace geos {
namespace algorithm { // geos::algorithm

class GEOS_DLL MinimumBoundingCircle {

private:

    // member variables
    const geom::Geometry* input;
    std::vector<geom::Coordinate> extremalPts;
    geom::Coordinate centre;
    double radius;

    void computeCentre();
    void compute();
    void computeCirclePoints();
    geom::Coordinate lowestPoint(std::vector<geom::Coordinate>& pts);
    geom::Coordinate pointWitMinAngleWithX(std::vector<geom::Coordinate>& pts, geom::Coordinate& P);
    geom::Coordinate pointWithMinAngleWithSegment(std::vector<geom::Coordinate>& pts,
            geom::Coordinate& P, geom::Coordinate& Q);
    std::vector<geom::Coordinate> farthestPoints(std::vector<geom::Coordinate>& pts);


public:

    MinimumBoundingCircle(const geom::Geometry* geom):
        input(nullptr),
        radius(0.0)
    {
        input = geom;
        centre.setNull();
    }

    ~MinimumBoundingCircle() {};

    /**
    * Gets a geometry which represents the Minimum Bounding Circle.
    * If the input is degenerate (empty or a single unique point),
    * this method will return an empty geometry or a single Point geometry.
    * Otherwise, a Polygon will be returned which approximates the
    * Minimum Bounding Circle.
    * (Note that because the computed polygon is only an approximation,
    * it may not precisely contain all the input points.)
    *
    * @return a Geometry representing the Minimum Bounding Circle.
    */
    std::unique_ptr<geom::Geometry> getCircle();

    /**
    * Gets a geometry representing a line between the two farthest points
    * in the input.
    * These points will be two of the extremal points of the Minimum Bounding Circle.
    * They also lie on the convex hull of the input.
    *
    * @return a LineString between the two farthest points of the input
    * @return a empty LineString if the input is empty
    * @return a Point if the input is a point
    */
    std::unique_ptr<geom::Geometry> getMaximumDiameter();

    /**
    * Gets a geometry representing the diameter of the computed Minimum Bounding
    * Circle.
    *
    * @return the diameter LineString of the Minimum Bounding Circle
    * @return a empty LineString if the input is empty
    * @return a Point if the input is a point
    */
    std::unique_ptr<geom::Geometry> getDiameter();

    /**
    * Gets the extremal points which define the computed Minimum Bounding Circle.
    * There may be zero, one, two or three of these points,
    * depending on the number of points in the input
    * and the geometry of those points.
    *
    * @return the points defining the Minimum Bounding Circle
    */
    std::vector<geom::Coordinate> getExtremalPoints();

    /**
    * Gets the centre point of the computed Minimum Bounding Circle.
    *
    * @return the centre point of the Minimum Bounding Circle
    * @return null if the input is empty
    */
    geom::Coordinate getCentre();

    /**
    * Gets the radius of the computed Minimum Bounding Circle.
    *
    * @return the radius of the Minimum Bounding Circle
    */
    double getRadius();

};

} // namespace geos::algorithm
} // namespace geos

#endif // GEOS_ALGORITHM_MINIMUMBOUNDINGCIRCLE_H

