/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/construct/LargestEmptyCircle.java
 * https://github.com/locationtech/jts/commit/98274a7ea9b40651e9de6323dc10fb2cac17a245
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_CONSTRUCT_LARGESTCIRCLE_H
#define GEOS_ALGORITHM_CONSTRUCT_LARGESTCIRCLE_H

#include <geos/geom/Coordinate.h>
#include <geos/geom/Point.h>
#include <geos/geom/Envelope.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/operation/distance/IndexedFacetDistance.h>

#include <memory>
#include <queue>



namespace geos {
namespace geom {
class Coordinate;
class Envelope;
class Geometry;
class GeometryFactory;
class LineString;
class Point;
}
namespace operation {
namespace distance {
class IndexedFacetDistance;
}
}
}


namespace geos {
namespace algorithm { // geos::algorithm
namespace construct { // geos::algorithm::construct

/**
 * Computes the Euclidean distance (L2 metric) from a Point to a Geometry.
 *
 * Also computes two points which are separated by the distance.
 */
class GEOS_DLL LargestEmptyCircle {

public:

    /**
    * Creates a new instance of a Largest Empty Circle construction.
    *
    * @param p_obstacles a geometry representing the obstacles (points and lines)
    * @param p_tolerance the distance tolerance for computing the circle center point
    */
    LargestEmptyCircle(const geom::Geometry* p_obstacles, double p_tolerance);
    LargestEmptyCircle(const geom::Geometry* p_obstacles, const geom::Geometry* p_boundary, double p_tolerance);
    ~LargestEmptyCircle() = default;

    /**
    * Computes the center point of the Largest Empty Circle
    * `within a set of obstacles, up to a given tolerance distance.
    *
    * @param p_obstacles a geometry representing the obstacles (points and lines)
    * @param p_tolerance the distance tolerance for computing the center point
    * @return the center point of the Largest Empty Circle
    */
    static std::unique_ptr<geom::Point> getCenter(const geom::Geometry* p_obstacles, double p_tolerance);

    /**
    * Computes a radius line of the Largest Empty Circle
    * within a set of obstacles, up to a given distance tolerance.
    *
    * @param p_obstacles a geometry representing the obstacles (points and lines)
    * @param p_tolerance the distance tolerance for computing the center point
    * @return a line from the center of the circle to a point on the edge
    */
    static std::unique_ptr<geom::LineString> getRadiusLine(const geom::Geometry* p_obstacles, double p_tolerance);

    std::unique_ptr<geom::Point> getCenter();
    std::unique_ptr<geom::Point> getRadiusPoint();
    std::unique_ptr<geom::LineString> getRadiusLine();


private:

    /* private members */
    double tolerance;
    const geom::Geometry* obstacles;
    const geom::GeometryFactory* factory;
    std::unique_ptr<geom::Geometry> boundary; // convexhull(obstacles)
    operation::distance::IndexedFacetDistance obstacleDistance;
    bool done;
    std::unique_ptr<algorithm::locate::IndexedPointInAreaLocator> ptLocator;
    std::unique_ptr<operation::distance::IndexedFacetDistance> boundaryDistance;
    geom::Coordinate centerPt;
    geom::Coordinate radiusPt;

    /* private methods */
    void setBoundary(const geom::Geometry* obstacles);

    /**
    * Computes the signed distance from a point to the constraints
    * (obstacles and boundary).
    * Points outside the boundary polygon are assigned a negative distance.
    * Their containing cells will be last in the priority queue
    * (but will still end up being tested since they may be refined).
    *
    * @param c the point to compute the distance for
    * @return the signed distance to the constraints (negative indicates outside the boundary)
    */
    double distanceToConstraints(const geom::Coordinate& c);
    double distanceToConstraints(double x, double y);
    void compute();

    /* private class */
    class Cell {
    private:
        static constexpr double SQRT2 = 1.4142135623730951;
        double x;
        double y;
        double hSize;
        double distance;
        double maxDist;

    public:
        Cell(double p_x, double p_y, double p_hSize, double p_distanceToConstraints)
            : x(p_x)
            , y(p_y)
            , hSize(p_hSize)
            , distance(p_distanceToConstraints)
            , maxDist(p_distanceToConstraints + (p_hSize*SQRT2))
        {};

        geom::Envelope getEnvelope() const
        {
            geom::Envelope env(x-hSize, x+hSize, y-hSize, y+hSize);
            return env;
        }

        bool isFullyOutside() const
        {
            return maxDist < 0.0;
        }
        bool isOutside() const
        {
            return distance < 0.0;
        }
        double getMaxDistance() const
        {
            return maxDist;
        }
        double getDistance() const
        {
            return distance;
        }
        double getHSize() const
        {
            return hSize;
        }
        double getX() const
        {
            return x;
        }
        double getY() const
        {
            return y;
        }
        bool operator< (const Cell& rhs) const
        {
            return maxDist < rhs.maxDist;
        }
        bool operator> (const Cell& rhs) const
        {
            return maxDist > rhs.maxDist;
        }
        bool operator==(const Cell& rhs) const
        {
            return maxDist == rhs.maxDist;
        }
    };

    bool mayContainCircleCenter(const Cell& cell, const Cell& farthestCell);
    void createInitialGrid(const geom::Envelope* env, std::priority_queue<Cell>& cellQueue);
    Cell createCentroidCell(const geom::Geometry* geom);

};


} // geos::algorithm::construct
} // geos::algorithm
} // geos

#endif // GEOS_ALGORITHM_CONSTRUCT_LARGESTCIRCLE_H
