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

#include <geos/algorithm/construct/LargestEmptyCircle.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/operation/distance/IndexedFacetDistance.h>

#include <typeinfo> // for dynamic_cast
#include <cassert>

using namespace geos::geom;


namespace geos {
namespace algorithm { // geos.algorithm
namespace construct { // geos.algorithm.construct



LargestEmptyCircle::LargestEmptyCircle(const Geometry* p_obstacles, double p_tolerance)
    : LargestEmptyCircle(p_obstacles, nullptr, p_tolerance)
{
}

LargestEmptyCircle::LargestEmptyCircle(const Geometry* p_obstacles, const Geometry* p_boundary, double p_tolerance)
    : tolerance(p_tolerance)
    , obstacles(p_obstacles)
    , factory(p_obstacles->getFactory())
    , obstacleDistance(p_obstacles)
    , done(false)
{
    if (!p_boundary)
    {
        boundary = p_obstacles->convexHull();
    }
    else
    {
        boundary = p_boundary->clone();
    }

    if (obstacles->isEmpty()) {
        throw util::IllegalArgumentException("Empty obstacles geometry is not supported");
    }
    if (boundary->isEmpty()) {
        throw util::IllegalArgumentException("Empty obstacles geometry is not supported");
    }
    if (!boundary->covers(obstacles)) {
        throw util::IllegalArgumentException("Boundary geometry does not cover obstacles");
    }

    // if boundary does not enclose an area cannot create a ptLocator
    if (boundary->getDimension() >= 2) {
        ptLocator.reset(new algorithm::locate::IndexedPointInAreaLocator(*(boundary.get())));
        boundaryDistance.reset(new operation::distance::IndexedFacetDistance(boundary.get()));
    }
}

/* public static */
std::unique_ptr<Point>
LargestEmptyCircle::getCenter(const Geometry* p_obstacles, double p_tolerance)
{
    LargestEmptyCircle lec(p_obstacles, p_tolerance);
    return lec.getCenter();
}

/* public static */
std::unique_ptr<LineString>
LargestEmptyCircle::getRadiusLine(const Geometry* p_obstacles, double p_tolerance)
{
    LargestEmptyCircle lec(p_obstacles, p_tolerance);
    return lec.getRadiusLine();
}

/* public */
std::unique_ptr<Point>
LargestEmptyCircle::getCenter()
{
    compute();
    return std::unique_ptr<Point>(factory->createPoint(centerPt));
}

/* public */
std::unique_ptr<Point>
LargestEmptyCircle::getRadiusPoint()
{
    compute();
    return std::unique_ptr<Point>(factory->createPoint(radiusPt));
}

/* public */
std::unique_ptr<LineString>
LargestEmptyCircle::getRadiusLine()
{
    compute();

    auto cl = factory->getCoordinateSequenceFactory()->create(2);
    cl->setAt(centerPt, 0);
    cl->setAt(radiusPt, 1);
    return factory->createLineString(std::move(cl));
}


/* private */
void
LargestEmptyCircle::createInitialGrid(const Envelope* env, std::priority_queue<Cell>& cellQueue)
{
    double minX = env->getMinX();
    double maxX = env->getMaxX();
    double minY = env->getMinY();
    double maxY = env->getMaxY();
    double width = env->getWidth();
    double height = env->getHeight();
    double cellSize = std::min(width, height);
    double hSize = cellSize / 2.0;

    // compute initial grid of cells to cover area
    for (double x = minX; x < maxX; x += cellSize) {
        for (double y = minY; y < maxY; y += cellSize) {
            cellQueue.emplace(x+hSize, y+hSize, hSize, distanceToConstraints(x+hSize, y+hSize));
        }
    }
}

/* private */
bool
LargestEmptyCircle::mayContainCircleCenter(const Cell& cell, const Cell& farthestCell)
{
    /**
     * Every point in the cell lies outside the boundary,
     * so they cannot be the center point
     */
    if (cell.isFullyOutside())
        return false;

    /**
     * The cell is outside, but overlaps the boundary
     * so it may contain a point which should be checked.
     * This is only the case if the potential overlap distance
     * is larger than the tolerance.
     */
    if (cell.isOutside()) {
        bool isOverlapSignificant = cell.getMaxDistance() > tolerance;
        return isOverlapSignificant;
    }

    /**
     * Cell is inside the boundary. It may contain the center
     * if the maximum possible distance is greater than the current distance
     * (up to tolerance).
     */
    double potentialIncrease = cell.getMaxDistance() - farthestCell.getDistance();
    return potentialIncrease > tolerance;
}


/* private */
double
LargestEmptyCircle::distanceToConstraints(const Coordinate& c)
{
    bool isOutside = ptLocator && (Location::EXTERIOR == ptLocator->locate(&c));
    std::unique_ptr<Point> pt(factory->createPoint(c));
    if (isOutside) {
        double boundaryDist = boundaryDistance->distance(pt.get());
        return -boundaryDist;

    }
    double dist = obstacleDistance.distance(pt.get());
    return dist;
}

/* private */
double
LargestEmptyCircle::distanceToConstraints(double x, double y)
{
    Coordinate coord(x, y);
    return distanceToConstraints(coord);
}

/* private */
LargestEmptyCircle::Cell
LargestEmptyCircle::createCentroidCell(const Geometry* geom)
{
    Coordinate c;
    geom->getCentroid(c);
    Cell cell(c.x, c.y, 0, distanceToConstraints(c));
    return cell;
}


/* private */
void
LargestEmptyCircle::compute()
{

    // check if already computed
    if (done) return;

    // if ptLocator is not present then result is degenerate (represented as zero-radius circle)
    if (!ptLocator) {
        const Coordinate* pt = obstacles->getCoordinate();
        centerPt = *pt;
        radiusPt = *pt;
        done = true;
        return;
    }

    // Priority queue of cells, ordered by decreasing distance from constraints
    std::priority_queue<Cell> cellQueue;
    createInitialGrid(obstacles->getEnvelopeInternal(), cellQueue);

    Cell farthestCell = createCentroidCell(obstacles);

    /**
     * Carry out the branch-and-bound search
     * of the cell space
     */
    while (!cellQueue.empty()) {

        // pick the most promising cell from the queue
        Cell cell = cellQueue.top();
        cellQueue.pop();

        // update the center cell if the candidate is further from the constraints
        if (cell.getDistance() > farthestCell.getDistance()) {
            farthestCell = cell;
        }

        /**
        * If this cell may contain a better approximation to the center
        * of the empty circle, then refine it (partition into subcells
        * which are added into the queue for further processing).
        * Otherwise the cell is pruned (not investigated further),
        * since no point in it can be further than the current farthest distance.
        */
        if (mayContainCircleCenter(cell, farthestCell)) {
            // split the cell into four sub-cells
            double h2 = cell.getHSize() / 2;
            cellQueue.emplace(cell.getX()-h2, cell.getY()-h2, h2, distanceToConstraints(cell.getX()-h2, cell.getY()-h2));
            cellQueue.emplace(cell.getX()+h2, cell.getY()-h2, h2, distanceToConstraints(cell.getX()+h2, cell.getY()-h2));
            cellQueue.emplace(cell.getX()-h2, cell.getY()+h2, h2, distanceToConstraints(cell.getX()-h2, cell.getY()+h2));
            cellQueue.emplace(cell.getX()+h2, cell.getY()+h2, h2, distanceToConstraints(cell.getX()+h2, cell.getY()+h2));
        }
    }

    // the farthest cell is the best approximation to the MIC center
    Cell centerCell = farthestCell;
    centerPt.x = centerCell.getX();
    centerPt.y = centerCell.getY();

    // compute radius point
    std::unique_ptr<Point> centerPoint(factory->createPoint(centerPt));
    std::vector<geom::Coordinate> nearestPts = obstacleDistance.nearestPoints(centerPoint.get());
    radiusPt = nearestPts[0];

    // flag computation
    done = true;
}



} // namespace geos.algorithm.construct
} // namespace geos.algorithm
} // namespace geos


