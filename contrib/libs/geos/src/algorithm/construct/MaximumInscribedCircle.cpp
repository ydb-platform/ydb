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
 * Last port: algorithm/construct/MaximumInscribedCircle.java
 * https://github.com/locationtech/jts/commit/98274a7ea9b40651e9de6323dc10fb2cac17a245
 *
 **********************************************************************/

#include <geos/algorithm/construct/MaximumInscribedCircle.h>
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


/* public */
MaximumInscribedCircle::MaximumInscribedCircle(const Geometry* polygonal, double p_tolerance)
    : inputGeom(polygonal)
    , inputGeomBoundary(polygonal->getBoundary())
    , tolerance(p_tolerance)
    , indexedDistance(inputGeomBoundary.get())
    , ptLocator(*polygonal)
    , factory(polygonal->getFactory())
    , done(false)
{
    if (!(typeid(*polygonal) == typeid(Polygon) ||
          typeid(*polygonal) == typeid(MultiPolygon))) {
        throw util::IllegalArgumentException("Input geometry must be a Polygon or MultiPolygon");
    }

    if (polygonal->isEmpty()) {
        throw util::IllegalArgumentException("Empty input geometry is not supported");
    }
}


/* public static */
std::unique_ptr<Point>
MaximumInscribedCircle::getCenter(const Geometry* polygonal, double tolerance)
{
    MaximumInscribedCircle mic(polygonal, tolerance);
    return mic.getCenter();
}

/* public static */
std::unique_ptr<LineString>
MaximumInscribedCircle::getRadiusLine(const Geometry* polygonal, double tolerance)
{
    MaximumInscribedCircle mic(polygonal, tolerance);
    return mic.getRadiusLine();
}

/* public */
std::unique_ptr<Point>
MaximumInscribedCircle::getCenter()
{
    compute();
    auto pt = factory->createPoint(centerPt);
    return std::unique_ptr<Point>(pt);
}

/* public */
std::unique_ptr<Point>
MaximumInscribedCircle::getRadiusPoint()
{
    compute();
    auto pt = factory->createPoint(radiusPt);
    return std::unique_ptr<Point>(pt);
}

/* public */
std::unique_ptr<LineString>
MaximumInscribedCircle::getRadiusLine()
{
    compute();

    auto cl = factory->getCoordinateSequenceFactory()->create(2);
    cl->setAt(centerPt, 0);
    cl->setAt(radiusPt, 1);
    return factory->createLineString(std::move(cl));
}

/* private */
void
MaximumInscribedCircle::createInitialGrid(const Envelope* env, std::priority_queue<Cell>& cellQueue)
{
    if (!std::isfinite(env->getArea())) {
        throw util::GEOSException("Non-finite envelope encountered.");
    }

    double minX = env->getMinX();
    double maxX = env->getMaxX();
    double minY = env->getMinY();
    double maxY = env->getMaxY();
    double width = env->getWidth();
    double height = env->getHeight();
    double cellSize = std::min(width, height);
    double hSize = cellSize / 2.0;

    // Collapsed geometries just end up using the centroid
    // as the answer and skip all the other machinery
    if (cellSize == 0) return;

    // compute initial grid of cells to cover area
    for (double x = minX; x < maxX; x += cellSize) {
        for (double y = minY; y < maxY; y += cellSize) {
            cellQueue.emplace(x+hSize, y+hSize, hSize, distanceToBoundary(x+hSize, y+hSize));
        }
    }
}

/* private */
double
MaximumInscribedCircle::distanceToBoundary(const Coordinate& c)
{
    std::unique_ptr<Point> pt(factory->createPoint(c));
    double dist = indexedDistance.distance(pt.get());
    // double dist = inputGeomBoundary->distance(pt.get());
    bool isOutside = (Location::EXTERIOR == ptLocator.locate(&c));
    if (isOutside) return -dist;
    return dist;
}

/* private */
double
MaximumInscribedCircle::distanceToBoundary(double x, double y)
{
    Coordinate coord(x, y);
    return distanceToBoundary(coord);
}

/* private */
MaximumInscribedCircle::Cell
MaximumInscribedCircle::createCentroidCell(const Geometry* geom)
{
    Coordinate c;
    geom->getCentroid(c);
    Cell cell(c.x, c.y, 0, distanceToBoundary(c));
    return cell;
}

/* private */
void
MaximumInscribedCircle::compute()
{

    // check if already computed
    if (done) return;

    // Priority queue of cells, ordered by maximum distance from boundary
    std::priority_queue<Cell> cellQueue;

    createInitialGrid(inputGeom->getEnvelopeInternal(), cellQueue);

    // use the area centroid as the initial candidate center point
    Cell farthestCell = createCentroidCell(inputGeom);

    /**
     * Carry out the branch-and-bound search
     * of the cell space
     */
    while (!cellQueue.empty()) {
        // pick the most promising cell from the queue
        Cell cell = cellQueue.top();
        cellQueue.pop();

        // std::cout << i << ": (" << cell.getX() << ", " << cell.getY() << ") " << cell.getHSize() << " dist = " << cell.getDistance() << std::endl;

        // update the center cell if the candidate is further from the boundary
        if (cell.getDistance() > farthestCell.getDistance()) {
            farthestCell = cell;
        }
        /**
        * Refine this cell if the potential distance improvement
        * is greater than the required tolerance.
        * Otherwise the cell is pruned (not investigated further),
        * since no point in it is further than
        * the current farthest distance.
        */
        double potentialIncrease = cell.getMaxDistance() - farthestCell.getDistance();
        if (potentialIncrease > tolerance) {
            // split the cell into four sub-cells
            double h2 = cell.getHSize() / 2;
            cellQueue.emplace(cell.getX()-h2, cell.getY()-h2, h2, distanceToBoundary(cell.getX()-h2, cell.getY()-h2));
            cellQueue.emplace(cell.getX()+h2, cell.getY()-h2, h2, distanceToBoundary(cell.getX()+h2, cell.getY()-h2));
            cellQueue.emplace(cell.getX()-h2, cell.getY()+h2, h2, distanceToBoundary(cell.getX()-h2, cell.getY()+h2));
            cellQueue.emplace(cell.getX()+h2, cell.getY()+h2, h2, distanceToBoundary(cell.getX()+h2, cell.getY()+h2));
        }
    }
    // std::cout << "number of iterations: " << i << std::endl;

    // the farthest cell is the best approximation to the MIC center
    Cell centerCell = farthestCell;
    centerPt.x = centerCell.getX();
    centerPt.y = centerCell.getY();

    // compute radius point
    std::unique_ptr<Point> centerPoint(factory->createPoint(centerPt));
    std::vector<geom::Coordinate> nearestPts = indexedDistance.nearestPoints(centerPoint.get());
    radiusPt = nearestPts[0];

    // flag computation
    done = true;
}




} // namespace geos.algorithm.construct
} // namespace geos.algorithm
} // namespace geos

