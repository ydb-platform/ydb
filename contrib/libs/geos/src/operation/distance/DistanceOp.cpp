/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/distance/DistanceOp.java r335 (JTS-1.12-)
 *
 **********************************************************************/

#include <geos/constants.h>
#include <geos/operation/distance/DistanceOp.h>
#include <geos/operation/distance/GeometryLocation.h>
#include <geos/operation/distance/ConnectedElementLocationFilter.h>
#include <geos/algorithm/PointLocator.h>
#include <geos/algorithm/Distance.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/LineSegment.h>
#include <geos/geom/util/PolygonExtracter.h>
#include <geos/geom/util/LinearComponentExtracter.h>
#include <geos/geom/util/PointExtracter.h>
#include <geos/util/IllegalArgumentException.h>

#include <vector>
#include <iostream>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

using namespace std;
using namespace geos::geom;
//using namespace geos::algorithm;

namespace geos {
namespace operation { // geos.operation
namespace distance { // geos.operation.distance

using namespace geom;
//using namespace geom::util;

/*public static (deprecated)*/
double
DistanceOp::distance(const Geometry* g0, const Geometry* g1)
{
    DistanceOp distOp(g0, g1);
    return distOp.distance();
}

/*public static*/
double
DistanceOp::distance(const Geometry& g0, const Geometry& g1)
{
    DistanceOp distOp(g0, g1);
    return distOp.distance();
}

/*public static*/
std::unique_ptr<CoordinateSequence>
DistanceOp::nearestPoints(const Geometry* g0, const Geometry* g1)
{
    DistanceOp distOp(g0, g1);
    return distOp.nearestPoints();
}

DistanceOp::DistanceOp(const Geometry* g0, const Geometry* g1):
    geom{g0, g1},
    terminateDistance(0.0),
    minDistance(DoubleInfinity)
{}

DistanceOp::DistanceOp(const Geometry& g0, const Geometry& g1):
    geom{&g0, &g1},
    terminateDistance(0.0),
    minDistance(DoubleInfinity)
{}

DistanceOp::DistanceOp(const Geometry& g0, const Geometry& g1, double tdist)
    :
    geom{&g0, &g1},
    terminateDistance(tdist),
    minDistance(DoubleInfinity)
{}

/**
 * Report the distance between the closest points on the input geometries.
 *
 * @return the distance between the geometries
 */
double
DistanceOp::distance()
{
    using geos::util::IllegalArgumentException;

    if(geom[0] == nullptr || geom[1] == nullptr) {
        throw IllegalArgumentException("null geometries are not supported");
    }
    if(geom[0]->isEmpty() || geom[1]->isEmpty()) {
        return 0.0;
    }
    computeMinDistance();
    return minDistance;
}

/* public */
std::unique_ptr<CoordinateSequence>
DistanceOp::nearestPoints()
{
    // lazily creates minDistanceLocation
    computeMinDistance();

    auto& locs = minDistanceLocation;

    // Empty input geometries result in this behaviour
    if(locs[0] == nullptr || locs[1] == nullptr) {
        // either both or none are set..
        assert(locs[0] == nullptr && locs[1] == nullptr);

        return nullptr;
    }

    std::unique_ptr<std::vector<Coordinate>> nearestPts(new std::vector<Coordinate>(2));
    (*nearestPts)[0] = locs[0]->getCoordinate();
    (*nearestPts)[1] = locs[1]->getCoordinate();

    return std::unique_ptr<CoordinateSequence>(new CoordinateArraySequence(nearestPts.release()));
}

void
DistanceOp::updateMinDistance(array<unique_ptr<GeometryLocation>, 2> & locGeom, bool flip)
{
    // if not set then don't update
    if(locGeom[0] == nullptr) {
#if GEOS_DEBUG
        std::cerr << "updateMinDistance called with loc[0] == null and loc[1] == " << locGeom[1] << std::endl;
#endif
        assert(locGeom[1] == nullptr);
        return;
    }

    if(flip) {
        minDistanceLocation[0] = std::move(locGeom[1]);
        minDistanceLocation[1] = std::move(locGeom[0]);
    }
    else {
        minDistanceLocation[0] = std::move(locGeom[0]);
        minDistanceLocation[1] = std::move(locGeom[1]);
    }
}

/*private*/
void
DistanceOp::computeMinDistance()
{
    // only compute once!
    if(computed) {
        return;
    }

#if GEOS_DEBUG
    std::cerr << "---Start: " << geom[0]->toString() << " - " << geom[1]->toString() << std::endl;
#endif

    computeContainmentDistance();

    if(minDistance <= terminateDistance) {
        computed = true;
        return;
    }

    computeFacetDistance();
    computed = true;

#if GEOS_DEBUG
    std::cerr << "---End " << std::endl;
#endif
}

/*private*/
void
DistanceOp::computeContainmentDistance()
{
    using geom::util::PolygonExtracter;

    Polygon::ConstVect polys1;
    PolygonExtracter::getPolygons(*(geom[1]), polys1);


#if GEOS_DEBUG
    std::cerr << "PolygonExtracter found " << polys1.size() << " polygons in geometry 2" << std::endl;
#endif

    // NOTE:
    // Expected to fill minDistanceLocation items
    // if minDistance <= terminateDistance

    array<std::unique_ptr<GeometryLocation>, 2> locPtPoly;
    // test if either geometry has a vertex inside the other
    if(! polys1.empty()) {
        auto insideLocs0 = ConnectedElementLocationFilter::getLocations(geom[0]);
        computeInside(insideLocs0, polys1, locPtPoly);

        if(minDistance <= terminateDistance) {
            assert(locPtPoly[0]);
            assert(locPtPoly[1]);

            minDistanceLocation[0] = std::move(locPtPoly[0]);
            minDistanceLocation[1] = std::move(locPtPoly[1]);

            return;
        }
    }

    Polygon::ConstVect polys0;
    PolygonExtracter::getPolygons(*(geom[0]), polys0);

#if GEOS_DEBUG
    std::cerr << "PolygonExtracter found " << polys0.size() << " polygons in geometry 1" << std::endl;
#endif


    if(! polys0.empty()) {
        auto insideLocs1 = ConnectedElementLocationFilter::getLocations(geom[1]);
        computeInside(insideLocs1, polys0, locPtPoly);
        if(minDistance <= terminateDistance) {
            // flip locations, since we are testing geom 1 VS geom 0
            assert(locPtPoly[0]);
            assert(locPtPoly[1]);

            minDistanceLocation[0] = std::move(locPtPoly[1]);
            minDistanceLocation[1] = std::move(locPtPoly[0]);

            return;
        }
    }
}


/*private*/
void
DistanceOp::computeInside(vector<unique_ptr<GeometryLocation>> & locs,
                          const Polygon::ConstVect& polys,
                          array<unique_ptr<GeometryLocation>, 2> & locPtPoly)
{
    for(auto& loc : locs) {
        for(const auto& poly : polys) {
			const Coordinate& pt = loc->getCoordinate();

			if (Location::EXTERIOR != ptLocator.locate(pt, static_cast<const Geometry*>(poly))) {
				minDistance = 0.0;
				locPtPoly[0] = std::move(loc);
				locPtPoly[1].reset(new GeometryLocation(poly, pt));
				return;
			}
        }
    }
}

/*private*/
void
DistanceOp::computeFacetDistance()
{
    using geom::util::LinearComponentExtracter;
    using geom::util::PointExtracter;

    array<unique_ptr<GeometryLocation>, 2> locGeom;

    /*
     * Geometries are not wholly inside, so compute distance from lines
     * and points of one to lines and points of the other
     */
    LineString::ConstVect lines0;
    LineString::ConstVect lines1;
    LinearComponentExtracter::getLines(*(geom[0]), lines0);
    LinearComponentExtracter::getLines(*(geom[1]), lines1);

#if GEOS_DEBUG
    std::cerr << "LinearComponentExtracter found "
              << lines0.size() << " lines in geometry 1 and "
              << lines1.size() << " lines in geometry 2 "
              << std::endl;
#endif

    // exit whenever minDistance goes LE than terminateDistance
    computeMinDistanceLines(lines0, lines1, locGeom);
    updateMinDistance(locGeom, false);
    if(minDistance <= terminateDistance) {
#if GEOS_DEBUG
        std::cerr << "Early termination after line-line distance" << std::endl;
#endif
        return;
    }

    Point::ConstVect pts1;
    PointExtracter::getPoints(*(geom[1]), pts1);

#if GEOS_DEBUG
    std::cerr << "PointExtracter found " << pts1.size() << " points in geometry 2" << std::endl;
#endif

    locGeom[0] = nullptr;
    locGeom[1] = nullptr;
    computeMinDistanceLinesPoints(lines0, pts1, locGeom);
    updateMinDistance(locGeom, false);
    if(minDistance <= terminateDistance) {
#if GEOS_DEBUG
        std::cerr << "Early termination after lines0-points1 distance" << std::endl;
#endif
        return;
    }

    Point::ConstVect pts0;
    PointExtracter::getPoints(*(geom[0]), pts0);

#if GEOS_DEBUG
    std::cerr << "PointExtracter found " << pts0.size() << " points in geometry 1" << std::endl;
#endif

    locGeom[0] = nullptr;
    locGeom[1] = nullptr;
    computeMinDistanceLinesPoints(lines1, pts0, locGeom);
    updateMinDistance(locGeom, true);
    if(minDistance <= terminateDistance) {
#if GEOS_DEBUG
        std::cerr << "Early termination after lines1-points0 distance" << std::endl;
#endif
        return;
    }

    locGeom[0] = nullptr;
    locGeom[1] = nullptr;
    computeMinDistancePoints(pts0, pts1, locGeom);
    updateMinDistance(locGeom, false);

#if GEOS_DEBUG
    std::cerr << "termination after pts-pts distance" << std::endl;
#endif
}

/*private*/
void
DistanceOp::computeMinDistanceLines(
    const LineString::ConstVect& lines0,
    const LineString::ConstVect& lines1,
    std::array<std::unique_ptr<GeometryLocation>, 2> & locGeom)
{
    for(const LineString* line0 : lines0) {
        for(const LineString* line1 : lines1) {

            if (line0->isEmpty() || line1->isEmpty())
                continue;

            computeMinDistance(line0, line1, locGeom);
            if(minDistance <= terminateDistance) {
                return;
            }
        }
    }
}

/*private*/
void
DistanceOp::computeMinDistancePoints(
    const Point::ConstVect& points0,
    const Point::ConstVect& points1,
    array<unique_ptr<GeometryLocation>, 2> & locGeom)
{
    for(const Point* pt0 : points0) {
        for(const Point* pt1 : points1) {

            if (pt1->isEmpty() || pt0->isEmpty())
                continue;

            double dist = pt0->getCoordinate()->distance(*(pt1->getCoordinate()));

#if GEOS_DEBUG
            std::cerr << "Distance "
                      << pt0->toString() << " - "
                      << pt1->toString() << ": "
                      << dist << ", minDistance: " << minDistance
                      << std::endl;
#endif

            if(dist < minDistance) {
                minDistance = dist;
                // this is wrong - need to determine closest points on both segments!!!
                locGeom[0].reset(new GeometryLocation(pt0, 0, *(pt0->getCoordinate())));
                locGeom[1].reset(new GeometryLocation(pt1, 0, *(pt1->getCoordinate())));
            }

            if(minDistance <= terminateDistance) {
                return;
            }
        }
    }
}

/*private*/
void
DistanceOp::computeMinDistanceLinesPoints(
    const LineString::ConstVect& lines,
    const Point::ConstVect& points,
    std::array<std::unique_ptr<GeometryLocation>, 2> & locGeom)
{
    for(const LineString* line : lines) {
        for(const Point* pt : points) {

            if (line->isEmpty() || pt->isEmpty())
                continue;

            computeMinDistance(line, pt, locGeom);
            if(minDistance <= terminateDistance) {
                return;
            }
        }
    }
}

/*private*/
void
DistanceOp::computeMinDistance(
    const LineString* line0,
    const LineString* line1,
    std::array<std::unique_ptr<GeometryLocation>, 2> & locGeom)
{
    using geos::algorithm::Distance;

    const Envelope* lineEnv0 = line0->getEnvelopeInternal();
    const Envelope* lineEnv1 = line1->getEnvelopeInternal();
    if(lineEnv0->distance(*lineEnv1) > minDistance) {
        return;
    }

    const CoordinateSequence* coord0 = line0->getCoordinatesRO();
    const CoordinateSequence* coord1 = line1->getCoordinatesRO();
    size_t npts0 = coord0->getSize();
    size_t npts1 = coord1->getSize();

    // brute force approach!
    for(size_t i = 0; i < npts0 - 1; ++i) {
        const Coordinate& p00 = coord0->getAt(i);
        const Coordinate& p01 = coord0->getAt(i+1);

        Envelope segEnv0(p00, p01);

        if (segEnv0.distanceSquared(*lineEnv1) > minDistance*minDistance) {
            continue;
        }

        for(size_t j = 0; j < npts1 - 1; ++j) {
            const Coordinate& p10 = coord1->getAt(j);
            const Coordinate& p11 = coord1->getAt(j+1);

            Envelope segEnv1(p10, p11);

            if (segEnv0.distanceSquared(segEnv1) > minDistance*minDistance) {
                continue;
            }

            double dist = Distance::segmentToSegment(p00, p01, p10, p11);
            if(dist < minDistance) {
                minDistance = dist;

                // TODO avoid copy from constructing segs, maybe
                // by making a static closestPoints that takes four
                // coordinate references
                LineSegment seg0(p00, p01);
                LineSegment seg1(p10, p11);
                auto closestPt = seg0.closestPoints(seg1);

                locGeom[0].reset(new GeometryLocation(line0, i, closestPt[0]));
                locGeom[1].reset(new GeometryLocation(line1, j, closestPt[1]));
            }
            if(minDistance <= terminateDistance) {
                return;
            }
        }
    }
}

/*private*/
void
DistanceOp::computeMinDistance(const LineString* line,
                               const Point* pt,
                               std::array<std::unique_ptr<GeometryLocation>, 2> & locGeom)
{
    using geos::algorithm::Distance;

    const Envelope* env0 = line->getEnvelopeInternal();
    const Envelope* env1 = pt->getEnvelopeInternal();
    if(env0->distance(*env1) > minDistance) {
        return;
    }
    const CoordinateSequence* coord0 = line->getCoordinatesRO();
    const Coordinate* coord = pt->getCoordinate();

    // brute force approach!
    size_t npts0 = coord0->getSize();
    for(size_t i = 0; i < npts0 - 1; ++i) {
        double dist = Distance::pointToSegment(*coord, coord0->getAt(i), coord0->getAt(i + 1));
        if(dist < minDistance) {
            minDistance = dist;

            // TODO avoid copy from constructing segs, maybe
            // by making a static closestPoints that takes three
            // coordinate references
            LineSegment seg(coord0->getAt(i), coord0->getAt(i + 1));
            Coordinate segClosestPoint;
            seg.closestPoint(*coord, segClosestPoint);

            locGeom[0].reset(new GeometryLocation(line, i, segClosestPoint));
            locGeom[1].reset(new GeometryLocation(pt, 0, *coord));
        }
        if(minDistance <= terminateDistance) {
            return;
        }
    }
}

/* public static */
bool
DistanceOp::isWithinDistance(const geom::Geometry& g0,
                             const geom::Geometry& g1,
                             double distance)
{
    DistanceOp distOp(g0, g1, distance);
    return distOp.distance() <= distance;
}

} // namespace geos.operation.distance
} // namespace geos.operation
} // namespace geos
