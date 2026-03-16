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
 **********************************************************************/

#include <geos/operation/overlayng/OverlayMixedPoints.h>

#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/algorithm/locate/PointOnGeometryLocator.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/operation/overlayng/IndexedPointOnLineLocator.h>
#include <geos/operation/overlayng/OverlayEdge.h>
#include <geos/operation/overlayng/OverlayGraph.h>
#include <geos/operation/overlayng/OverlayLabel.h>
#include <geos/operation/overlayng/OverlayNG.h>
#include <geos/operation/overlayng/OverlayUtil.h>
#include <geos/util/Assert.h>

namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/**
 * @brief Extracts and rounds coordinates from a geometry
 *
 */
class CoordinateExtractingFilter: public geom::CoordinateFilter {
public:
    CoordinateExtractingFilter(CoordinateArraySequence& p_pts, const PrecisionModel& p_pm)
        : pts(p_pts), pm(p_pm)
    {}

    /**
     * Destructor.
     * Virtual dctor promises appropriate behaviour when someone will
     * delete a derived-class object via a base-class pointer.
     * http://www.parashift.com/c++-faq-lite/virtual-functions.html#faq-20.7
     */
    ~CoordinateExtractingFilter() override {}

    /**
     * Performs a filtering operation with or on coord in "read-only" mode.
     * @param coord The "read-only" Coordinate to which
     * 				the filter is applied.
     */
    void
    filter_ro(const geom::Coordinate* coord) override
    {
        Coordinate p(*coord);
        pm.makePrecise(p);
        pts.add(p);
    }

private:
    CoordinateArraySequence& pts;
    const PrecisionModel& pm;
};

/*public*/
OverlayMixedPoints::OverlayMixedPoints(int p_opCode, const Geometry* geom0, const Geometry* geom1, const PrecisionModel* p_pm)
    : opCode(p_opCode)
    , pm(p_pm ? p_pm : geom0->getPrecisionModel())
    , geometryFactory(geom0->getFactory())
    , resultDim(OverlayUtil::resultDimension(opCode, geom0->getDimension(), geom1->getDimension()))
{
    // name the dimensional geometries
    if (geom0->getDimension() == 0) {
        geomPoint = geom0;
        geomNonPointInput = geom1;
        isPointRHS = false;
    }
    else {
        geomPoint = geom1;
        geomNonPointInput = geom0;
        isPointRHS = true;
    }
}

/*public static*/
std::unique_ptr<Geometry>
OverlayMixedPoints::overlay(int opCode, const Geometry* geom0, const Geometry* geom1, const PrecisionModel* pm)
{
    OverlayMixedPoints overlay(opCode, geom0, geom1, pm);
    return overlay.getResult();
}


/*public*/
std::unique_ptr<Geometry>
OverlayMixedPoints::getResult()
{
    // reduce precision of non-point input, if required
    geomNonPoint = prepareNonPoint(geomNonPointInput);
    geomNonPointDim = geomNonPoint->getDimension();
    locator = createLocator(geomNonPoint.get());

    std::unique_ptr<CoordinateArraySequence> coords = extractCoordinates(geomPoint, pm);

    switch (opCode) {
        case OverlayNG::INTERSECTION: {
            return computeIntersection(coords.get());
        }
        case OverlayNG::UNION:
        case OverlayNG::SYMDIFFERENCE: {
            // UNION and SYMDIFFERENCE have same output
            return computeUnion(coords.get());
        }
        case OverlayNG::DIFFERENCE: {
            return computeDifference(coords.get());
        }
    }
    util::Assert::shouldNeverReachHere("Unknown overlay op code");
    return nullptr;
}

/*private*/
std::unique_ptr<PointOnGeometryLocator>
OverlayMixedPoints::createLocator(const Geometry* p_geomNonPoint)
{
    if (geomNonPointDim == 2) {
        std::unique_ptr<PointOnGeometryLocator> ipial(new IndexedPointInAreaLocator(*p_geomNonPoint));
        return ipial;
    }
    else {
        std::unique_ptr<PointOnGeometryLocator> ipoll(new IndexedPointOnLineLocator(*p_geomNonPoint));
        return ipoll;
    }
    // never get here
    std::unique_ptr<PointOnGeometryLocator> n(nullptr);
    return n;
}


/*private*/
std::unique_ptr<Geometry>
OverlayMixedPoints::prepareNonPoint(const Geometry* geomInput)
{
    // if non-point not in output no need to node it
    if (resultDim == 0) {
        return geomInput->clone();
    }
    // Node and round the non-point geometry for output
    return OverlayNG::geomunion(geomInput, pm);
}

/*private*/
std::unique_ptr<Geometry>
OverlayMixedPoints::computeIntersection(const CoordinateArraySequence* coords) const
{
    std::vector<std::unique_ptr<Point>> points = findPoints(true, coords);
    return createPointResult(points);
}

/*private*/
std::unique_ptr<Geometry>
OverlayMixedPoints::computeUnion(const CoordinateArraySequence* coords)
{
    std::vector<std::unique_ptr<Point>> resultPointList = findPoints(false, coords);
    std::vector<std::unique_ptr<LineString>> resultLineList;
    if (geomNonPointDim == 1) {
        resultLineList = extractLines(geomNonPoint.get());
    }
    std::vector<std::unique_ptr<Polygon>> resultPolyList;
    if (geomNonPointDim == 2) {
        resultPolyList = extractPolygons(geomNonPoint.get());
    }

    return OverlayUtil::createResultGeometry(resultPolyList, resultLineList, resultPointList, geometryFactory);
}

/*private*/
std::unique_ptr<Geometry>
OverlayMixedPoints::computeDifference(const CoordinateArraySequence* coords)
{
    if (isPointRHS) {
        return geomNonPoint->clone();
    }
    std::vector<std::unique_ptr<Point>> points = findPoints(false, coords);
    return createPointResult(points);
}

/*private*/
std::unique_ptr<Geometry>
OverlayMixedPoints::copyNonPoint() const
{
    // We always want to return a copy, unlike in
    // Java land, because our geomNonPoint is already
    // a unique_ptr that we are managing locally.
    // If we are really really careful, maybe we
    // could release() the pointer, but will leave
    // that to future generations.
    return geomNonPoint->clone();
}

/*private*/
std::unique_ptr<Geometry>
OverlayMixedPoints::createPointResult(std::vector<std::unique_ptr<Point>>& points) const
{
    if (points.size() == 0) {
        return geometryFactory->createEmpty(0);
    }
    else if (points.size() == 1) {
        auto& pt = points[0];
        std::unique_ptr<Geometry> rsltPt(pt.release());
        return rsltPt;
    }
    return geometryFactory->createMultiPoint(std::move(points));
}

/*private*/
std::vector<std::unique_ptr<Point>>
OverlayMixedPoints::findPoints(bool isCovered, const CoordinateArraySequence* coords) const
{
    // use set to remove duplicates
    std::set<Coordinate> resultCoords;
    // keep only points contained
    for (std::size_t i = 0; i < coords->size(); i++) {
        const Coordinate& coord = coords->getAt(i);
        if (hasLocation(isCovered, coord)) {
            resultCoords.insert(coord);
        }
    }
    return createPoints(resultCoords);
}

/*private*/
std::vector<std::unique_ptr<Point>>
OverlayMixedPoints::createPoints(std::set<Coordinate>& coords) const
{
    std::vector<std::unique_ptr<Point>> points;
    for (const Coordinate& coord : coords) {
        std::unique_ptr<Point> point(geometryFactory->createPoint(coord));
        points.push_back(std::move(point));
    }
    return points;
}

/*private*/
bool
OverlayMixedPoints::hasLocation(bool isCovered, const Coordinate& coord) const
{
    bool isExterior = (Location::EXTERIOR == locator->locate(&coord));
    if (isCovered) {
        return !isExterior;
    }
    return isExterior;
}


/*private*/
std::unique_ptr<CoordinateArraySequence>
OverlayMixedPoints::extractCoordinates(const Geometry* points, const PrecisionModel* p_pm) const
{
    std::unique_ptr<CoordinateArraySequence> coords(new CoordinateArraySequence());

    CoordinateExtractingFilter filter(*coords, *p_pm);
    points->apply_ro(&filter);
    return coords;
}

/*private*/
std::vector<std::unique_ptr<Polygon>>
OverlayMixedPoints::extractPolygons(const Geometry* geom) const
{
    std::vector<std::unique_ptr<Polygon>> list;
    for (std::size_t i = 0; i < geom->getNumGeometries(); i++) {
        const Polygon* poly = static_cast<const Polygon*>(geom->getGeometryN(i));
        if(!poly->isEmpty()) {
            Polygon* p = static_cast<Polygon*>(poly->clone().release());
            list.emplace_back(p);
        }
    }
    return list;
}

/*private*/
std::vector<std::unique_ptr<LineString>>
OverlayMixedPoints::extractLines(const Geometry* geom) const
{
    std::vector<std::unique_ptr<LineString>> list;
    for (std::size_t i = 0; i < geom->getNumGeometries(); i++) {
        const LineString* line = static_cast<const LineString*>(geom->getGeometryN(i));
        if (! line->isEmpty()) {
            LineString* l = static_cast<LineString*>(line->clone().release());
            list.emplace_back(l);
        }
    }
    return list;
}

} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
