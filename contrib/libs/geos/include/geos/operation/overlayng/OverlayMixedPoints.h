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

#pragma once


#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/LineString.h>

#include <geos/export.h>

#include <set>
#include <memory>
#include <vector>


// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class PrecisionModel;
class Geometry;
class Coordinate;
class CoordinateArraySequence;
}
namespace algorithm {
namespace locate {
class PointOnGeometryLocator;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;
using algorithm::locate::PointOnGeometryLocator;

/**
 * Computes an overlay where one input is Point(s) and one is not.
 * This class supports overlay being used as an efficient way
 * to find points within or outside a polygon.
 *
 * Input semantics are:
 *
 *  - Duplicates are removed from Point output
 *  - Non-point output is rounded and noded using the given precision model
 *
 * Output semantics are:
 *
 *   - An empty result is an empty atomic geometry
 *     with dimension determined by the inputs and the operation,
 *     as per overlay semantics
 *
 * For efficiency the following optimizations are used:
 *
 *  - Input points are not included in the noding of the non-point input geometry
 * (in particular, they do not participate in snap-rounding if that is used).
 *  - If the non-point input geometry is not included in the output
 * it is not rounded and noded.  This means that points
 * are compared to the non-rounded geometry.
 * This will be apparent in the result.
 *
 * @author Martin Davis
 */
class GEOS_DLL OverlayMixedPoints {

private:

    // Members
    int opCode;
    const PrecisionModel* pm;
    const Geometry* geomPoint;
    const Geometry* geomNonPointInput;
    const GeometryFactory* geometryFactory;
    bool isPointRHS;

    std::unique_ptr<Geometry> geomNonPoint;
    int geomNonPointDim;
    std::unique_ptr<PointOnGeometryLocator> locator;
    int resultDim;

    // Methods
    std::unique_ptr<PointOnGeometryLocator> createLocator(const Geometry* geomNonPoint);

    std::unique_ptr<Geometry> prepareNonPoint(const Geometry* geomInput);

    std::unique_ptr<Geometry> computeIntersection(const CoordinateArraySequence* coords) const;

    std::unique_ptr<Geometry> computeUnion(const CoordinateArraySequence* coords);

    std::unique_ptr<Geometry> computeDifference(const CoordinateArraySequence* coords);

    std::unique_ptr<Geometry> createPointResult(std::vector<std::unique_ptr<Point>>& points) const;

    std::vector<std::unique_ptr<Point>> findPoints(bool isCovered, const CoordinateArraySequence* coords) const;

    std::vector<std::unique_ptr<Point>> createPoints(std::set<Coordinate>& coords) const;

    bool hasLocation(bool isCovered, const Coordinate& coord) const;

    std::unique_ptr<Geometry> copyNonPoint() const;

    std::unique_ptr<CoordinateArraySequence> extractCoordinates(const Geometry* points, const PrecisionModel* pm) const;

    std::vector<std::unique_ptr<Polygon>> extractPolygons(const Geometry* geom) const;

    std::vector<std::unique_ptr<LineString>> extractLines(const Geometry* geom) const;



public:

    OverlayMixedPoints(int p_opCode, const Geometry* geom0, const Geometry* geom1, const PrecisionModel* p_pm);

    static std::unique_ptr<Geometry> overlay(int opCode, const Geometry* geom0, const Geometry* geom1, const PrecisionModel* pm);

    std::unique_ptr<Geometry> getResult();




};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

