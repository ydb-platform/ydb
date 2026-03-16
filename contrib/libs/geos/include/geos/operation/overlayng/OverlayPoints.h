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

#include <geos/export.h>

#include <geos/geom/Geometry.h>
#include <geos/geom/Point.h>

#include <map>
#include <vector>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class CoordinateSequence;
class GeometryFactory;
class Geometry;
class PrecisionModel;
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/**
 * Performs an overlay operation on inputs which are both point geometries.
 *
 * Semantics are:
 *
 *  - Points are rounded to the precision model if provided
 *  - Points with identical XY values are merged to a single point
 *  - Extended ordinate values are preserved in the output,
 *    apart from merging
 *  - An empty result is returned as <code>POINT EMPTY</code>
 *
 * @author Martin Davis
 */
class GEOS_DLL OverlayPoints {

private:

    // Members
    int opCode;
    const Geometry* geom0;
    const Geometry* geom1;
    const PrecisionModel* pm;
    const GeometryFactory* geometryFactory;
    std::vector<std::unique_ptr<Point>> resultList;

    // Methods
    void
    computeIntersection(std::map<Coordinate, std::unique_ptr<Point>>& map0,
                        std::map<Coordinate, std::unique_ptr<Point>>& map1,
                        std::vector<std::unique_ptr<Point>>& resultList);

    void
    computeDifference(std::map<Coordinate, std::unique_ptr<Point>>& map0,
                      std::map<Coordinate, std::unique_ptr<Point>>& map1,
                      std::vector<std::unique_ptr<Point>>& resultList);

    void
    computeUnion(std::map<Coordinate, std::unique_ptr<Point>>& map0,
                 std::map<Coordinate, std::unique_ptr<Point>>& map1,
                 std::vector<std::unique_ptr<Point>>& resultList);

    std::map<Coordinate, std::unique_ptr<Point>> buildPointMap(const Geometry* geom);

public:

    /**
    * Creates an instance of an overlay operation on inputs which are both point geometries.
    */
    OverlayPoints(int p_opCode, const Geometry* p_geom0, const Geometry* p_geom1, const PrecisionModel* p_pm)
        : opCode(p_opCode)
        , geom0(p_geom0)
        , geom1(p_geom1)
        , pm(p_pm)
        , geometryFactory(p_geom0->getFactory()) {}

    OverlayPoints(const OverlayPoints&) = delete;
    OverlayPoints& operator=(const OverlayPoints&) = delete;

    /**
    * Performs an overlay operation on inputs which are both point geometries.
    */
    static std::unique_ptr<Geometry> overlay(int opCode, const Geometry* geom0, const Geometry* geom1, const PrecisionModel* pm);

    /**
    * Gets the result of the overlay.
    *
    * @return the overlay result
    */
    std::unique_ptr<Geometry> getResult();


};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
