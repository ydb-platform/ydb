/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/validate/OverlayResultValidator.java rev. 1.4 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_OVERLAY_OVERLAYRESULTVALIDATOR_H
#define GEOS_OP_OVERLAY_OVERLAYRESULTVALIDATOR_H

#include <geos/export.h>
#include <geos/operation/overlay/OverlayOp.h> // for OpCode enum
#include <geos/operation/overlay/validate/FuzzyPointLocator.h> // composition
#include <geos/geom/Location.h> // for Location::Value type

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class Coordinate;
}
}

namespace geos {
namespace operation { // geos::operation
namespace overlay { // geos::operation::overlay
namespace validate { // geos::operation::overlay::validate

/** \brief
 * Validates that the result of an overlay operation is
 * geometrically correct within a determined tolerance.
 *
 * Uses fuzzy point location to find points which are
 * definitely in either the interior or exterior of the result
 * geometry, and compares these results with the expected ones.
 *
 * This algorithm is only useful where the inputs are polygonal.
 *
 * This is a heuristic test, and may return false positive results
 * (I.e. it may fail to detect an invalid result.)
 * It should never return a false negative result, however
 * (I.e. it should never report a valid result as invalid.)
 *
 * @see OverlayOp
 */
class GEOS_DLL OverlayResultValidator {

public:

    static bool isValid(
        const geom::Geometry& geom0,
        const geom::Geometry& geom1,
        OverlayOp::OpCode opCode,
        const geom::Geometry& result);

    OverlayResultValidator(
        const geom::Geometry& geom0,
        const geom::Geometry& geom1,
        const geom::Geometry& result);

    bool isValid(OverlayOp::OpCode opCode);

    geom::Coordinate&
    getInvalidLocation()
    {
        return invalidLocation;
    }

private:

    double boundaryDistanceTolerance;

    const geom::Geometry& g0;

    const geom::Geometry& g1;

    const geom::Geometry& gres;

    FuzzyPointLocator fpl0;

    FuzzyPointLocator fpl1;

    FuzzyPointLocator fplres;

    geom::Coordinate invalidLocation;

    std::vector<geom::Coordinate> testCoords;

    void addTestPts(const geom::Geometry& g);

    void addVertices(const geom::Geometry& g);

    bool testValid(OverlayOp::OpCode overlayOp);

    bool testValid(OverlayOp::OpCode overlayOp, const geom::Coordinate& pt);

    bool isValidResult(OverlayOp::OpCode overlayOp,
                       std::vector<geom::Location>& location);

    static double computeBoundaryDistanceTolerance(
        const geom::Geometry& g0, const geom::Geometry& g1);

    // Declare type as noncopyable
    OverlayResultValidator(const OverlayResultValidator& other) = delete;
    OverlayResultValidator& operator=(const OverlayResultValidator& rhs) = delete;
};

} // namespace geos::operation::overlay::validate
} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_OVERLAY_OVERLAYRESULTVALIDATOR_H
