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
 * Last port: operation/overlay/validate/FuzzyPointLocator.java rev. 1.1 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_OVERLAY_FUZZYPOINTLOCATOR_H
#define GEOS_OP_OVERLAY_FUZZYPOINTLOCATOR_H

#include <geos/export.h>
#include <geos/algorithm/PointLocator.h> // for composition
#include <geos/geom/Geometry.h> // for unique_ptr visibility of dtor
#include <geos/geom/Location.h> // for Location::Value enum

#include <vector>
#include <memory> // for unique_ptr

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
 * Finds the most likely Location of a point relative to
 * the polygonal components of a geometry, using a tolerance value.
 *
 * If a point is not clearly in the Interior or Exterior,
 * it is considered to be on the Boundary.
 * In other words, if the point is within the tolerance of the Boundary,
 * it is considered to be on the Boundary; otherwise,
 * whether it is Interior or Exterior is determined directly.
 */
class GEOS_DLL FuzzyPointLocator {

public:

    FuzzyPointLocator(const geom::Geometry& geom, double nTolerance);

    geom::Location getLocation(const geom::Coordinate& pt);

private:

    const geom::Geometry& g;

    double tolerance;

    algorithm::PointLocator ptLocator;

    std::unique_ptr<geom::Geometry> linework;

    // this function has been obsoleted
    std::unique_ptr<geom::Geometry> getLineWork(const geom::Geometry& geom);

    /// Extracts linework for polygonal components.
    ///
    /// @param geom the geometry from which to extract
    /// @return a lineal geometry containing the extracted linework
    std::unique_ptr<geom::Geometry> extractLineWork(const geom::Geometry& geom);

    // Declare type as noncopyable
    FuzzyPointLocator(const FuzzyPointLocator& other) = delete;
    FuzzyPointLocator& operator=(const FuzzyPointLocator& rhs) = delete;
};

} // namespace geos::operation::overlay::validate
} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_OVERLAY_FUZZYPOINTLOCATOR_H
