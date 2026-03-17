/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://trac.osgeo.org/geos
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

#include <memory>


// Forward declarations
namespace geos {
namespace geom {
class Geometry;
}
}

namespace geos {
namespace operation { // geos::operation
namespace geounion {  // geos::operation::geounion

/**
 * A strategy class that adapts UnaryUnion to different
 * kinds of overlay algorithms.
 *
 * @author Martin Davis
 *
 */
class GEOS_DLL UnionStrategy {

public:

    virtual ~UnionStrategy() {};

    /**
    * Computes the union of two geometries.
    * This method may throw a {@link util::TopologyException}
    * if one is encountered
    */
    virtual std::unique_ptr<geom::Geometry> Union(const geom::Geometry*, const geom::Geometry*) = 0;

    /**
    * Indicates whether the union function operates using
    * a floating (full) precision model.
    * If this is the case, then the unary union code
    * can make use of the operation::union::OverlapUnion performance optimization,
    * and perhaps other optimizations as well.
    * Otherwise, the union result extent may not be the same as the extent of the inputs,
    * which prevents using some optimizations.
    */
    virtual bool isFloatingPrecision() const = 0;


};

} // namespace geos::operation::geounion
} // namespace geos::operation
} // namespace geos

