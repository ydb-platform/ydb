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

#include <vector>
#include <memory>
#include <map>


// Forward declarations
namespace geos {
namespace geom {
class PrecisionModel;
class Geometry;
}
namespace operation {
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/**
* Reduces the precision of a geometry by rounding it to the
* supplied {@link geom::PrecisionModel}.
*
* The output is always a valid geometry.  This implies that input components
* may be merged if they are closer than the grid precision.
* if merging is not desired, then the individual geometry components
* should be processed separately.
*
* The output is fully noded.
* This provides an effective way to node / snap-round a collection of {@link geom::LineString}s.
*/
class GEOS_DLL PrecisionReducer {

private:

    // Members

    // Methods


public:

    PrecisionReducer() {};

    static std::unique_ptr<Geometry> reducePrecision(const Geometry* geom, const PrecisionModel* pm, bool replacePrecisionModel = false);



};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

