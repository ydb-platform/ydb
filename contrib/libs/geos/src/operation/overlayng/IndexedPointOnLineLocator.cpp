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

#include <geos/operation/overlayng/IndexedPointOnLineLocator.h>

#include <geos/algorithm/PointLocator.h>
#include <geos/algorithm/locate/PointOnGeometryLocator.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>



namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


/*public*/
geom::Location
IndexedPointOnLineLocator::locate(const geom::Coordinate* p) {
    // TODO: optimize this with a segment index
    algorithm::PointLocator locator;
    return locator.locate(*p, &inputGeom);
}



} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
