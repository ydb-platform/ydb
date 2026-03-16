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

#include <geos/operation/overlayng/PrecisionReducer.h>

#include <geos/operation/overlayng/OverlayNG.h>
#include <geos/geom/Geometry.h>


namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

/*public static*/
std::unique_ptr<Geometry>
PrecisionReducer::reducePrecision(const Geometry* geom, const PrecisionModel* pm, bool replacePrecisionModel)
{
    if (replacePrecisionModel) {
        auto gf = GeometryFactory::create(pm, geom->getSRID());
        OverlayNG ov(geom, nullptr, gf.get(), OverlayNG::UNION);
        /**
         * Ensure reducing a area only produces polygonal result.
         * (I.e. collapse lines are not output)
         */
        if (geom->getDimension() == 2)
            ov.setAreaResultOnly(true);

        return ov.getResult();
    }
    else {
        OverlayNG ov(geom, nullptr, pm, OverlayNG::UNION);
        if (geom->getDimension() == 2)
            ov.setAreaResultOnly(true);

        return ov.getResult();
    }
}



} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
