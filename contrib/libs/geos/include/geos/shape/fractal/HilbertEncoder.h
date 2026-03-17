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
#include <string>
#include <vector>
#include <cstdint>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class Geometry;
class Envelope;
}
}

namespace geos {
namespace shape {   // geos.shape
namespace fractal { // geos.shape.fractal


class GEOS_DLL HilbertEncoder {

public:

    HilbertEncoder(uint32_t p_level, geom::Envelope& extent);
    uint32_t encode(const geom::Envelope* env);
    static void sort(std::vector<geom::Geometry*>& geoms);

private:

    uint32_t level;
    double minx;
    double miny;
    double strideX;
    double strideY;

};


} // namespace geos.shape.fractal
} // namespace geos.shape
} // namespace geos



