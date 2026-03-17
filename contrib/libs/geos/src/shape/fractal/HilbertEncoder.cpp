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

#include <geos/shape/fractal/HilbertEncoder.h>
#include <geos/shape/fractal/HilbertCode.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Geometry.h>

#include <cstdint>
#include <vector>

namespace geos {
namespace shape {   // geos.shape
namespace fractal { // geos.shape.fractal


HilbertEncoder::HilbertEncoder(uint32_t p_level, geom::Envelope& extent)
    : level(p_level)
{
    int hside = (int)std::pow(2, level) - 1;

    minx = extent.getMinX();
    strideX = extent.getWidth() / hside;

    miny = extent.getMinY();
    strideY = extent.getHeight() / hside;
}

uint32_t
HilbertEncoder::encode(const geom::Envelope* env)
{
    double midx = env->getWidth()/2 + env->getMinX();
    uint32_t x = 0;
    if (midx > minx && strideX != 0)
        x = (uint32_t) ((midx - minx) / strideX);

    double midy = env->getHeight()/2 + env->getMinY();
    uint32_t y = 0;
    if (midy > miny && strideY != 0)
        y = (uint32_t) ((midy - miny) / strideY);

    return HilbertCode::encode(level, x, y);
}


/* public static */
void
HilbertEncoder::sort(std::vector<geom::Geometry*>& geoms)
{
    struct HilbertComparator {

        HilbertEncoder& enc;

        HilbertComparator(HilbertEncoder& e)
            : enc(e) {};

        bool
        operator()(const geom::Geometry* a, const geom::Geometry* b)
        {
            return enc.encode(a->getEnvelopeInternal()) > enc.encode(b->getEnvelopeInternal());
        }
    };

    geom::Envelope extent;
    for (const geom::Geometry* geom: geoms)
    {
        if (extent.isNull())
            extent = *(geom->getEnvelopeInternal());
        else
            extent.expandToInclude(*(geom->getEnvelopeInternal()));
    }
    if (extent.isNull()) return;

    HilbertEncoder encoder(12, extent);
    HilbertComparator hilbertCompare(encoder);
    std::sort(geoms.begin(), geoms.end(), hilbertCompare);
    return;
}


} // namespace geos.shape.fractal
} // namespace geos.shape
} // namespace geos
