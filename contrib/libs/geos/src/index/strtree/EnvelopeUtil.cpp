/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/index/strtree/EnvelopeUtil.h>
#include <geos/geom/Envelope.h>
#include <algorithm>

namespace geos {
namespace index { // geos.index
namespace strtree { // geos.index.strtree

// EnvelopeUtil::EnvelopeUtil() : {}


/*static private*/
static double
distance(double x1, double y1, double x2, double y2)
{
    double dx = x2 - x1;
    double dy = y2 - y1;
    return std::sqrt(dx * dx + dy * dy);
}

/*static public*/
double
EnvelopeUtil::maximumDistance(const geom::Envelope* env1, const geom::Envelope* env2)
{
    double minx = std::min(env1->getMinX(), env2->getMinX());
    double miny = std::min(env1->getMinY(), env2->getMinY());
    double maxx = std::max(env1->getMaxX(), env2->getMaxX());
    double maxy = std::max(env1->getMaxY(), env2->getMaxY());
    return distance(minx, miny, maxx, maxy);
}


} // namespace geos.index.strtree
} // namespace geos.index
} // namespace geos

