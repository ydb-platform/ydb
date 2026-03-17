/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: index/quadtree/IntervalSize.java rev 1.7 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/index/quadtree/IntervalSize.h>

#include <algorithm>
#include <cmath>

using namespace std;

namespace geos {
namespace index { // geos.index
namespace quadtree { // geos.index.quadtree

/* public static */
bool
IntervalSize::isZeroWidth(double mn, double mx)
{
    double width = mx - mn;
    if(width == 0.0) {
        return true;
    }

    double maxAbs = max(fabs(mn), fabs(mx));
    double scaledInterval = width / maxAbs;
    int level;
    frexp(scaledInterval, &level);
    level -= 1;
    return level <= MIN_BINARY_EXPONENT;
}

} // namespace geos.index.quadtree
} // namespace geos.index
} // namespace geos
