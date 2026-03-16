/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/prep/PreparedPoint.java rev. 1.2 (JTS-1.10)
 *
 **********************************************************************/


#include <geos/geom/prep/PreparedPoint.h>

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep

bool
PreparedPoint::intersects(const geom::Geometry* g) const
{
    if(! envelopesIntersect(g)) {
        return false;
    }

    // This avoids computing topology for the test geometry
    return isAnyTargetComponentInTest(g);
}

} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos
