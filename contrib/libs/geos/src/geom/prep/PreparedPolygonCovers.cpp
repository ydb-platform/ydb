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
 * Last port: geom/prep/PreparedPolygonCovers.java rev 1.2 (JTS-1.10)
 * (2007-12-12)
 *
 **********************************************************************/


#include <geos/geom/prep/PreparedPolygonCovers.h>
#include <geos/geom/prep/PreparedPolygon.h>
#include <geos/geom/Geometry.h>

namespace geos {
namespace geom { // geos.geom
namespace prep { // geos.geom.prep
//
// private:
//

//
// protected:
//
bool
PreparedPolygonCovers::fullTopologicalPredicate(const geom::Geometry* geom)
{
    bool result = prepPoly->getGeometry().covers(geom);
    return result;
}

//
// public:
//

} // namespace geos.geom.prep
} // namespace geos.geom
} // namespace geos
