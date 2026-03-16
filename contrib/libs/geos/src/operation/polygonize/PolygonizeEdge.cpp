/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/polygonize/PolygonizeEdge.java rev. 1.3 (JTS-1.10)
 *
 **********************************************************************/


#include <geos/operation/polygonize/PolygonizeEdge.h>

using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace polygonize { // geos.operation.polygonize

PolygonizeEdge::PolygonizeEdge(const LineString* newLine)
{
    line = newLine;
}

const LineString*
PolygonizeEdge::getLine()
{
    return line;
}

} // namespace geos.operation.polygonize
} // namespace geos.operation
} // namespace geos
