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
 * Last port: operation/linemerge/LineMergeEdge.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/operation/linemerge/LineMergeEdge.h>

using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace linemerge { // geos.operation.linemerge

/**
 * Constructs a LineMergeEdge with vertices given by the specified LineString.
 */
LineMergeEdge::LineMergeEdge(const LineString* newLine):
    line(newLine)
{
    //line=newLine;
}

/**
 * Returns the LineString specifying the vertices of this edge.
 */
const LineString*
LineMergeEdge::getLine() const
{
    return line;
}

} // namespace geos.operation.linemerge
} // namespace geos.operation
} // namespace geos
