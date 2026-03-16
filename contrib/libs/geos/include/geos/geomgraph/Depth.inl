/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geomgraph/Depth.java rev. 1.4 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_GEOMGRAPH_DEPTH_INL
#define GEOS_GEOMGRAPH_DEPTH_INL

#include <geos/geomgraph/Depth.h>
#include <geos/geom/Position.h>
#include <geos/geom/Location.h>

using geos::geom::Position;

namespace geos {
namespace geomgraph {

INLINE
Depth::Depth()
{
    // initialize depth array to a sentinel value
    for(int i = 0; i < 2; i++) {
        for(int j = 0; j < 3; j++) {
            depth[i][j] = NULL_VALUE;
        }
    }
}

INLINE int
Depth::getDepth(int geomIndex, int posIndex) const
{
    return depth[geomIndex][posIndex];
}

INLINE void
Depth::setDepth(int geomIndex, int posIndex, int depthValue)
{
    depth[geomIndex][posIndex] = depthValue;
}

INLINE geom::Location
Depth::getLocation(int geomIndex, int posIndex) const
{
    if(depth[geomIndex][posIndex] <= 0) {
        return geom::Location::EXTERIOR;
    }
    return geom::Location::INTERIOR;
}

INLINE void
Depth::add(int geomIndex, int posIndex, geom::Location location)
{
    if(location == geom::Location::INTERIOR) {
        depth[geomIndex][posIndex]++;
    }
}

INLINE int
Depth::depthAtLocation(geom::Location location)
{
    if(location == geom::Location::EXTERIOR) {
        return 0;
    }
    if(location == geom::Location::INTERIOR) {
        return 1;
    }
    return NULL_VALUE;
}

/**
 * A Depth object is null (has never been initialized) if all depths are null.
 */
INLINE bool
Depth::isNull() const
{
    for(int i = 0; i < 2; i++) {
        for(int j = 0; j < 3; j++) {
            if(depth[i][j] != NULL_VALUE) {
                return false;
            }
        }
    }
    return true;
}

INLINE bool
Depth::isNull(int geomIndex) const
{
    return depth[geomIndex][1] == NULL_VALUE;
}

INLINE bool
Depth::isNull(int geomIndex, int posIndex) const
{
    return depth[geomIndex][posIndex] == NULL_VALUE;
}

INLINE int
Depth::getDelta(int geomIndex) const
{
    return depth[geomIndex][Position::RIGHT] - depth[geomIndex][Position::LEFT];
}

}
}

#endif
