/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: simplify/TaggedLineSegment.java rev. 1.1 (JTS-1.7.1)
 *
 **********************************************************************/

#include <geos/simplify/TaggedLineSegment.h>

#include <cassert>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

namespace geos {
namespace simplify { // geos::simplify

TaggedLineSegment::TaggedLineSegment(const geom::Coordinate& p_p0,
                                     const geom::Coordinate& p_p1,
                                     const geom::Geometry* nParent,
                                     size_t nIndex)
    :
    LineSegment(p_p0, p_p1),
    parent(nParent),
    index(nIndex)
{
}

TaggedLineSegment::TaggedLineSegment(const geom::Coordinate& p_p0,
                                     const geom::Coordinate& p_p1)
    :
    LineSegment(p_p0, p_p1),
    parent(nullptr),
    index(0)
{
}

TaggedLineSegment::TaggedLineSegment(const TaggedLineSegment& ls)
    :
    LineSegment(ls),
    parent(ls.parent),
    index(ls.index)
{
}

const geom::Geometry*
TaggedLineSegment::getParent() const
{
    return parent;
}

size_t
TaggedLineSegment::getIndex() const
{
    return index;
}

} // namespace geos::simplify
} // namespace geos
