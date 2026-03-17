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

#ifdef _MSC_VER
#pragma warning(disable:4355)
#endif

#include <cassert>
#include <string>
#include <sstream>

#include <geos/edgegraph/HalfEdge.h>
#include <geos/edgegraph/MarkHalfEdge.h>
#include <geos/geom/Coordinate.h>

namespace geos {
namespace edgegraph { // geos.edgegraph

/*public static*/
bool
MarkHalfEdge::isMarked(HalfEdge* e)
{
    return static_cast<MarkHalfEdge*>(e)->isMarked();
}

/*public static*/
void
MarkHalfEdge::mark(HalfEdge* e)
{
    static_cast<MarkHalfEdge*>(e)->mark();
}

/*public static*/
void
MarkHalfEdge::setMark(HalfEdge* e, bool isMarked)
{
    static_cast<MarkHalfEdge*>(e)->setMark(isMarked);
}

/*public static*/
void
MarkHalfEdge::setMarkBoth(HalfEdge* e, bool isMarked)
{
    static_cast<MarkHalfEdge*>(e)->setMark(isMarked);
    static_cast<MarkHalfEdge*>(e->sym())->setMark(isMarked);
}

/*public static*/
void
MarkHalfEdge::markBoth(HalfEdge* e)
{
    static_cast<MarkHalfEdge*>(e)->mark();
    static_cast<MarkHalfEdge*>(e->sym())->mark();
}




} // namespace geos.edgegraph
} // namespace geos
