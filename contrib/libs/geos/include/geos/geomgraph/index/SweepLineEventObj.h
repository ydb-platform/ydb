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
 **********************************************************************/

#ifndef GEOS_GEOMGRAPH_INDEX_SWEEPLINEEVENTOBJ_H
#define GEOS_GEOMGRAPH_INDEX_SWEEPLINEEVENTOBJ_H

#include <geos/export.h>

namespace geos {
namespace geomgraph { // geos::geomgraph
namespace index { // geos::geomgraph::index

// This is here so that SweepLineEvent constructor
// can use it as argument type.
// Both  SweepLineSegment and MonotoneChain will
// inherit from it.
class GEOS_DLL SweepLineEventOBJ {
public:
    virtual
    ~SweepLineEventOBJ() {}
};


} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos

#endif

