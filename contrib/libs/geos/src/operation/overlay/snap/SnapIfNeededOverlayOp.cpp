/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009 2011 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/snap/SnapIfNeededOverlayOp.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/operation/overlay/snap/SnapIfNeededOverlayOp.h>
#include <geos/operation/overlay/snap/SnapOverlayOp.h>
#include <geos/operation/overlay/OverlayOp.h>
#include <geos/geom/Geometry.h> // for use in unique_ptr
#include <geos/util.h>

#include <cassert>
#include <limits> // for numeric_limits
#include <memory> // for unique_ptr

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

using namespace std;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay
namespace snap { // geos.operation.overlay.snap

/* public */
unique_ptr<Geometry>
SnapIfNeededOverlayOp::getResultGeometry(OverlayOp::OpCode opCode)
{
    using geos::util::TopologyException;

    unique_ptr<Geometry> result;

    TopologyException origEx;

    // Try with original input
    try {
        result.reset(OverlayOp::overlayOp(&geom0, &geom1, opCode));
        return result;
    }
    catch(const TopologyException& ex) {
        origEx = ex; // save original exception
#if GEOS_DEBUG
        std::cerr << "Overlay op threw " << ex.what() << ". Will try snapping now" << std::endl;
#endif
    }

    // Try snapping
    try {
        result = SnapOverlayOp::overlayOp(geom0, geom1, opCode);
        return result;
    }
    catch(const TopologyException& ex) {
        ::geos::ignore_unused_variable_warning(ex);
#if GEOS_DEBUG
        std::cerr << "Overlay op on snapped geoms threw " << ex.what() << ". Will try snapping now" << std::endl;
#endif
        throw origEx;
    }
}


} // namespace geos.operation.snap
} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

