/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009  Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/snap/SnapOverlayOp.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/operation/overlay/snap/SnapOverlayOp.h>
#include <geos/operation/overlay/snap/GeometrySnapper.h>
#include <geos/precision/CommonBitsRemover.h>
#include <geos/geom/Geometry.h>

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

/* private */
void
SnapOverlayOp::computeSnapTolerance()
{
    snapTolerance = GeometrySnapper::computeOverlaySnapTolerance(geom0,
                    geom1);

    // cout << "Snap tol = " <<  snapTolerance << endl;
}

/* public */
unique_ptr<Geometry>
SnapOverlayOp::getResultGeometry(OverlayOp::OpCode opCode)
{
    geom::GeomPtrPair prepGeom;
    snap(prepGeom);
    GeomPtr result(OverlayOp::overlayOp(prepGeom.first.get(),
                                        prepGeom.second.get(), opCode));
    prepareResult(*result);
    return result;
}

/* private */
void
SnapOverlayOp::snap(geom::GeomPtrPair& snapGeom)
{
    geom::GeomPtrPair remGeom;
    removeCommonBits(geom0, geom1, remGeom);

    GeometrySnapper::snap(*remGeom.first, *remGeom.second,
                          snapTolerance, snapGeom);

    // MD - may want to do this at some point, but it adds cycles
//    checkValid(snapGeom[0]);
//    checkValid(snapGeom[1]);

    /*
    System.out.println("Snapped geoms: ");
    System.out.println(snapGeom[0]);
    System.out.println(snapGeom[1]);
    */

}

/* private */
void
SnapOverlayOp::removeCommonBits(const geom::Geometry& p_geom0,
                                const geom::Geometry& p_geom1,
                                geom::GeomPtrPair& remGeom)
{
    cbr.reset(new precision::CommonBitsRemover());
    cbr->add(&p_geom0);
    cbr->add(&p_geom1);

    remGeom.first = p_geom0.clone();
    cbr->removeCommonBits(remGeom.first.get());
    remGeom.second = p_geom1.clone();
    cbr->removeCommonBits(remGeom.second.get());
}

/*private*/
void
SnapOverlayOp::prepareResult(geom::Geometry& geom)
{
    cbr->addCommonBits(&geom);
}


} // namespace geos.operation.snap
} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

