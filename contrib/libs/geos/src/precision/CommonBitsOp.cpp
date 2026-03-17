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

#include <geos/precision/CommonBitsOp.h>
#include <geos/precision/CommonBitsRemover.h>
#include <geos/geom/Geometry.h>

#include <vector>
#include <string>
#include <memory>
#include <cassert>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

using namespace std;
using namespace geos::geom;

namespace geos {
namespace precision { // geos.precision

/*public*/
CommonBitsOp::CommonBitsOp()
    :
    returnToOriginalPrecision(true)

{
#if GEOS_DEBUG
    std::cerr << "CommonBitsOp[" << this
              << "]::CommonBitsOp()" << std::endl;
#endif
}

/*public*/
CommonBitsOp::CommonBitsOp(bool nReturnToOriginalPrecision)
    :
    returnToOriginalPrecision(nReturnToOriginalPrecision)
{
#if GEOS_DEBUG
    std::cerr << "CommonBitsOp[" << this
              << "]::CommonBitsOp(bool "
              << nReturnToOriginalPrecision << ")"
              << std::endl;
#endif
}

/*public*/
std::unique_ptr<Geometry>
CommonBitsOp::intersection(
    const Geometry* geom0,
    const Geometry* geom1)
{
    unique_ptr<Geometry> rgeom0;
    unique_ptr<Geometry> rgeom1;
    removeCommonBits(geom0, geom1, rgeom0, rgeom1);
    return computeResultPrecision(rgeom0->intersection(rgeom1.get()));
}

/*public*/
std::unique_ptr<Geometry>
CommonBitsOp::Union(
    const Geometry* geom0,
    const Geometry* geom1)
{
    unique_ptr<Geometry> rgeom0;
    unique_ptr<Geometry> rgeom1;
    removeCommonBits(geom0, geom1, rgeom0, rgeom1);
    return computeResultPrecision(rgeom0->Union(rgeom1.get()));
}

/*public*/
std::unique_ptr<Geometry>
CommonBitsOp::difference(
    const Geometry* geom0,
    const Geometry* geom1)
{
    unique_ptr<Geometry> rgeom0;
    unique_ptr<Geometry> rgeom1;
    removeCommonBits(geom0, geom1, rgeom0, rgeom1);
    return computeResultPrecision(rgeom0->difference(rgeom1.get()));
}

/*public*/
std::unique_ptr<Geometry>
CommonBitsOp::symDifference(
    const Geometry* geom0,
    const Geometry* geom1)
{
    unique_ptr<Geometry> rgeom0;
    unique_ptr<Geometry> rgeom1;
    removeCommonBits(geom0, geom1, rgeom0, rgeom1);
    return computeResultPrecision(rgeom0->symDifference(rgeom1.get()));
}

/*public*/
std::unique_ptr<Geometry>
CommonBitsOp::buffer(const Geometry* geom0, double distance)
{
    unique_ptr<Geometry> rgeom0(removeCommonBits(geom0));
    return computeResultPrecision(rgeom0->buffer(distance));
}

/*public*/
std::unique_ptr<Geometry>
CommonBitsOp::computeResultPrecision(std::unique_ptr<Geometry> result)
{
    assert(cbr.get());
    if(returnToOriginalPrecision) {
        cbr->addCommonBits(result.get());
    }
    return result;
}

/*private*/
std::unique_ptr<Geometry>
CommonBitsOp::removeCommonBits(const Geometry* geom0)
{
    cbr.reset(new CommonBitsRemover());
    cbr->add(geom0);

#if GEOS_DEBUG
    const Coordinate& commonCoord = cbr->getCommonCoordinate();
    cerr << "CommonBitsRemover bits: " << commonCoord.x << ", " << commonCoord.y << endl;
#endif

    auto geom = geom0->clone();
    cbr->removeCommonBits(geom.get());

    return geom;
}

/*private*/
void
CommonBitsOp::removeCommonBits(
    const geom::Geometry* geom0,
    const geom::Geometry* geom1,
    std::unique_ptr<geom::Geometry>& rgeom0,
    std::unique_ptr<geom::Geometry>& rgeom1)

{
    cbr.reset(new CommonBitsRemover());

    cbr->add(geom0);
    cbr->add(geom1);

#if GEOS_DEBUG
    const Coordinate& commonCoord = cbr->getCommonCoordinate();
    cerr << "CommonBitsRemover bits: " << commonCoord.x << ", " << commonCoord.y << endl;
#endif

    rgeom0 = geom0->clone();
    cbr->removeCommonBits(rgeom0.get());

    rgeom1 = geom1->clone();
    cbr->removeCommonBits(rgeom1.get());
}

} // namespace geos.precision
} // namespace geos
