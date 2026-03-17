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
 * Last port: simplify/DouglasPeuckerLineSimplifier.java rev. 1.4
 *
 **********************************************************************/

#include <geos/simplify/DouglasPeuckerLineSimplifier.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/LineSegment.h>

#include <vector>
#include <memory> // for unique_ptr

namespace geos {

/// Line simplification algorithms
namespace simplify { // geos::simplify

/*public static*/
DouglasPeuckerLineSimplifier::CoordsVectAutoPtr
DouglasPeuckerLineSimplifier::simplify(
    const DouglasPeuckerLineSimplifier::CoordsVect& nPts,
    double distanceTolerance)
{
    DouglasPeuckerLineSimplifier simp(nPts);
    simp.setDistanceTolerance(distanceTolerance);
    return simp.simplify();
}

/*public*/
DouglasPeuckerLineSimplifier::DouglasPeuckerLineSimplifier(
    const DouglasPeuckerLineSimplifier::CoordsVect& nPts)
    :
    pts(nPts)
{
}

/*public*/
void
DouglasPeuckerLineSimplifier::setDistanceTolerance(
    double nDistanceTolerance)
{
    distanceTolerance = nDistanceTolerance;
}

/*public*/
DouglasPeuckerLineSimplifier::CoordsVectAutoPtr
DouglasPeuckerLineSimplifier::simplify()
{
    CoordsVectAutoPtr coordList(new CoordsVect());

    // empty coordlist is the simplest, won't simplify further
    if(pts.empty()) {
        return coordList;
    }

    usePt = BoolVectAutoPtr(new BoolVect(pts.size(), true));
    simplifySection(0, pts.size() - 1);

    for(std::size_t i = 0, n = pts.size(); i < n; ++i) {
        if(usePt->operator[](i)) {
            coordList->push_back(pts[i]);
        }
    }

    // unique_ptr transfer ownership to its
    // returned copy
    return coordList;
}

/*private*/
void
DouglasPeuckerLineSimplifier::simplifySection(
    std::size_t i,
    std::size_t j)
{
    if((i + 1) == j) {
        return;
    }

    geos::geom::LineSegment seg(pts[i], pts[j]);
    double maxDistance = -1.0;

    std::size_t maxIndex = i;

    for(std::size_t k = i + 1; k < j; k++) {
        double distance = seg.distance(pts[k]);
        if(distance > maxDistance) {
            maxDistance = distance;
            maxIndex = k;
        }
    }
    if(maxDistance <= distanceTolerance) {
        for(std::size_t k = i + 1; k < j; k++) {
            usePt->operator[](k) = false;
        }
    }
    else {
        simplifySection(i, maxIndex);
        simplifySection(maxIndex, j);
    }
}

} // namespace geos::simplify
} // namespace geos
