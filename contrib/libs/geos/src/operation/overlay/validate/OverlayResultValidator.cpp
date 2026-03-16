/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/validate/OverlayResultValidator.java rev. 1.4 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/overlay/validate/OverlayResultValidator.h>
#include <geos/operation/overlay/validate/FuzzyPointLocator.h>
#include <geos/operation/overlay/validate/OffsetPointGenerator.h>
#include <geos/operation/overlay/snap/GeometrySnapper.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>

#include <cassert>
#include <functional>
#include <vector>
#include <memory> // for unique_ptr
#include <algorithm> // for std::min etc.

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#if GEOS_DEBUG
#include <iomanip> // for setprecision
#endif

#define COMPUTE_Z 1
#define USE_ELEVATION_MATRIX 1
#define USE_INPUT_AVGZ 0

using namespace std;
using namespace geos::geom;
using namespace geos::geomgraph;
using namespace geos::algorithm;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay
namespace validate { // geos.operation.overlay.validate

namespace { // anonymous namespace

#if GEOS_DEBUG
unique_ptr<MultiPoint>
toMultiPoint(vector<Coordinate>& coords)
{
    const GeometryFactory& gf = *(GeometryFactory::getDefaultInstance());
    const CoordinateSequenceFactory& csf =
        *(gf.getCoordinateSequenceFactory());

    unique_ptr< vector<Coordinate> > nc(new vector<Coordinate>(coords));
    unique_ptr<CoordinateSequence> cs(csf.create(nc.release()));

    unique_ptr<MultiPoint> mp(gf.createMultiPoint(*cs));

    return mp;
}
#endif

} // anonymous namespace


/* static public */
bool
OverlayResultValidator::isValid(const Geometry& geom0, const Geometry& geom1,
                                OverlayOp::OpCode opCode,
                                const Geometry& result)
{
    OverlayResultValidator validator(geom0, geom1, result);
    return validator.isValid(opCode);
}

/*public*/
OverlayResultValidator::OverlayResultValidator(
    const Geometry& geom0,
    const Geometry& geom1,
    const Geometry& result)
    :
    boundaryDistanceTolerance(
        computeBoundaryDistanceTolerance(geom0, geom1)
    ),
    g0(geom0),
    g1(geom1),
    gres(result),
    fpl0(g0, boundaryDistanceTolerance),
    fpl1(g1, boundaryDistanceTolerance),
    fplres(gres, boundaryDistanceTolerance),
    invalidLocation()
{
}

/*public*/
bool
OverlayResultValidator::isValid(OverlayOp::OpCode overlayOp)
{

    addTestPts(g0);
    addTestPts(g1);
    addTestPts(gres);

    if(! testValid(overlayOp)) {
#if GEOS_DEBUG
        cerr << "OverlayResultValidator:" << endl
             << "Points:" << *toMultiPoint(testCoords) << endl
             << "Geom0: " << g0 << endl
             << "Geom1: " << g1 << endl
             << "Reslt: " << gres << endl
             << "Locat: " << getInvalidLocation()
             << endl;
#endif
        return false;
    }


    return true;
}

/*private*/
void
OverlayResultValidator::addTestPts(const Geometry& g)
{
    OffsetPointGenerator ptGen(g, 5 * boundaryDistanceTolerance);
    unique_ptr< vector<geom::Coordinate> > pts = ptGen.getPoints();
    testCoords.insert(testCoords.end(), pts->begin(), pts->end());
}

/*private*/
void
OverlayResultValidator::addVertices(const Geometry& g)
{
    // TODO: optimize this by not copying coordinates
    //       and pre-allocating memory
    unique_ptr<CoordinateSequence> cs(g.getCoordinates());

    testCoords.reserve(testCoords.size() + cs->size());
    for (size_t i = 0; i < cs->size(); i++) {
        testCoords.push_back(cs->getAt(i));
    }
}

/*private*/
bool
OverlayResultValidator::testValid(OverlayOp::OpCode overlayOp)
{
    for(size_t i = 0, n = testCoords.size(); i < n; ++i) {
        Coordinate& pt = testCoords[i];
        if(! testValid(overlayOp, pt)) {
            invalidLocation = pt;
            return false;
        }
    }
    return true;
}

/*private*/
bool
OverlayResultValidator::testValid(OverlayOp::OpCode overlayOp,
                                  const Coordinate& pt)
{
    // TODO use std::array<geom::Location, 3> ?
    std::vector<geom::Location> location(3);

    location[0] = fpl0.getLocation(pt);
    location[1] = fpl1.getLocation(pt);
    location[2] = fplres.getLocation(pt);

#if GEOS_DEBUG
    cerr << setprecision(10) << "Point " << pt << endl
         << "Loc0: " << location[0] << endl
         << "Loc1: " << location[1] << endl
         << "Locr: " << location[2] << endl;
#endif

    /*
     * If any location is on the Boundary, can't deduce anything,
     * so just return true
     */
    if(find(location.begin(), location.end(), Location::BOUNDARY) != location.end()) {
#if GEOS_DEBUG
        cerr << "OverlayResultValidator: testpoint " << pt <<
             " is on the boundary, blindly returning a positive answer (is valid)" << endl;
#endif
        return true;
    }

    return isValidResult(overlayOp, location);
}

/* private */
bool
OverlayResultValidator::isValidResult(OverlayOp::OpCode overlayOp,
                                      std::vector<geom::Location>& location)
{
    bool expectedInterior = OverlayOp::isResultOfOp(location[0],
                            location[1], overlayOp);

    bool resultInInterior = (location[2] == Location::INTERIOR);

    bool p_isValid = !(expectedInterior ^ resultInInterior);

    return p_isValid;
}

/*private static*/
double
OverlayResultValidator::computeBoundaryDistanceTolerance(
    const geom::Geometry& g0, const geom::Geometry& g1)
{
    using geos::operation::overlay::snap::GeometrySnapper;

    return std::min(GeometrySnapper::computeSizeBasedSnapTolerance(g0),
                    GeometrySnapper::computeSizeBasedSnapTolerance(g1));
}

} // namespace geos.operation.overlay.validate
} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

