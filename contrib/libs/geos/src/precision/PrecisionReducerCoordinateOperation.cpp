/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: precision/PrecisionreducerCoordinateOperation.java r591 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/precision/PrecisionReducerCoordinateOperation.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/operation/valid/RepeatedPointRemover.h>
#include <geos/util.h>

#include <vector>

using namespace geos::geom;

namespace geos {
namespace precision { // geos.precision

std::unique_ptr<CoordinateSequence>
PrecisionReducerCoordinateOperation::edit(const CoordinateSequence* cs,
        const Geometry* geom)
{
    auto csSize = cs->size();

    if(csSize == 0) {
        return nullptr;
    }

    auto vc = detail::make_unique<std::vector<Coordinate>>(csSize);

    // copy coordinates and reduce
    for(size_t i = 0; i < csSize; ++i) {
        (*vc)[i] = cs->getAt(i);
        targetPM.makePrecise((*vc)[i]);
    }

    // reducedCoords take ownership of 'vc'
    auto reducedCoords = geom->getFactory()->getCoordinateSequenceFactory()->create(vc.release());

    // remove repeated points, to simplify returned geometry as
    // much as possible.
    std::unique_ptr<CoordinateSequence> noRepeatedCoords = operation::valid::RepeatedPointRemover::removeRepeatedPoints(reducedCoords.get());

    /*
     * Check to see if the removal of repeated points
     * collapsed the coordinate List to an invalid length
     * for the type of the parent geometry.
     * It is not necessary to check for Point collapses,
     * since the coordinate list can
     * never collapse to less than one point.
     * If the length is invalid, return the full-length coordinate array
     * first computed, or null if collapses are being removed.
     * (This may create an invalid geometry - the client must handle this.)
     */
    unsigned int minLength = 0;
    if(dynamic_cast<const LineString*>(geom)) {
        minLength = 2;
    }
    if(dynamic_cast<const LinearRing*>(geom)) {
        minLength = 4;
    }

    if(removeCollapsed) {
        reducedCoords = nullptr;
    }

    // return null or original length coordinate array
    if(noRepeatedCoords->getSize() < minLength) {
        return reducedCoords;
    }

    // ok to return shorter coordinate array
    return noRepeatedCoords;
}


} // namespace geos.precision
} // namespace geos
