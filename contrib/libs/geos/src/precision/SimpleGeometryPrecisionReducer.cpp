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
 ***********************************************************************
 *
 * Last port: precision/SimpleGeometryPrecisionReducer.cpp rev. 1.10 (JTS-1.7)
 *
 **********************************************************************/

#include <geos/precision/SimpleGeometryPrecisionReducer.h>
#include <geos/geom/util/GeometryEditor.h>
#include <geos/geom/util/CoordinateOperation.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/operation/valid/RepeatedPointRemover.h>
#include <geos/util.h>

#include <vector>
#include <typeinfo>

using namespace geos::geom;
using namespace geos::geom::util;

namespace geos {
namespace precision { // geos.precision

namespace {

class PrecisionReducerCoordinateOperation :
    public geom::util::CoordinateOperation {
    using CoordinateOperation::edit;
private:

    SimpleGeometryPrecisionReducer* sgpr;

public:

    PrecisionReducerCoordinateOperation(
        SimpleGeometryPrecisionReducer* newSgpr);

    /// Ownership of returned CoordinateSequence to caller
    std::unique_ptr<CoordinateSequence> edit(const CoordinateSequence* coordinates,
                                             const Geometry* geom) override;
};

PrecisionReducerCoordinateOperation::PrecisionReducerCoordinateOperation(
    SimpleGeometryPrecisionReducer* newSgpr)
{
    sgpr = newSgpr;
}

std::unique_ptr<CoordinateSequence>
PrecisionReducerCoordinateOperation::edit(const CoordinateSequence* cs,
        const Geometry* geom)
{
    if(cs->getSize() == 0) {
        return nullptr;
    }

    auto csSize = cs->size();

    auto vc = detail::make_unique<std::vector<Coordinate>>(csSize);

    // copy coordinates and reduce
    for(unsigned int i = 0; i < csSize; ++i) {
        (*vc)[i] = cs->getAt(i);
        sgpr->getPrecisionModel()->makePrecise((*vc)[i]);
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
    if(typeid(*geom) == typeid(LineString)) {
        minLength = 2;
    }
    if(typeid(*geom) == typeid(LinearRing)) {
        minLength = 4;
    }

    if(sgpr->getRemoveCollapsed()) {
        reducedCoords = nullptr;
    }
    // return null or original length coordinate array
    if(noRepeatedCoords->getSize() < minLength) {
        return reducedCoords;
    }
    // ok to return shorter coordinate array
    return noRepeatedCoords;
}

} // anonymous namespace

//---------------------------------------------------------------


SimpleGeometryPrecisionReducer::SimpleGeometryPrecisionReducer(
    const PrecisionModel* pm)
    :
    newPrecisionModel(pm),
    removeCollapsed(true)
{
    //removeCollapsed = true;
    //changePrecisionModel = false;
    //newPrecisionModel = pm;
}

/*
 * Sets whether the reduction will result in collapsed components
 * being removed completely, or simply being collapsed to an (invalid)
 * Geometry of the same type.
 *
 * @param removeCollapsed if <code>true</code> collapsed components will be removed
 */
void
SimpleGeometryPrecisionReducer::setRemoveCollapsedComponents(bool nRemoveCollapsed)
{
    removeCollapsed = nRemoveCollapsed;
}

const PrecisionModel*
SimpleGeometryPrecisionReducer::getPrecisionModel()
{
    return newPrecisionModel;
}

bool
SimpleGeometryPrecisionReducer::getRemoveCollapsed()
{
    return removeCollapsed;
}

std::unique_ptr<Geometry>
SimpleGeometryPrecisionReducer::reduce(const Geometry* geom)
{
    GeometryEditor geomEdit;
    PrecisionReducerCoordinateOperation prco(this);
    return geomEdit.edit(geom, &prco);
}

} // namespace geos.precision
} // namespace geos

