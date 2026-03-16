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

#pragma once

#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/algorithm/locate/PointOnGeometryLocator.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Location.h>
#include <geos/export.h>

#include <array>

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


/**
 * Manages the input geometries for an overlay operation.
 * The second geometry is allowed to be null,
 * to support for instance precision reduction.
 *
 * @author Martin Davis
 *
 */

using namespace geos::algorithm::locate;
using namespace geos::geom;

class GEOS_DLL InputGeometry {

private:

    // Members
    std::array<const Geometry*, 2> geom;
    std::unique_ptr<PointOnGeometryLocator> ptLocatorA;
    std::unique_ptr<PointOnGeometryLocator> ptLocatorB;
    std::array<bool, 2> isCollapsed;



public:

    InputGeometry(const Geometry* geomA, const Geometry* geomB);

    bool isSingle() const;
    int getDimension(int index) const;
    const Geometry* getGeometry(int geomIndex) const;
    const Envelope* getEnvelope(int geomIndex) const;
    bool isEmpty(int geomIndex) const;
    bool isArea(int geomIndex) const;
    int getAreaIndex() const;
    bool isLine(int geomIndex) const;
    bool isAllPoints() const;
    bool hasPoints() const;

    /**
    * Tests if an input geometry has edges.
    * This indicates that topology needs to be computed for them.
    *
    * @param geomIndex
    * @return true if the input geometry has edges
    */
    bool hasEdges(int geomIndex) const;

    /**
    * Determines the location within an area geometry.
    * This allows disconnected edges to be fully
    * located.
    *
    * @param geomIndex the index of the geometry
    * @param pt the coordinate to locate
    * @return the location of the coordinate
    *
    * @see Location
    */
    Location locatePointInArea(int geomIndex, const Coordinate& pt);

    PointOnGeometryLocator* getLocator(int geomIndex);
    void setCollapsed(int geomIndex, bool isGeomCollapsed);


};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

