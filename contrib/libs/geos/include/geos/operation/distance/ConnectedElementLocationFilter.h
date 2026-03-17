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
 **********************************************************************
 *
 * Last port: operation/distance/ConnectedElementLocationFilter.java b98e8234
 *
 **********************************************************************/

#ifndef GEOS_OP_DISTANCE_CONNECTEDELEMENTLOCATIONFILTER_H
#define GEOS_OP_DISTANCE_CONNECTEDELEMENTLOCATIONFILTER_H

#include <geos/export.h>

#include <geos/geom/GeometryFilter.h> // for inheritance
#include <geos/operation/distance/GeometryLocation.h>

#include <memory>
#include <vector>

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
}
}


namespace geos {
namespace operation { // geos::operation
namespace distance { // geos::operation::distance

/** \brief
 * A ConnectedElementPointFilter extracts a single point from each connected
 * element in a Geometry (e.g. a polygon, linestring or point) and returns
 * them in a list. Empty geometries do not provide a location item.
 *
 * The elements of the list are GeometryLocation.
 *
 * Empty geometries do not provide a location item.
 */
class GEOS_DLL ConnectedElementLocationFilter: public geom::GeometryFilter {
private:

    std::vector<std::unique_ptr<GeometryLocation>> locations;
    ConnectedElementLocationFilter() = default;
    ConnectedElementLocationFilter(const ConnectedElementLocationFilter&) = delete;
    ConnectedElementLocationFilter& operator=(const ConnectedElementLocationFilter&) = delete;
public:
    /** \brief
     * Returns a list containing a point from each Polygon, LineString, and
     * Point found inside the specified geometry.
     *
     * Thus, if the specified geometry is not a GeometryCollection,
     * an empty list will be returned. The elements of the list
     * are [GeometryLocations](@ref operation::distance::GeometryLocation).
     */
    static std::vector<std::unique_ptr<GeometryLocation>> getLocations(const geom::Geometry* geom);

    void filter_ro(const geom::Geometry* geom) override;
    void filter_rw(geom::Geometry* geom) override;
};


} // namespace geos::operation::distance
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_DISTANCE_CONNECTEDELEMENTLOCATIONFILTER_H

