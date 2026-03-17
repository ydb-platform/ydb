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
 * Last port: operation/distance/ConnectedElementPointFilter.java rev. 1.7 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_DISTANCE_CONNECTEDELEMENTPOINTFILTER_H
#define GEOS_OP_DISTANCE_CONNECTEDELEMENTPOINTFILTER_H

#include <geos/export.h>

#include <geos/geom/GeometryFilter.h> // for inheritance

#include <vector>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class Geometry;
}
}


namespace geos {
namespace operation { // geos::operation
namespace distance { // geos::operation::distance

/** \brief
 * Extracts a single point
 * from each connected element in a Geometry
 * (e.g. a polygon, linestring or point)
 * and returns them in a list
 */
class GEOS_DLL ConnectedElementPointFilter: public geom::GeometryFilter {

private:
    std::vector<const geom::Coordinate*>* pts;

public:
    /**
     * Returns a list containing a Coordinate from each Polygon, LineString, and Point
     * found inside the specified geometry. Thus, if the specified geometry is
     * not a GeometryCollection, an empty list will be returned.
     */
    static std::vector<const geom::Coordinate*>* getCoordinates(const geom::Geometry* geom);

    ConnectedElementPointFilter(std::vector<const geom::Coordinate*>* newPts)
        :
        pts(newPts)
    {}

    void filter_ro(const geom::Geometry* geom) override;

    //void filter_rw(geom::Geometry * /*geom*/) {};
};


} // namespace geos::operation::distance
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_DISTANCE_CONNECTEDELEMENTPOINTFILTER_H

