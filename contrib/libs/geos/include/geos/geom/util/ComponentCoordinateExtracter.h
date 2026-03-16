/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_GEOM_UTIL_COMPONENTCOORDINATEEXTRACTER_H
#define GEOS_GEOM_UTIL_COMPONENTCOORDINATEEXTRACTER_H

#include <vector>

#include <geos/geom/GeometryComponentFilter.h>
#include <geos/geom/Geometry.h> // to be removed when we have the .inl
#include <geos/geom/Coordinate.h> // to be removed when we have the .inl
#include <geos/geom/LineString.h> // to be removed when we have the .inl
#include <geos/geom/Point.h> // to be removed when we have the .inl

namespace geos {
namespace geom { // geos::geom
namespace util { // geos::geom::util

/** \brief
 * Extracts a single representative {@link Coordinate}
 * from each connected component of a {@link Geometry}.
 *
 * @version 1.9
 */
class ComponentCoordinateExtracter : public GeometryComponentFilter {
public:
    /**
     * Push the linear components from a single geometry into
     * the provided vector.
     * If more than one geometry is to be processed, it is more
     * efficient to create a single ComponentCoordinateFilter instance
     * and pass it to multiple geometries.
     */
    static void getCoordinates(const Geometry& geom, std::vector<const Coordinate*>& ret);

    /**
     * Constructs a ComponentCoordinateFilter with a list in which
     * to store Coordinates found.
     */
    ComponentCoordinateExtracter(std::vector<const Coordinate*>& newComps);

    void filter_rw(Geometry* geom) override;

    void filter_ro(const Geometry* geom) override;

private:

    Coordinate::ConstVect& comps;

    // Declare type as noncopyable
    ComponentCoordinateExtracter(const ComponentCoordinateExtracter& other) = delete;
    ComponentCoordinateExtracter& operator=(const ComponentCoordinateExtracter& rhs) = delete;
};

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#endif //GEOS_GEOM_UTIL_COMPONENTCOORDINATEEXTRACTER_H
