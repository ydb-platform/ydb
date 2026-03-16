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
 * Last port: operation/distance/GeometryLocation.java rev. 1.7 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_DISTANCE_GEOMETRYLOCATION_H
#define GEOS_OP_DISTANCE_GEOMETRYLOCATION_H

#include <geos/export.h>

#include <geos/geom/Coordinate.h> // for composition

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
 * Represents the location of a point on a Geometry.
 *
 * Maintains both the actual point location
 * (which may not be exact, if the point is not a vertex)
 * as well as information about the component
 * and segment index where the point occurs.
 * Locations inside area Geometrys will not have an associated segment index,
 * so in this case the segment index will have the sentinel value of
 * INSIDE_AREA.
 */
class GEOS_DLL GeometryLocation {
private:
    const geom::Geometry* component;
    size_t segIndex;
    bool inside_area;
    geom::Coordinate pt;
public:
    /** \brief
     * A Special value of segmentIndex used for locations
     * inside area geometries.
     *
     * These locations are not located on a segment,
     * and thus do not have an associated segment index.
     */
    static const int INSIDE_AREA = -1;

    /** \brief
     * Constructs a GeometryLocation specifying a point on a geometry,
     * as well as the segment that the point is on (or INSIDE_AREA if
     * the point is not on a segment).
     *
     * @param component the component of the geometry containing the point
     * @param segIndex the segment index of the location, or INSIDE_AREA
     * @param pt the coordinate of the location
     */
    GeometryLocation(const geom::Geometry* component,
                     size_t segIndex, const geom::Coordinate& pt);

    /** \brief
     * Constructs a GeometryLocation specifying a point inside an
     * area geometry.
     *
     * @param component the component of the geometry containing the point
     * @param pt the coordinate of the location
     */
    GeometryLocation(const geom::Geometry* component,
                     const geom::Coordinate& pt);

    /**
     * Returns the geometry component on (or in) which this location occurs.
     */
    const geom::Geometry* getGeometryComponent();

    /**
     * Returns the segment index for this location.
     *
     * If the location is inside an
     * area, the index will have the value INSIDE_AREA;
     *
     * @return the segment index for the location, or INSIDE_AREA
     */
    size_t getSegmentIndex();

    /**
     * Returns the geom::Coordinate of this location.
     */
    geom::Coordinate& getCoordinate();

    /** \brief
     * Tests whether this location represents a point
     * inside an area geometry.
     */
    bool isInsideArea();

    std::string toString();
};

} // namespace geos::operation::distance
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_DISTANCE_GEOMETRYLOCATION_H

