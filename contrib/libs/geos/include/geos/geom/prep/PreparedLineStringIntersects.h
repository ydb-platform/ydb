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
 *
 **********************************************************************
 *
 * Last port: geom/prep/PreparedLineStringIntersects.java r338 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_PREPAREDLINESTRINGINTERSECTS_H
#define GEOS_GEOM_PREP_PREPAREDLINESTRINGINTERSECTS_H

#include <geos/geom/prep/PreparedLineString.h>
#include <geos/algorithm/PointLocator.h>
#include <geos/geom/util/ComponentCoordinateExtracter.h>
#include <geos/geom/Coordinate.h>
#include <geos/noding/SegmentStringUtil.h>

namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep

/**
 * \brief
 * Computes the <tt>intersects</tt> spatial relationship predicate
 * for a target {@link PreparedLineString} relative to all other
 * {@link Geometry} classes.
 *
 * Uses short-circuit tests and indexing to improve performance.
 *
 * @author Martin Davis
 *
 */
class PreparedLineStringIntersects {
public:

    /**
     * Computes the intersects predicate between a {@link PreparedLineString}
     * and a {@link Geometry}.
     *
     * @param prep the prepared linestring
     * @param geom a test geometry
     * @return true if the linestring intersects the geometry
     */
    static bool
    intersects(PreparedLineString& prep, const geom::Geometry* geom)
    {
        PreparedLineStringIntersects op(prep);
        return op.intersects(geom);
    }

    /**
     * \todo FIXME - mloskot: Why not taking linestring through const reference?
     */
    PreparedLineStringIntersects(PreparedLineString& prep)
        : prepLine(prep)
    { }

    /**
     * Tests whether this geometry intersects a given geometry.
     *
     * @param g the test geometry
     * @return true if the test geometry intersects
     */
    bool intersects(const geom::Geometry* g) const;

protected:
    PreparedLineString& prepLine;

    /**
     * Tests whether any representative point of the test Geometry intersects
     * the target geometry.
     * Only handles test geometries which are Puntal (dimension 0)
     *
     * @param testGeom a Puntal geometry to test
     * @return true if any point of the argument intersects the prepared geometry
     */
    bool isAnyTestPointInTarget(const geom::Geometry* testGeom) const;

    // Declare type as noncopyable
    PreparedLineStringIntersects(const PreparedLineStringIntersects& other) = delete;
    PreparedLineStringIntersects& operator=(const PreparedLineStringIntersects& rhs) = delete;
};

} // namespace geos::geom::prep
} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_PREP_PREPAREDLINESTRINGINTERSECTS_H

