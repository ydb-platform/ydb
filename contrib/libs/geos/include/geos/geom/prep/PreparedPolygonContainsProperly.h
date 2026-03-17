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
 * Last port: geom/prep/PreparedPolygonContainsProperly.java rev 1.5 (JTS-1.10)
 * (2007-12-12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_PREPAREDPOLYGONCONTAINSPROPERLY_H
#define GEOS_GEOM_PREP_PREPAREDPOLYGONCONTAINSPROPERLY_H

#include <geos/geom/prep/PreparedPolygonPredicate.h> // inherited

namespace geos {
namespace geom {
class Geometry;

namespace prep {
class PreparedPolygon;
}
}
}

namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep

/**
 * \brief
 * Computes the <tt>containsProperly</tt> spatial relationship predicate
 * for {@link PreparedPolygon}s relative to all other {@link Geometry} classes.
 *
 * Uses short-circuit tests and indexing to improve performance.
 *
 * A Geometry A <tt>containsProperly</tt> another Geometry B iff
 * all points of B are contained in the Interior of A.
 * Equivalently, B is contained in A AND B does not intersect
 * the Boundary of A.
 *
 * The advantage to using this predicate is that it can be computed
 * efficiently, with no need to compute topology at individual points.
 * In a situation with many geometries intersecting the boundary
 * of the target geometry, this can make a performance difference.
 *
 * @author Martin Davis
 */
class PreparedPolygonContainsProperly : public PreparedPolygonPredicate {
private:
protected:
public:
    /** \brief
     * Computes the <tt>containsProperly</tt> predicate between a {@link PreparedPolygon}
     * and a {@link Geometry}.
     *
     * @param prep the prepared polygon
     * @param geom a test geometry
     * @return true if the polygon properly contains the geometry
     */
    static
    bool
    containsProperly(const PreparedPolygon* const prep, const geom::Geometry* geom)
    {
        PreparedPolygonContainsProperly polyInt(prep);
        return polyInt.containsProperly(geom);
    }

    /** \brief
     * Creates an instance of this operation.
     *
     * @param prep the PreparedPolygon to evaluate
     */
    PreparedPolygonContainsProperly(const PreparedPolygon* const prep)
        :	PreparedPolygonPredicate(prep)
    { }

    /** \brief
     * Tests whether this PreparedPolygon containsProperly a given geometry.
     *
     * @param geom the test geometry
     * @return true if the test geometry is contained properly
     */
    bool
    containsProperly(const geom::Geometry* geom);

};

} // geos::geom::prep
} // geos::geom
} // geos

#endif // GEOS_GEOM_PREP_PREPAREDPOLYGONCONTAINSPROPERLY_H
