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
 * Last port: geom/prep/PreparedPolygonIntersects.java rev 1.6 (JTS-1.10)
 * (2007-12-12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_PREPAREDPOLYGONINTERSECTS_H
#define GEOS_GEOM_PREP_PREPAREDPOLYGONINTERSECTS_H

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
 * Computes the <tt>intersects</tt> spatial relationship predicate
 * for {@link PreparedPolygon}s relative to all other {@link Geometry} classes.
 *
 * Uses short-circuit tests and indexing to improve performance.
 *
 * @author Martin Davis
 *
 */
class PreparedPolygonIntersects : public PreparedPolygonPredicate {
private:
protected:
public:
    /** \brief
     * Computes the intersects predicate between a {@link PreparedPolygon}
     * and a {@link Geometry}.
     *
     * @param prep the prepared polygon
     * @param geom a test geometry
     * @return true if the polygon intersects the geometry
     */
    static bool
    intersects(const PreparedPolygon* const prep, const geom::Geometry* geom)
    {
        PreparedPolygonIntersects polyInt(prep);
        return polyInt.intersects(geom);
    }

    /** \brief
     * Creates an instance of this operation.
     *
     * @param prep the PreparedPolygon to evaluate
     */
    PreparedPolygonIntersects(const PreparedPolygon* const prep)
        :	PreparedPolygonPredicate(prep)
    { }

    /** \brief
     * Tests whether this PreparedPolygon intersects a given geometry.
     *
     * @param geom the test geometry
     * @return true if the test geometry intersects
     */
    bool intersects(const geom::Geometry* geom);

};

} // geos::geom::prep
} // geos::geom
} // geos

#endif // GEOS_GEOM_PREP_PREPAREDPOLYGONINTERSECTS_H
