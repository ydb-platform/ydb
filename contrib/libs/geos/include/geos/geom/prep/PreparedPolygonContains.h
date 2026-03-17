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
 * Last port: geom/prep/PreparedPolygonContains.java rev 1.5 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_PREPAREDPOLYGONCONTAINS_H
#define GEOS_GEOM_PREP_PREPAREDPOLYGONCONTAINS_H

#include <geos/geom/prep/AbstractPreparedPolygonContains.h> // inherited

// forward declarations
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
 * Computes the <tt>contains</tt> spatial relationship predicate
 * for a {@link PreparedPolygon} relative to all other {@link Geometry} classes.
 *
 * Uses short-circuit tests and indexing to improve performance.
 *
 * It is not possible to short-circuit in all cases, in particular
 * in the case where the test geometry touches the polygon linework.
 * In this case full topology must be computed.
 *
 * @author Martin Davis
 *
 */
class PreparedPolygonContains : public AbstractPreparedPolygonContains {
public:

    /**
     * Creates an instance of this operation.
     *
     * @param prepPoly the PreparedPolygon to evaluate
     */
    PreparedPolygonContains(const PreparedPolygon* const prepPoly);

    /**
     * Tests whether this PreparedPolygon <tt>contains</tt> a given geometry.
     *
     * @param geom the test geometry
     * @return true if the test geometry is contained
     */
    bool
    contains(const geom::Geometry* geom)
    {
        return eval(geom);
    }

    /**
     * Computes the <tt>contains</tt> predicate between a {@link PreparedPolygon}
     * and a {@link Geometry}.
     *
     * @param prep the prepared polygon
     * @param geom a test geometry
     * @return true if the polygon contains the geometry
     */
    static bool
    contains(const PreparedPolygon* const prep, const geom::Geometry* geom)
    {
        PreparedPolygonContains polyInt(prep);
        return polyInt.contains(geom);
    }

protected:
    /**
     * Computes the full topological <tt>contains</tt> predicate.
     * Used when short-circuit tests are not conclusive.
     *
     * @param geom the test geometry
     * @return true if this prepared polygon contains the test geometry
     */
    bool fullTopologicalPredicate(const geom::Geometry* geom) override;

};

} // geos::geom::prep
} // geos::geom
} // geos

#endif // GEOS_GEOM_PREP_PREPAREDPOLYGONCONTAINS_H
