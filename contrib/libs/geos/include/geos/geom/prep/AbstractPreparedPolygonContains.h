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
 * Last port: geom/prep/AbstractPreparedPolygonContains.java r388 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_ABSTRACTPREPAREDPOLYGONCONTAINS_H
#define GEOS_GEOM_PREP_ABSTRACTPREPAREDPOLYGONCONTAINS_H

#include <geos/geom/prep/PreparedPolygonPredicate.h> // inherited


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
 * A base class containing the logic for computes the <tt>contains</tt>
 * and <tt>covers</tt> spatial relationship predicates
 * for a {@link PreparedPolygon} relative to all other {@link Geometry} classes.
 *
 * Uses short-circuit tests and indexing to improve performance.
 *
 * Contains and covers are very similar, and differ only in how certain
 * cases along the boundary are handled.  These cases require
 * full topological evaluation to handle, so all the code in
 * this class is common to both predicates.
 *
 * It is not possible to short-circuit in all cases, in particular
 * in the case where line segments of the test geometry touches the polygon
 * linework.
 * In this case full topology must be computed.
 * (However, if the test geometry consists of only points, this
 * <i>can</i> be evaluated in an optimized fashion.
 *
 * @author Martin Davis
 *
 */
class AbstractPreparedPolygonContains : public PreparedPolygonPredicate {
private:
    // information about geometric situation
    bool hasSegmentIntersection;
    bool hasProperIntersection;
    bool hasNonProperIntersection;

    bool isProperIntersectionImpliesNotContainedSituation(const geom::Geometry* testGeom);

    /**
     * Tests whether a geometry consists of a single polygon with no holes.
     *
     * @return true if the geometry is a single polygon with no holes
     */
    bool isSingleShell(const geom::Geometry& geom);

    void findAndClassifyIntersections(const geom::Geometry* geom);

protected:
    /**
     * This flag controls a difference between contains and covers.
     *
     * For contains the value is true.
     * For covers the value is false.
     */
    bool requireSomePointInInterior;

    /**
     * Evaluate the <tt>contains</tt> or <tt>covers</tt> relationship
     * for the given geometry.
     *
     * @param geom the test geometry
     * @return true if the test geometry is contained
     */
    bool eval(const geom::Geometry* geom);

    /**
     * Evaluate the <tt>contains</tt> or <tt>covers</tt> relationship
     * for the given Puntal geometry.
     *
     * @param geom the test geometry
     * @param outermostLoc outermost Location of all points in geom
     * @return true if the test geometry is contained/covered in the target
     */
    bool evalPointTestGeom(const geom::Geometry* geom, geom::Location outermostLoc);

    /**
     * Computes the full topological predicate.
     * Used when short-circuit tests are not conclusive.
     *
     * @param geom the test geometry
     * @return true if this prepared polygon has the relationship with the test geometry
     */
    virtual bool fullTopologicalPredicate(const geom::Geometry* geom) = 0;

public:
    AbstractPreparedPolygonContains(const PreparedPolygon* const p_prepPoly)
        :	PreparedPolygonPredicate(p_prepPoly),
          hasSegmentIntersection(false),
          hasProperIntersection(false),
          hasNonProperIntersection(false),
          requireSomePointInInterior(true)
    { }

    AbstractPreparedPolygonContains(const PreparedPolygon* const p_prepPoly, bool p_requireSomePointInInterior)
        :	PreparedPolygonPredicate(p_prepPoly),
          hasSegmentIntersection(false),
          hasProperIntersection(false),
          hasNonProperIntersection(false),
          requireSomePointInInterior(p_requireSomePointInInterior)
    { }

    ~AbstractPreparedPolygonContains() override
    { }

};

} // geos::geom::prep
} // geos::geom
} // geos

#endif // GEOS_GEOM_PREP_ABSTRACTPREPAREDPOLYGONCONTAINS_H
