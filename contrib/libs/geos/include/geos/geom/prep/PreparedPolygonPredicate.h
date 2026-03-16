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
 * Last port: geom/prep/PreparedPolygonPredicate.java rev. 1.4 (JTS-1.10)
 * (2007-12-12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_PREPAREDPOLYGONPREDICATE_H
#define GEOS_GEOM_PREP_PREPAREDPOLYGONPREDICATE_H

#include <geos/geom/Coordinate.h>
#include <geos/geom/Location.h>

// forward declarations
namespace geos {
namespace algorithm {
namespace locate {
class PointOnGeometryLocator;
}
}
namespace geom {
class Geometry;

namespace prep {
class PreparedPolygon;
}
}
namespace noding {
class FastSegmentSetIntersectionFinder;
}
}


namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep

/**
 * \brief
 * A base class for predicate operations on {@link PreparedPolygon}s.
 *
 * @author mbdavis
 *
 */
class PreparedPolygonPredicate {
private:
    // Declare type as noncopyable
    PreparedPolygonPredicate(const PreparedPolygonPredicate& other) = delete;
    PreparedPolygonPredicate& operator=(const PreparedPolygonPredicate& rhs) = delete;

protected:
    const PreparedPolygon* const prepPoly;

    /** \brief
     * Returns the outermost Location among a test point from each
     * components of the test geometry.
     *
     * @param testGeom a geometry to test
     * @return the outermost Location
     */
    geom::Location getOutermostTestComponentLocation(const geom::Geometry* testGeom) const;

    /** \brief
     * Tests whether all components of the test Geometry
     * are contained in the interior of the target geometry.
     *
     * Handles both linear and point components.
     *
     * @param testGeom a geometry to test
     * @return true if all componenta of the argument are contained in
     *              the target geometry interior
     */
    bool isAllTestComponentsInTargetInterior(const geom::Geometry* testGeom) const;

    /** \brief
     * Tests whether any component of the test Geometry intersects
     * the area of the target geometry.
     *
     * Handles test geometries with both linear and point components.
     *
     * @param testGeom a geometry to test
     * @return true if any component of the argument intersects the
     *              prepared geometry
     */
    bool isAnyTestComponentInTarget(const geom::Geometry* testGeom) const;

    /** \brief
     * Tests whether any component of the test Geometry intersects
     * the interior of the target geometry.
     *
     * Handles test geometries with both linear and point components.
     *
     * @param testGeom a geometry to test
     * @return true if any component of the argument intersects the
     *              prepared area geometry interior
     */
    bool isAnyTestComponentInTargetInterior(const geom::Geometry* testGeom) const;

    /**
     * Tests whether any component of the target geometry
     * intersects the test geometry (which must be an areal geometry)
     *
     * @param testGeom the test geometry
     * @param targetRepPts the representative points of the target geometry
     * @return true if any component intersects the areal test geometry
     */
    bool isAnyTargetComponentInAreaTest(const geom::Geometry* testGeom,
                                        const geom::Coordinate::ConstVect* targetRepPts) const;

public:
    /** \brief
     * Creates an instance of this operation.
     *
     * @param p_prepPoly the PreparedPolygon to evaluate
     */
    PreparedPolygonPredicate(const PreparedPolygon* const p_prepPoly)
        :	prepPoly(p_prepPoly)
    { }

    virtual
    ~PreparedPolygonPredicate()
    { }

};

} // namespace geos::geom::prep
} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_PREP_PREPAREDPOLYGONPREDICATE_H

