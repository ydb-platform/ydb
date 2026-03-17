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
 * Last port: geom/prep/BasicPreparedGeometry.java rev. 1.5 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_BASICPREPAREDGEOMETRY_H
#define GEOS_GEOM_PREP_BASICPREPAREDGEOMETRY_H

#include <geos/geom/prep/PreparedGeometry.h> // for inheritance
//#include <geos/algorithm/PointLocator.h>
//#include <geos/geom/util/ComponentCoordinateExtracter.h>
#include <geos/geom/Coordinate.h>
//#include <geos/geom/Location.h>

#include <vector>
#include <string>

namespace geos {
namespace geom {
class Geometry;
class Coordinate;
}
}


namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep

// * \class BasicPreparedGeometry

/**
 *
 * \brief
 * A base class for {@link PreparedGeometry} subclasses.
 *
 * Contains default implementations for methods, which simply delegate
 * to the equivalent {@link Geometry} methods.
 * This class may be used as a "no-op" class for Geometry types
 * which do not have a corresponding {@link PreparedGeometry} implementation.
 *
 * @author Martin Davis
 *
 */
class BasicPreparedGeometry: public PreparedGeometry {
private:
    const geom::Geometry* baseGeom;
    Coordinate::ConstVect representativePts;

protected:
    /**
     * Sets the original {@link Geometry} which will be prepared.
     */
    void setGeometry(const geom::Geometry* geom);

    /**
     * Determines whether a Geometry g interacts with
     * this geometry by testing the geometry envelopes.
     *
     * @param g a Geometry
     * @return true if the envelopes intersect
     */
    bool envelopesIntersect(const geom::Geometry* g) const;

    /**
     * Determines whether the envelope of
     * this geometry covers the Geometry g.
     *
     *
     * @param g a Geometry
     * @return true if g is contained in this envelope
     */
    bool envelopeCovers(const geom::Geometry* g) const;

public:
    BasicPreparedGeometry(const Geometry* geom);

    ~BasicPreparedGeometry() override = default;

    const geom::Geometry&
    getGeometry() const override
    {
        return *baseGeom;
    }

    /**
     * Gets the list of representative points for this geometry.
     * One vertex is included for every component of the geometry
     * (i.e. including one for every ring of polygonal geometries)
     *
     * @return a List of Coordinate
     */
    const Coordinate::ConstVect*
    getRepresentativePoints()  const
    {
        return &representativePts;
    }

    /**
     * Tests whether any representative of the target geometry
     * intersects the test geometry.
     * This is useful in A/A, A/L, A/P, L/P, and P/P cases.
     *
     * @param testGeom the test geometry
     * @return true if any component intersects the areal test geometry
     */
    bool isAnyTargetComponentInTest(const geom::Geometry* testGeom) const;

    /**
     * Default implementation.
     */
    bool contains(const geom::Geometry* g) const override;

    /**
     * Default implementation.
     */
    bool containsProperly(const geom::Geometry* g) const override;

    /**
     * Default implementation.
     */
    bool coveredBy(const geom::Geometry* g) const override;

    /**
     * Default implementation.
     */
    bool covers(const geom::Geometry* g) const override;

    /**
     * Default implementation.
     */
    bool crosses(const geom::Geometry* g) const override;

    /**
     * Standard implementation for all geometries.
     * Supports {@link GeometryCollection}s as input.
     */
    bool disjoint(const geom::Geometry* g) const override;

    /**
     * Default implementation.
     */
    bool intersects(const geom::Geometry* g) const override;

    /**
     * Default implementation.
     */
    bool overlaps(const geom::Geometry* g) const override;

    /**
     * Default implementation.
     */
    bool touches(const geom::Geometry* g) const override;

    /**
     * Default implementation.
     */
    bool within(const geom::Geometry* g) const override;

    /**
     * Default implementation.
     */
    std::unique_ptr<geom::CoordinateSequence> nearestPoints(const geom::Geometry* g) const override;

    /**
     * Default implementation.
     */
    double distance(const geom::Geometry* g) const override;

    std::string toString();

};

} // namespace geos::geom::prep
} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_PREP_BASICPREPAREDGEOMETRY_H
