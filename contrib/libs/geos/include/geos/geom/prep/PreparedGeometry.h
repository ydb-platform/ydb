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
 * Last port: geom/prep/PreparedGeometry.java rev. 1.11 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_PREPAREDGEOMETRY_H
#define GEOS_GEOM_PREP_PREPAREDGEOMETRY_H

#include <vector>
#include <memory>
#include <geos/export.h>

// Forward declarations
namespace geos {
    namespace geom {
        class Geometry;
        class Coordinate;
        class CoordinateSequence;
    }
}


namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep

/**
 * \class PreparedGeometry
 *
 * \brief
 * An interface for classes which prepare {@link Geometry}s
 * in order to optimize the performance
 * of repeated calls to specific geometric operations.
 *
 * A given implementation may provide optimized implementations
 * for only some of the specified methods,
 * and delegate the remaining methods to the original {@link Geometry} operations.
 * An implementation may also only optimize certain situations,
 * and delegate others.
 * See the implementing classes for documentation about which methods and situations
 * they optimize.
 *
 */
class GEOS_DLL PreparedGeometry {
public:
    virtual
    ~PreparedGeometry() {}

    /** \brief
     * Gets the original {@link Geometry} which has been prepared.
     *
     * @return the base geometry
     */
    virtual const geom::Geometry& getGeometry() const = 0;

    /** \brief
     * Tests whether the base {@link Geometry} contains a given geometry.
     *
     * @param geom the Geometry to test
     * @return true if this Geometry contains the given Geometry
     *
     * @see Geometry#contains(Geometry)
     */
    virtual bool contains(const geom::Geometry* geom) const = 0;

    /** \brief
     * Tests whether the base {@link Geometry} properly contains
     * a given geometry.
     *
     * The <code>containsProperly</code> predicate has the following
     * equivalent definitions:
     *
     * - Every point of the other geometry is a point of this
     *   geometry's interior.
     * - The DE-9IM Intersection Matrix for the two geometries matches
     *   <code>[T**FF*FF*]</code>
     *
     * In other words, if the test geometry has any interaction with
     * the boundary of the target
     * geometry the result of <tt>containsProperly</tt> is <tt>false</tt>.
     * This is different semantics to the {@link Geometry::contains}
     * predicate, in which test geometries can intersect the target's
     * boundary and still be contained.
     *
     * The advantage of using this predicate is that it can be computed
     * efficiently, since it avoids the need to compute the full
     * topological relationship of the input boundaries in cases where
     * they intersect.
     *
     * An example use case is computing the intersections
     * of a set of geometries with a large polygonal geometry.
     * Since <tt>intersection</tt> is a fairly slow operation, it can
     * be more efficient
     * to use <tt>containsProperly</tt> to filter out test geometries
     * which lie
     * wholly inside the area.  In these cases the intersection is
     * known <i>a priori</i> to be exactly the original test geometry.
     *
     * @param geom the Geometry to test
     * @return true if this Geometry properly contains the given Geometry
     *
     * @see Geometry::contains
     *
     */
    virtual bool containsProperly(const geom::Geometry* geom) const = 0;

    /** \brief
     * Tests whether the base {@link Geometry} is covered by a given geometry.
     *
     * @param geom the Geometry to test
     * @return true if this Geometry is covered by the given Geometry
     *
     * @see Geometry#coveredBy(Geometry)
     */
    virtual bool coveredBy(const geom::Geometry* geom) const = 0;

    /** \brief
     * Tests whether the base {@link Geometry} covers a given geometry.
     *
     * @param geom the Geometry to test
     * @return true if this Geometry covers the given Geometry
     *
     * @see Geometry#covers(Geometry)
     */
    virtual bool covers(const geom::Geometry* geom) const = 0;

    /** \brief
     * Tests whether the base {@link Geometry} crosses a given geometry.
     *
     * @param geom the Geometry to test
     * @return true if this Geometry crosses the given Geometry
     *
     * @see Geometry#crosses(Geometry)
     */
    virtual bool crosses(const geom::Geometry* geom) const = 0;

    /** \brief
     * Tests whether the base {@link Geometry} is disjoint from a given geometry.
     *
     * @param geom the Geometry to test
     * @return true if this Geometry is disjoint from the given Geometry
     *
     * @see Geometry#disjoint(Geometry)
     */
    virtual bool disjoint(const geom::Geometry* geom) const = 0;

    /** \brief
     * Tests whether the base {@link Geometry} intersects a given geometry.
     *
     * @param geom the Geometry to test
     * @return true if this Geometry intersects the given Geometry
     *
     * @see Geometry#intersects(Geometry)
     */
    virtual bool intersects(const geom::Geometry* geom) const = 0;

    /** \brief
     * Tests whether the base {@link Geometry} overlaps a given geometry.
     *
     * @param geom the Geometry to test
     * @return true if this Geometry overlaps the given Geometry
     *
     * @see Geometry#overlaps(Geometry)
     */
    virtual bool overlaps(const geom::Geometry* geom) const = 0;

    /** \brief
     * Tests whether the base {@link Geometry} touches a given geometry.
     *
     * @param geom the Geometry to test
     * @return true if this Geometry touches the given Geometry
     *
     * @see Geometry#touches(Geometry)
     */
    virtual bool touches(const geom::Geometry* geom) const = 0;

    /** \brief
     * Tests whether the base {@link Geometry} is within a given geometry.
     *
     * @param geom the Geometry to test
     * @return true if this Geometry is within the given Geometry
     *
     * @see Geometry#within(Geometry)
     */
    virtual bool within(const geom::Geometry* geom) const = 0;

    /** \brief
     * Compute the nearest locations on the base {@link Geometry} and
     * the given geometry.
     *
     * @param geom the Geometry to compute the nearest point to
     * @return true the nearest points
     *
     */
    virtual std::unique_ptr<geom::CoordinateSequence> nearestPoints(const geom::Geometry* geom) const = 0;

    /** \brief
     * Compute the minimum distance between the base {@link Geometry} and
     * the given geometry.
     *
     * @param geom the Geometry to compute the distance to
     * @return the minimum distance between the two geometries
     *
     */
    virtual double distance(const geom::Geometry* geom) const = 0;
};


} // namespace geos::geom::prep
} // namespace geos::geom
} // namespace geos


#endif // ndef GEOS_GEOM_PREP_PREPAREDGEOMETRY_H
