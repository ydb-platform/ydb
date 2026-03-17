/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006-2011 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/util/GeometryCombiner.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_UTIL_GEOMETRYCOMBINER_H
#define GEOS_GEOM_UTIL_GEOMETRYCOMBINER_H

#include <memory>
#include <vector>

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class GeometryFactory;
}
}

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

/** \brief
 * Combines [Geometrys](@ref Geometry) to produce a GeometryCollection
 * of the most appropriate type.
 *
 * Input geometries which are already collections will have their elements
 * extracted first.
 * No validation of the result geometry is performed.
 * (The only case where invalidity is possible is where polygonal geometries
 * are combined and result in a self-intersection).
 *
 * @see GeometryFactory#buildGeometry
 */
class GeometryCombiner {
public:
    /** \brief
     * Combines a collection of geometries.
     *
     * @param geoms the geometries to combine (ownership left to caller)
     * @return the combined geometry
     */
    static std::unique_ptr<Geometry> combine(std::vector<const Geometry*> const& geoms);
    static std::unique_ptr<Geometry> combine(std::vector<std::unique_ptr<Geometry>> const& geoms);

    /** \brief
     * Combines two geometries.
     *
     * @param g0 a geometry to combine (ownership left to caller)
     * @param g1 a geometry to combine (ownership left to caller)
     * @return the combined geometry
     */
    static std::unique_ptr<Geometry> combine(const Geometry* g0, const Geometry* g1);

    /** \brief
     * Combines three geometries.
     *
     * @param g0 a geometry to combine (ownership left to caller)
     * @param g1 a geometry to combine (ownership left to caller)
     * @param g2 a geometry to combine (ownership left to caller)
     * @return the combined geometry
     */
    static std::unique_ptr<Geometry> combine(const Geometry* g0, const Geometry* g1, const Geometry* g2);

private:
    GeometryFactory const* geomFactory;
    bool skipEmpty;
    std::vector<const Geometry*> const& inputGeoms;

public:
    /** \brief
     * Creates a new combiner for a collection of geometries.
     *
     * @param geoms the geometries to combine
     */
    GeometryCombiner(std::vector<const Geometry*> const& geoms);

    /** \brief
     * Extracts the GeometryFactory used by the geometries in a collection.
     *
     * @param geoms
     * @return a GeometryFactory
     */
    static GeometryFactory const* extractFactory(std::vector<const Geometry*> const& geoms);

    /** \brief
     * Computes the combination of the input geometries
     * to produce the most appropriate Geometry or GeometryCollection.
     *
     * @return a Geometry which is the combination of the inputs
     */
    std::unique_ptr<Geometry> combine();

private:
    void extractElements(const Geometry* geom, std::vector<const Geometry*>& elems);

    // Declare type as noncopyable
    GeometryCombiner(const GeometryCombiner& other) = delete;
    GeometryCombiner& operator=(const GeometryCombiner& rhs) = delete;
};

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#endif
