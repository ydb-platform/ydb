/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/union/PointGeometryUnion.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_UNION_POINTGEOMETRYUNION_H
#define GEOS_OP_UNION_POINTGEOMETRYUNION_H
#include <geos/export.h>

#include <vector>
#include <algorithm>

// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class Geometry;
}
}

namespace geos {
namespace operation { // geos::operation
namespace geounion {  // geos::operation::geounion

/**
 * \brief
 * Computes the union of a puntal geometry with another
 * arbitrary [Geometry](@ref geom::Geometry).
 *
 * Does not copy any component geometries.
 *
 */
class GEOS_DLL PointGeometryUnion {
public:

    static std::unique_ptr<geom::Geometry> Union(
        const geom::Geometry& pointGeom,
        const geom::Geometry& otherGeom);


    PointGeometryUnion(const geom::Geometry& pointGeom,
                       const geom::Geometry& otherGeom);

    std::unique_ptr<geom::Geometry> Union() const;

private:
    const geom::Geometry& pointGeom;
    const geom::Geometry& otherGeom;
    const geom::GeometryFactory* geomFact;

    // Declared as non-copyable
    PointGeometryUnion(const PointGeometryUnion& other);
    PointGeometryUnion& operator=(const PointGeometryUnion& rhs);
};

} // namespace geos::operation::union
} // namespace geos::operation
} // namespace geos

#endif
