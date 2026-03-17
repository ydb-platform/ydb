/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright 2011-2014 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2019 Even Rouault <even.rouault@spatialys.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 **********************************************************************
 *
 * Ported from rtgeom_geos.c from
 *   rttopo - topology library
 *   http://git.osgeo.org/gitea/rttopo/librttopo
 * with relicensing from GPL to LGPL with Copyright holder permission.
 *
 **********************************************************************/

#ifndef GEOS_OP_POLYGONIZE_BUILDAREA_H
#define GEOS_OP_POLYGONIZE_BUILDAREA_H

#include <geos/export.h>

#include <memory>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
}
}

namespace geos {
namespace operation { // geos::operation
namespace polygonize { // geos::operation::polygonize

/** \brief Creates an areal geometry formed by the constituent linework of given geometry.
 *
 * The return type can be a Polygon or MultiPolygon, depending on input.
 * If the input lineworks do not form polygons NULL is returned.
 * The inputs can be LINESTRINGS, MULTILINESTRINGS, POLYGONS, MULTIPOLYGONS, and GeometryCollections.
 *
 * This algorithm will assume all inner geometries represent holes
 *
 * This is the equivalent of the PostGIS ST_BuildArea() function.
 */
class GEOS_DLL BuildArea {

public:

    /** \brief
     * Create a BuildArea object.
     */
    BuildArea() = default;

    ~BuildArea() = default;

    /** \brief Return the area built fromthe constituent linework of the input geometry. */
    std::unique_ptr<geom::Geometry> build(const geom::Geometry* geom);
};

} // namespace geos::operation::polygonize
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_POLYGONIZE_BUILDAREA_H
