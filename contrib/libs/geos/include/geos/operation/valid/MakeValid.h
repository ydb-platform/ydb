/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright 2009-2010 Sandro Santilli <strk@kbt.io>
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

#ifndef GEOS_OP_VALID_MAKEVALID_H
#define GEOS_OP_VALID_MAKEVALID_H

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
namespace valid { // geos::operation::valid

/** \brief  The function attempts to create a valid representation of a given
 * invalid geometry without losing any of the input vertices.
 *
 * Already-valid geometries are returned without further intervention.
 *
 * Supported inputs are: POINTS, MULTIPOINTS, LINESTRINGS, MULTILINESTRINGS, POLYGONS, MULTIPOLYGONS and GEOMETRYCOLLECTIONS containing any mix of them.
 *
 * In case of full or partial dimensional collapses, the output geometry may be a collection of lower-to-equal dimension geometries or a geometry of lower dimension.
 *
 * Single polygons may become multi-geometries in case of self-intersections.
 */
class GEOS_DLL MakeValid {

public:

    /** \brief
     * Create a MakeValid object.
     */
    MakeValid() = default;

    ~MakeValid() = default;

    /** \brief Return a valid version of the input geometry. */
    std::unique_ptr<geom::Geometry> build(const geom::Geometry* geom);
};

} // namespace geos::operation::valid
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_VALID_MAKEVALID_H
