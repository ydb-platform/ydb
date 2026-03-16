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
 **********************************************************************/

#ifndef GEOS_GEOM_LOCATION_H
#define GEOS_GEOM_LOCATION_H

#include <geos/export.h>
#include <iostream> // for ostream

#include <geos/inline.h>

namespace geos {
namespace geom { // geos::geom

/** \brief
 * Constants representing the location of a point relative to a geometry.
 *
 * They can also be thought of as the row or column index of a DE-9IM matrix.
 * For a description of the DE-9IM, see the
 * [OpenGIS Simple Features Specification for SQL](http://www.opengis.org/techno/specs.htm").
 */

enum class GEOS_DLL Location : char {
    /**
     *  Used for uninitialized location values.
     */
    NONE = (char)(-1), // Instead of NULL

    /**
     * DE-9IM row index of the interior of the first geometry and
     * column index of the interior of the second geometry.
     * Location value for the interior of a geometry.
     */
    INTERIOR = 0,

    /**
     * DE-9IM row index of the boundary of the first geometry and
     * column index of the boundary of the second geometry.
     * Location value for the boundary of a geometry.
     */
    BOUNDARY = 1,

    /**
     * DE-9IM row index of the exterior of the first geometry and
     * column index of the exterior of the second geometry.
     * Location value for the exterior of a geometry.
     */
    EXTERIOR = 2
};

GEOS_DLL std::ostream& operator<<(std::ostream& os, const Location& loc);

} // namespace geos::geom
} // namespace geos

#endif // ndef GEOS_GEOM_LOCATION_H

