/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_GEOM_UTIL_GEOMETRYEDITOROPERATION_H
#define GEOS_GEOM_UTIL_GEOMETRYEDITOROPERATION_H

#include <geos/export.h>
#include <memory>

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


/**
 * A interface which specifies an edit operation for Geometries.
 */
class GEOS_DLL GeometryEditorOperation {

public:
    /**
     * Edits a Geometry by returning a new Geometry with a modification.
     * The returned Geometry might be the same as the Geometry passed in.
     *
     * @param geometry the Geometry to modify
     *
     * @param factory the factory with which to construct the modified
     *                Geometry (may be different to the factory of the
     *                input geometry)
     *
     * @return a new Geometry which is a modification of the input Geometry
     */
    virtual std::unique_ptr<Geometry> edit(const Geometry* geometry,
                                           const GeometryFactory* factory) = 0;

    virtual
    ~GeometryEditorOperation() {}
};

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#endif
