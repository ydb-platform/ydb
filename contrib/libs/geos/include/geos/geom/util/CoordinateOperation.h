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

#ifndef GEOS_GEOM_UTIL_COORDINATEOPERATION_H
#define GEOS_GEOM_UTIL_COORDINATEOPERATION_H

#include <geos/export.h>
#include <geos/geom/util/GeometryEditorOperation.h> // for inheritance

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class CoordinateSequence;
class GeometryFactory;
}
}


namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

/**
 * A GeometryEditorOperation which modifies the coordinate list of a
 * Geometry.
 * Operates on Geometry subclasses which contains a single coordinate list.
 */
class GEOS_DLL CoordinateOperation: public GeometryEditorOperation {

public:

    /**
     * Return a newly created geometry, ownership to caller
     */
    std::unique_ptr<Geometry> edit(const Geometry* geometry,
                                   const GeometryFactory* factory) override;

    /**
     * Edits the array of Coordinate from a Geometry.
     *
     * @param coordinates the coordinate array to operate on
     * @param geometry the geometry containing the coordinate list
     * @return an edited coordinate array (which may be the same as
     *         the input)
     */
    virtual std::unique_ptr<CoordinateSequence> edit(const CoordinateSequence* coordinates,
                                                     const Geometry* geometry) = 0;


    ~CoordinateOperation() override = default;
};



} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#endif
