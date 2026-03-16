/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/util/GeometryEditor.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_UTIL_GEOMETRYEDITOR_H
#define GEOS_GEOM_UTIL_GEOMETRYEDITOR_H

#include <geos/export.h>
#include <memory>

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class GeometryFactory;
class GeometryCollection;
class Polygon;
namespace util {
class GeometryEditorOperation;
}
}
}


namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

/**
 * Supports creating a new Geometry which is a modification of an existing one.
 * Geometry objects are intended to be treated as immutable.
 * This class allows you to "modify" a Geometry
 * by traversing it and creating a new Geometry with the same overall
 * structure but possibly modified components.
 *
 * The following kinds of modifications can be made:
 *
 * - the values of the coordinates may be changed.
 *   Changing coordinate values may make the result Geometry invalid;
 *   this is not checked by the GeometryEditor
 * - the coordinate lists may be changed
 *   (e.g. by adding or deleting coordinates).
 *   The modifed coordinate lists must be consistent with their original
 *   parent component
 *   (e.g. a LinearRing must always have at least 4 coordinates, and the
 *   first and last coordinate must be equal)
 * - components of the original geometry may be deleted
 *   (e.g. holes may be removed from a Polygon, or LineStrings removed
 *   from a MultiLineString). Deletions will be propagated up the component
 *   tree appropriately.
 *
 * Note that all changes must be consistent with the original Geometry's
 * structure
 * (e.g. a Polygon cannot be collapsed into a LineString).
 *
 * The resulting Geometry is not checked for validity.
 * If validity needs to be enforced, the new Geometry's isValid should
 * be checked.
 *
 * @see Geometry::isValid
 *
 */
class GEOS_DLL GeometryEditor {
private:
    /**
     * The factory used to create the modified Geometry
     */
    const GeometryFactory* factory;

    std::unique_ptr<Polygon> editPolygon(const Polygon* polygon,
                                         GeometryEditorOperation* operation);

    std::unique_ptr<GeometryCollection> editGeometryCollection(
            const GeometryCollection* collection,
            GeometryEditorOperation* operation);

public:

    /**
     * Creates a new GeometryEditor object which will create
     * an edited Geometry with the same GeometryFactory as the
     * input Geometry.
     */
    GeometryEditor();

    /**
     * Creates a new GeometryEditor object which will create
     * the edited Geometry with the given GeometryFactory
     *
     * @param newFactory the GeometryFactory to create the edited
     *                   Geometry with
     */
    GeometryEditor(const GeometryFactory* newFactory);

    /**
     * Edit the input Geometry with the given edit operation.
     * Clients will create subclasses of GeometryEditorOperation or
     * CoordinateOperation to perform required modifications.
     *
     * @param geometry the Geometry to edit
     * @param operation the edit operation to carry out
     * @return a new Geometry which is the result of the editing
     *
     */
    std::unique_ptr<Geometry> edit(const Geometry* geometry,
                                   GeometryEditorOperation* operation); // final
};

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#endif
