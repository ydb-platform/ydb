/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
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
 * Last port: operation/polygonize/PolygonizeEdge.java rev. 1.3 (JTS-1.10)
 *
 **********************************************************************/


#ifndef GEOS_OP_POLYGONIZE_POLYGONIZEEDGE_H
#define GEOS_OP_POLYGONIZE_POLYGONIZEEDGE_H

#include <geos/export.h>

#include <geos/planargraph/Edge.h> // for inheritance

// Forward declarations
namespace geos {
namespace geom {
class LineString;
}
}

namespace geos {
namespace operation { // geos::operation
namespace polygonize { // geos::operation::polygonize

/** \brief
 * An edge of a polygonization graph.
 *
 * @version 1.4
 */
class GEOS_DLL PolygonizeEdge: public planargraph::Edge {
private:
    // Externally owned
    const geom::LineString* line;
public:

    // Keep the given pointer (won't do anything to it)
    PolygonizeEdge(const geom::LineString* newLine);

    // Just return what it was given initially
    const geom::LineString* getLine();
};

} // namespace geos::operation::polygonize
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_POLYGONIZE_POLYGONIZEEDGE_H
