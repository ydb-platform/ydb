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
 * Last port: operation/linemerge/EdgeString.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_LINEMERGE_EDGESTRING_H
#define GEOS_OP_LINEMERGE_EDGESTRING_H

#include <geos/export.h>
#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class CoordinateArraySequence;
class CoordinateSequence;
class LineString;
}
namespace operation {
namespace linemerge {
class LineMergeDirectedEdge;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace linemerge { // geos::operation::linemerge

/**
 * \brief
 * A sequence of LineMergeDirectedEdge forming one of the lines that will
 * be output by the line-merging process.
 */
class GEOS_DLL EdgeString {
private:
    const geom::GeometryFactory* factory;
    std::vector<LineMergeDirectedEdge*> directedEdges;
    geom::CoordinateArraySequence* coordinates;
    geom::CoordinateSequence* getCoordinates();
public:
    /**
     * \brief
     * Constructs an EdgeString with the given factory used to
     * convert this EdgeString to a LineString
     */
    EdgeString(const geom::GeometryFactory* newFactory);

    ~EdgeString() = default;

    /**
    * Adds a directed edge which is known to form part of this line.
    */
    void add(LineMergeDirectedEdge* directedEdge);

    /**
     * Converts this EdgeString into a LineString.
     */
    geom::LineString* toLineString();
};

} // namespace geos::operation::linemerge
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_LINEMERGE_EDGESTRING_H
