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
 **********************************************************************
 *
 * Last port: operation/linemerge/LineMerger.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_LINEMERGE_LINEMERGER_H
#define GEOS_OP_LINEMERGE_LINEMERGER_H

#include <geos/export.h>
#include <geos/geom/LineString.h>
#include <geos/operation/linemerge/LineMergeGraph.h> // for composition

#include <memory>
#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class Geometry;
}
namespace planargraph {
class Node;
}
namespace operation {
namespace linemerge {
class EdgeString;
class LineMergeDirectedEdge;
}
}
}


namespace geos {
namespace operation { // geos::operation
namespace linemerge { // geos::operation::linemerge

/**
 *
 * \brief
 * Sews together a set of fully noded LineStrings.
 *
 * Sewing stops at nodes of degree 1 or 3 or more.
 * The exception is an isolated loop, which only has degree-2 nodes,
 * in which case a node is simply chosen as a starting point.
 * The direction of each merged LineString will be that of the majority
 * of the LineStrings from which it was derived.
 *
 * Any dimension of Geometry is handled.
 * The constituent linework is extracted to form the edges.
 * The edges must be correctly noded; that is, they must only meet
 * at their endpoints.
 *
 * The LineMerger will still run on incorrectly noded input
 * but will not form polygons from incorrected noded edges.
 *
 */
class GEOS_DLL LineMerger {

private:

    LineMergeGraph graph;

    std::vector<std::unique_ptr<geom::LineString>> mergedLineStrings;

    std::vector<EdgeString*> edgeStrings;

    const geom::GeometryFactory* factory;

    void merge();

    void buildEdgeStringsForObviousStartNodes();

    void buildEdgeStringsForIsolatedLoops();

    void buildEdgeStringsForUnprocessedNodes();

    void buildEdgeStringsForNonDegree2Nodes();

    void buildEdgeStringsStartingAt(planargraph::Node* node);

    EdgeString* buildEdgeStringStartingWith(LineMergeDirectedEdge* start);

public:
    LineMerger();
    ~LineMerger();

    /**
     * \brief
     * Adds a collection of Geometries to be processed.
     * May be called multiple times.
     *
     * Any dimension of Geometry may be added; the constituent
     * linework will be extracted.
     */
    void add(std::vector<const geom::Geometry*>* geometries);

    /**
     * \brief
     * Adds a Geometry to be processed.
     * May be called multiple times.
     *
     * Any dimension of Geometry may be added; the constituent
     * linework will be extracted.
     */
    void add(const geom::Geometry* geometry);

    /**
     * \brief
     * Returns the LineStrings built by the merging process.
     *
     * Ownership of vector _and_ its elements to caller.
     */
    std::vector<std::unique_ptr<geom::LineString>> getMergedLineStrings();

    void add(const geom::LineString* lineString);

    // Declare type as noncopyable
    LineMerger(const LineMerger& other) = delete;
    LineMerger& operator=(const LineMerger& rhs) = delete;
};

} // namespace geos::operation::linemerge
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_LINEMERGE_LINEMERGER_H
