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
 ***********************************************************************
 *
 * Last port: operation/overlay/LineBuilder.java rev. 1.15 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_OVERLAY_LINEBUILDER_H
#define GEOS_OP_OVERLAY_LINEBUILDER_H

#include <geos/export.h>

#include <geos/operation/overlay/OverlayOp.h> // for OverlayOp::OpCode enum

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class CoordinateSequence;
class LineString;
}
namespace geomgraph {
class DirectedEdge;
class Edge;
}
namespace algorithm {
class PointLocator;
}
namespace operation {
namespace overlay {
class OverlayOp;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace overlay { // geos::operation::overlay

/** \brief
 * Forms JTS LineStrings out of a the graph of geomgraph::DirectedEdge
 * created by an OverlayOp.
 *
 */
class GEOS_DLL LineBuilder {

public:

    LineBuilder(OverlayOp* newOp,
                const geom::GeometryFactory* newGeometryFactory,
                algorithm::PointLocator* newPtLocator);

    ~LineBuilder() = default;

    /**
     * @return a list of the LineStrings in the result of the specified overlay operation
     */
    std::vector<geom::LineString*>* build(OverlayOp::OpCode opCode);

    /**
     * Collect line edges which are in the result.
     *
     * Line edges are in the result if they are not part of
     * an area boundary, if they are in the result of the overlay operation,
     * and if they are not covered by a result area.
     *
     * @param de the directed edge to test.
     * @param opCode the overlap operation
     * @param edges the list of included line edges.
     */
    void collectLineEdge(geomgraph::DirectedEdge* de,
                         OverlayOp::OpCode opCode,
                         std::vector<geomgraph::Edge*>* edges);

private:
    OverlayOp* op;
    const geom::GeometryFactory* geometryFactory;
    algorithm::PointLocator* ptLocator;
    std::vector<geomgraph::Edge*> lineEdgesList;
    std::vector<geom::LineString*>* resultLineList;
    void findCoveredLineEdges();
    void collectLines(OverlayOp::OpCode opCode);
    void buildLines(OverlayOp::OpCode opCode);
    void labelIsolatedLines(std::vector<geomgraph::Edge*>* edgesList);

    /**
     * Collect edges from Area inputs which should be in the result but
     * which have not been included in a result area.
     * This happens ONLY:
     *
     *  -  during an intersection when the boundaries of two
     *     areas touch in a line segment
     *  -   OR as a result of a dimensional collapse.
     *
     */
    void collectBoundaryTouchEdge(geomgraph::DirectedEdge* de,
                                  OverlayOp::OpCode opCode,
                                  std::vector<geomgraph::Edge*>* edges);

    /**
     * Label an isolated node with its relationship to the target geometry.
     */
    void labelIsolatedLine(geomgraph::Edge* e, int targetIndex);

    /*
     * If the given CoordinateSequence has mixed 3d/2d vertexes
     * set Z for all vertexes missing it.
     * The Z value is interpolated between 3d vertexes and copied
     * from a 3d vertex to the end.
     */
    void propagateZ(geom::CoordinateSequence* cs);
};

} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_OVERLAY_LINEBUILDER_H
