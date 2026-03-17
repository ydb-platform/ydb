/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#pragma once

#include <geos/export.h>

#include <geos/operation/overlayng/InputGeometry.h>
#include <geos/operation/overlayng/OverlayNG.h>
#include <geos/geom/Location.h>
#include <geos/geom/LineString.h>

#include <vector>


// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class LineString;
}
namespace operation {
namespace overlayng {
class OverlayEdge;
class OverlayGraph;
class OverlayLabel;
class InputGeometry;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

/**
 * Finds and builds overlay result lines from the overlay graph.
 * Output linework has the following semantics:
 *
 *  - Linework is fully noded
 *  - Lines are as long as possible between nodes
 *
 * Various strategies are possible for how to
 * merge graph edges into lines.
 * This implementation uses the approach
 * of having output lines run contiguously from node to node.
 * For rings a node point is chosen arbitrarily.
 *
 * Another possible strategy would be to preserve input linework
 * as far as possible (i.e. any sections of input lines which are not
 * coincident with other linework would be preserved).
 *
 * It would also be possible to output LinearRings,
 * if the input is a LinearRing and is unchanged.
 * This will require additional info from the input linework.
 *
 * @author Martin Davis
 */
class GEOS_DLL LineBuilder {

private:

    // Members
    OverlayGraph* graph;
    int opCode;
    const geom::GeometryFactory* geometryFactory;
    bool hasResultArea;
    int inputAreaIndex;
    std::vector<std::unique_ptr<geom::LineString>> lines;

    /**
    * Indicates whether intersections are allowed to produce
    * heterogeneous results including proper boundary touches.
    * This does not control inclusion of touches along collapses.
    * True provides the original JTS semantics.
    */
    bool isAllowMixedResult = ! OverlayNG::STRICT_MODE_DEFAULT;

    /**
    * Allow lines created by area topology collapses
    * to appear in the result.
    * True provides the original JTS semantics.
    */
    bool isAllowCollapseLines = ! OverlayNG::STRICT_MODE_DEFAULT;

    void markResultLines();

    /**
    * Checks if the topology indicated by an edge label
    * determines that this edge should be part of a result line.
    *
    * Note that the logic here relies on the semantic
    * that for intersection lines are only returned if
    * there is no result area components.
    */
    bool isResultLine(const OverlayLabel* lbl) const;

    /**
    * Determines the effective location for a line,
    * for the purpose of overlay operation evaluation.
    * Line edges and Collapses are reported as INTERIOR
    * so they may be included in the result
    * if warranted by the effect of the operation
    * on the two edges.
    * (For instance, the intersection of line edge and a collapsed boundary
    * is included in the result).
    */
    geom::Location effectiveLocation(const OverlayLabel* lbl, int geomIndex) const;

    void addResultLines();
    void addResultLinesMerged();

    std::unique_ptr<geom::LineString> toLine(OverlayEdge* edge);

    void addResultLinesForNodes();

    /**
    * Adds lines which form rings (i.e. have only degree-2 vertices).
    */
    void addResultLinesRings();

    /**
    * Traverses edges from edgeStart which
    * lie in a single line (have degree = 2).
    *
    * The direction of the linework is preserved as far as possible.
    * Specifically, the direction of the line is determined
    * by the start edge direction. This implies
    * that if all edges are reversed, the created line
    * will be reversed to match.
    * (Other more complex strategies would be possible.
    * E.g. using the direction of the majority of segments,
    * or preferring the direction of the A edges.)
    */
    std::unique_ptr<geom::LineString> buildLine(OverlayEdge* node);

    /*
    * Finds the next edge around a node which forms
    * part of a result line.
    */
    OverlayEdge* nextLineEdgeUnvisited(OverlayEdge* node) const;

    /**
    * Computes the degree of the line edges incident on a node
    */
    int degreeOfLines(OverlayEdge* node) const;



public:

    LineBuilder(const InputGeometry* inputGeom, OverlayGraph* p_graph, bool p_hasResultArea, int p_opCode, const geom::GeometryFactory* geomFact)
        : graph(p_graph)
        , opCode(p_opCode)
        , geometryFactory(geomFact)
        , hasResultArea(p_hasResultArea)
        , inputAreaIndex(inputGeom->getAreaIndex())
        , isAllowMixedResult(! OverlayNG::STRICT_MODE_DEFAULT)
        , isAllowCollapseLines(! OverlayNG::STRICT_MODE_DEFAULT)
        {}

    LineBuilder(const LineBuilder&) = delete;
    LineBuilder& operator=(const LineBuilder&) = delete;

    std::vector<std::unique_ptr<geom::LineString>> getLines();

    void setStrictMode(bool p_isStrictResultMode)
    {
        isAllowCollapseLines = ! p_isStrictResultMode;
        isAllowMixedResult = ! p_isStrictResultMode;
    }

};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

