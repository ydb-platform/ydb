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
 * Last port: operation/overlay/OverlayOp.java r567 (JTS-1.12+)
 *
 **********************************************************************/

#ifndef GEOS_OP_OVERLAY_OVERLAYOP_H
#define GEOS_OP_OVERLAY_OVERLAYOP_H

#include <geos/export.h>

#include <geos/algorithm/PointLocator.h> // for composition
#include <geos/geom/Dimension.h> // for Dimension::DimensionType
#include <geos/geom/Location.h>
#include <geos/geomgraph/EdgeList.h> // for composition
#include <geos/geomgraph/PlanarGraph.h> // for inline (GeometryGraph->PlanarGraph)
#include <geos/operation/GeometryGraphOperation.h> // for inheritance

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class Coordinate;
class Envelope;
class GeometryFactory;
class Polygon;
class LineString;
class Point;
}
namespace geomgraph {
class Label;
class Edge;
class Node;
}
namespace operation {
namespace overlay {
class ElevationMatrix;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace overlay { // geos::operation::overlay

/// \brief Computes the geometric overlay of two Geometry.
///
/// The overlay can be used to determine any
/// boolean combination of the geometries.
///
class GEOS_DLL OverlayOp: public GeometryGraphOperation {

public:

    ///  \brief The spatial functions supported by this class.
    ///
    /// These operations implement various boolean combinations of
    /// the resultants of the overlay.
    ///
    enum OpCode {
        /// The code for the Intersection overlay operation.
        opINTERSECTION = 1,
        /// The code for the Union overlay operation.
        opUNION = 2,
        /// The code for the Difference overlay operation.
        opDIFFERENCE = 3,
        /// The code for the Symmetric Difference overlay operation.
        opSYMDIFFERENCE = 4
    };

    /** \brief
     * Computes an overlay operation for the given geometry arguments.
     *
     * @param geom0 the first geometry argument
     * @param geom1 the second geometry argument
     * @param opCode the code for the desired overlay operation
     * @return the result of the overlay operation
     * @throws TopologyException if a robustness problem is encountered
     */
    static geom::Geometry* overlayOp(const geom::Geometry* geom0,
                                     const geom::Geometry* geom1,
                                     OpCode opCode);
    //throw(TopologyException *);

    /** \brief
     * Tests whether a point with a given topological [Label](@ref geomgraph::Label)
     * relative to two geometries is contained in
     * the result of overlaying the geometries using
     * a given overlay operation.
     *
     * The method handles arguments of [Location::NONE](@ref geom::Location::NONE) correctly
     *
     * @param label the topological label of the point
     * @param opCode the code for the overlay operation to test
     * @return true if the label locations correspond to the overlayOpCode
     */
    static bool isResultOfOp(const geomgraph::Label& label, OpCode opCode);

    /// \brief This method will handle arguments of Location.NULL correctly
    ///
    /// @return true if the locations correspond to the opCode
    ///
    static bool isResultOfOp(geom::Location loc0, geom::Location loc1, OpCode opCode);

    /// \brief Construct an OverlayOp with the given Geometry args.
    ///
    /// Ownership of passed args will remain to caller, and
    /// the OverlayOp won't change them in any way.
    ///
    OverlayOp(const geom::Geometry* g0, const geom::Geometry* g1);

    ~OverlayOp() override; // FIXME: virtual ?

    /** \brief
     * Gets the result of the overlay for a given overlay operation.
     *
     * Note: this method can be called once only.
     *
     * @param overlayOpCode the overlay operation to perform
     * @return the compute result geometry
     * @throws TopologyException if a robustness problem is encountered
     */
    geom::Geometry* getResultGeometry(OpCode overlayOpCode);
    // throw(TopologyException *);

    /** \brief
     * Gets the graph constructed to compute the overlay.
     *
     * @return the overlay graph
     */
    geomgraph::PlanarGraph&
    getGraph()
    {
        return graph;
    }

    /** \brief
     * This method is used to decide if a point node should be included
     * in the result or not.
     *
     * @return `true` if the coord point is covered by a result Line
     *         or Area geometry
     */
    bool isCoveredByLA(const geom::Coordinate& coord);

    /** \brief
     * This method is used to decide if an L edge should be included
     * in the result or not.
     *
     * @return `true` if the coord point is covered by a result Area geometry
     */
    bool isCoveredByA(const geom::Coordinate& coord);

    /*
     * @return true if the coord is located in the interior or boundary of
     * a geometry in the list.
     */

    /**
    * Creates an empty result geometry of the appropriate dimension,
    * based on the given overlay operation and the dimensions of the inputs.
    * The created geometry is always an atomic geometry,
    * not a collection.
    *
    * The empty result is constructed using the following rules:
    *
    * * #opINTERSECTION  result has the dimension of the lowest input dimension
    * * #opUNION - result has the dimension of the highest input dimension
    * * #opDIFFERENCE - result has the dimension of the left-hand input
    * * #opSYMDIFFERENCE - result has the dimension of the highest input dimension
    */
    static std::unique_ptr<geom::Geometry> createEmptyResult(
        OverlayOp::OpCode overlayOpCode, const geom::Geometry* a,
        const geom::Geometry* b, const geom::GeometryFactory* geomFact);

protected:

    /** \brief
     * Insert an edge from one of the noded input graphs.
     *
     * Checks edges that are inserted to see if an
     * identical edge already exists.
     * If so, the edge is not inserted, but its label is merged
     * with the existing edge.
     */
    void insertUniqueEdge(geomgraph::Edge* e);

private:

    algorithm::PointLocator ptLocator;

    const geom::GeometryFactory* geomFact;

    geom::Geometry* resultGeom;

    geomgraph::PlanarGraph graph;

    geomgraph::EdgeList edgeList;

    std::vector<geom::Polygon*>* resultPolyList;

    std::vector<geom::LineString*>* resultLineList;

    std::vector<geom::Point*>* resultPointList;

    void computeOverlay(OpCode opCode); // throw(TopologyException *);

    void insertUniqueEdges(std::vector<geomgraph::Edge*>* edges, const geom::Envelope* env = nullptr);

    /*
     * If either of the GeometryLocations for the existing label is
     * exactly opposite to the one in the labelToMerge,
     * this indicates a dimensional collapse has happened.
     * In this case, convert the label for that Geometry to a Line label
     */
    //Not needed
    //void checkDimensionalCollapse(geomgraph::Label labelToMerge, geomgraph::Label existingLabel);

    /** \brief
     * Update the labels for edges according to their depths.
     *
     * For each edge, the depths are first normalized.
     * Then, if the depths for the edge are equal,
     * this edge must have collapsed into a line edge.
     * If the depths are not equal, update the label
     * with the locations corresponding to the depths
     * (i.e. a depth of 0 corresponds to a Location of EXTERIOR,
     * a depth of 1 corresponds to INTERIOR)
     */
    void computeLabelsFromDepths();

    /** \brief
     * If edges which have undergone dimensional collapse are found,
     * replace them with a new edge which is a L edge
     */
    void replaceCollapsedEdges();

    /** \brief
     * Copy all nodes from an arg geometry into this graph.
     *
     * The node label in the arg geometry overrides any previously
     * computed label for that argIndex.
     * (E.g. a node may be an intersection node with
     * a previously computed label of BOUNDARY,
     * but in the original arg Geometry it is actually
     * in the interior due to the Boundary Determination Rule)
     */
    void copyPoints(int argIndex, const geom::Envelope* env = nullptr);

    /** \brief
     * Compute initial labelling for all DirectedEdges at each node.
     *
     * In this step, DirectedEdges will acquire a complete labelling
     * (i.e. one with labels for both Geometries)
     * only if they
     * are incident on a node which has edges for both Geometries
     */
    void computeLabelling(); // throw(TopologyException *);

    /**
     * For nodes which have edges from only one Geometry incident on them,
     * the previous step will have left their dirEdges with no
     * labelling for the other Geometry.
     * However, the sym dirEdge may have a labelling for the other
     * Geometry, so merge the two labels.
     */
    void mergeSymLabels();

    void updateNodeLabelling();

    /**
     * Incomplete nodes are nodes whose labels are incomplete.
     *
     * (e.g. the location for one Geometry is NULL).
     * These are either isolated nodes,
     * or nodes which have edges from only a single Geometry incident
     * on them.
     *
     * Isolated nodes are found because nodes in one graph which
     * don't intersect nodes in the other are not completely
     * labelled by the initial process of adding nodes to the nodeList.
     * To complete the labelling we need to check for nodes that
     * lie in the interior of edges, and in the interior of areas.
     *
     * When each node labelling is completed, the labelling of the
     * incident edges is updated, to complete their labelling as well.
     */
    void labelIncompleteNodes();

    /** \brief
     * Label an isolated node with its relationship to the target geometry.
     */
    void labelIncompleteNode(geomgraph::Node* n, int targetIndex);

    /** \brief
     * Find all edges whose label indicates that they are in the result
     * area(s), according to the operation being performed.
     *
     * Since we want polygon shells to be
     * oriented CW, choose dirEdges with the interior of the result
     * on the RHS.
     * Mark them as being in the result.
     * Interior Area edges are the result of dimensional collapses.
     * They do not form part of the result area boundary.
     */
    void findResultAreaEdges(OpCode opCode);

    /**
     * If both a dirEdge and its sym are marked as being in the result,
     * cancel them out.
     */
    void cancelDuplicateResultEdges();

    /**
     * @return true if the coord is located in the interior or boundary of
     * a geometry in the list.
     */
    bool isCovered(const geom::Coordinate& coord,
                   std::vector<geom::Geometry*>* geomList);

    /**
     * @return true if the coord is located in the interior or boundary of
     * a geometry in the list.
     */
    bool isCovered(const geom::Coordinate& coord,
                   std::vector<geom::Polygon*>* geomList);

    /**
     * @return true if the coord is located in the interior or boundary of
     * a geometry in the list.
     */
    bool isCovered(const geom::Coordinate& coord,
                   std::vector<geom::LineString*>* geomList);
    /**
    * For empty result, what is the correct geometry type to apply to
    * the empty?
    */
    static geom::Dimension::DimensionType resultDimension(OverlayOp::OpCode overlayOpCode,
            const geom::Geometry* g0, const geom::Geometry* g1);

    /**
     * Build a Geometry containing all Geometries in the given vectors.
     * Takes element's ownership, vector control is left to caller.
     */
    geom::Geometry* computeGeometry(
        std::vector<geom::Point*>* nResultPointList,
        std::vector<geom::LineString*>* nResultLineList,
        std::vector<geom::Polygon*>* nResultPolyList,
        OverlayOp::OpCode opCode);

    /// Caches for memory management
    std::vector<geomgraph::Edge*>dupEdges;

    /** \brief
     * Merge Z values of node with those of the segment or vertex in
     * the given Polygon it is on.
     */
    int mergeZ(geomgraph::Node* n, const geom::Polygon* poly) const;

    /**
     * Merge Z values of node with those of the segment or vertex in
     * the given LineString it is on.
     * @returns 1 if an intersection is found, 0 otherwise.
     */
    int mergeZ(geomgraph::Node* n, const geom::LineString* line) const;

    /**
     * Average Z of input geometries
     */
    double avgz[2];
    bool avgzcomputed[2];

    double getAverageZ(int targetIndex);
    static double getAverageZ(const geom::Polygon* poly);

    ElevationMatrix* elevationMatrix;

    /// Throw TopologyException if an obviously wrong result has
    /// been computed.
    void checkObviouslyWrongResult(OpCode opCode);

};

} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_OVERLAY_OVERLAYOP_H
