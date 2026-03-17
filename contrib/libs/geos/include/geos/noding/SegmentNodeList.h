/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006      Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/SegmentNodeList.java rev. 1.8 (JTS-1.10)
 *
 **********************************************************************/

#pragma once

#include <geos/export.h>

#include <geos/inline.h>

#include <cassert>
#include <iostream>
#include <vector>
#include <set>
#include <memory>

#include <geos/noding/SegmentNode.h> // for composition

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
}
namespace noding {
class SegmentString;
class NodedSegmentString;
}
}

namespace geos {
namespace noding { // geos::noding

/** \brief
 * A list of the SegmentNode present along a
 * NodedSegmentString.
 */
class GEOS_DLL SegmentNodeList {
private:
    std::set<SegmentNode*, SegmentNodeLT> nodeMap;
    std::deque<SegmentNode> nodeQue;

    // the parent edge
    const NodedSegmentString& edge;

    /**
     * Checks the correctness of the set of split edges corresponding
     * to this edge
     *
     * @param splitEdges the split edges for this edge (in order)
     */
    void checkSplitEdgesCorrectness(const std::vector<SegmentString*>& splitEdges) const;

    /**
     * Create a new "split edge" with the section of points between
     * (and including) the two intersections.
     * The label for the new edge is the same as the label for the
     * parent edge.
     *
     * ownership of return value is transferred
     */
    std::unique_ptr<SegmentString> createSplitEdge(const SegmentNode* ei0, const SegmentNode* ei1) const;

    /**
    * Extracts the points for a split edge running between two nodes.
    * The extracted points should contain no duplicate points.
    * There should always be at least two points extracted
    * (which will be the given nodes).
    *
    * @param ei0 the start node of the split edge
    * @param ei1 the end node of the split edge
    * @return the points for the split edge
    */
    void createSplitEdgePts(const SegmentNode* ei0, const SegmentNode* ei1, std::vector<geom::Coordinate>& pts) const;

    /**
     * Adds nodes for any collapsed edge pairs.
     * Collapsed edge pairs can be caused by inserted nodes, or they
     * can be pre-existing in the edge vertex list.
     * In order to provide the correct fully noded semantics,
     * the vertex at the base of a collapsed pair must also be added
     * as a node.
     */
    void addCollapsedNodes();

    /**
     * Adds nodes for any collapsed edge pairs
     * which are pre-existing in the vertex list.
     */
    void findCollapsesFromExistingVertices(
        std::vector<std::size_t>& collapsedVertexIndexes) const;

    /**
     * Adds nodes for any collapsed edge pairs caused by inserted nodes
     * Collapsed edge pairs occur when the same coordinate is inserted
     * as a node both before and after an existing edge vertex.
     * To provide the correct fully noded semantics,
     * the vertex must be added as a node as well.
     */
    void findCollapsesFromInsertedNodes(
        std::vector<std::size_t>& collapsedVertexIndexes) const;

    bool findCollapseIndex(const SegmentNode& ei0, const SegmentNode& ei1,
                           size_t& collapsedVertexIndex) const;

    void addEdgeCoordinates(const SegmentNode* ei0, const SegmentNode* ei1, std::vector<geom::Coordinate>& coordList) const;

    // Declare type as noncopyable
    SegmentNodeList(const SegmentNodeList& other) = delete;
    SegmentNodeList& operator=(const SegmentNodeList& rhs) = delete;

public:

    friend std::ostream& operator<< (std::ostream& os, const SegmentNodeList& l);

    typedef std::set<SegmentNode*, SegmentNodeLT> container;
    typedef container::iterator iterator;
    typedef container::const_iterator const_iterator;

    SegmentNodeList(const NodedSegmentString* newEdge): edge(*newEdge) {}

    SegmentNodeList(const NodedSegmentString& newEdge): edge(newEdge) {}

    ~SegmentNodeList();

    const NodedSegmentString&
    getEdge() const
    {
        return edge;
    }

    /**
     * Adds an intersection into the list, if it isn't already there.
     * The input segmentIndex is expected to be normalized.
     *
     * @return the SegmentIntersection found or added. It will be
     *	   destroyed at SegmentNodeList destruction time.
     *
     * @param intPt the intersection Coordinate, will be copied
     * @param segmentIndex
     */
    SegmentNode* add(const geom::Coordinate& intPt, std::size_t segmentIndex);

    SegmentNode*
    add(const geom::Coordinate* intPt, std::size_t segmentIndex)
    {
        return add(*intPt, segmentIndex);
    }

    /*
     * returns the set of SegmentNodes
     */
    //replaces iterator()
    // TODO: obsolete this function
    std::set<SegmentNode*, SegmentNodeLT>*
    getNodes()
    {
        return &nodeMap;
    }

    /// Return the number of nodes in this list
    size_t
    size() const
    {
        return nodeMap.size();
    }

    container::iterator
    begin()
    {
        return nodeMap.begin();
    }
    container::const_iterator
    begin() const
    {
        return nodeMap.begin();
    }
    container::iterator
    end()
    {
        return nodeMap.end();
    }
    container::const_iterator
    end() const
    {
        return nodeMap.end();
    }

    /**
     * Adds entries for the first and last points of the edge to the list
     */
    void addEndpoints();

    /**
     * Creates new edges for all the edges that the intersections in this
     * list split the parent edge into.
     * Adds the edges to the input list (this is so a single list
     * can be used to accumulate all split edges for a Geometry).
     */
    void addSplitEdges(std::vector<SegmentString*>& edgeList);

    void
    addSplitEdges(std::vector<SegmentString*>* edgeList)
    {
        assert(edgeList);
        addSplitEdges(*edgeList);
    }

    /**
    * Gets the list of coordinates for the fully noded segment string,
    * including all original segment string vertices and vertices
    * introduced by nodes in this list.
    * Repeated coordinates are collapsed.
    *
    * @return an array of Coordinates
    *
    */
    std::unique_ptr<std::vector<geom::Coordinate>> getSplitCoordinates();


};

std::ostream& operator<< (std::ostream& os, const SegmentNodeList& l);

} // namespace geos::noding
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

