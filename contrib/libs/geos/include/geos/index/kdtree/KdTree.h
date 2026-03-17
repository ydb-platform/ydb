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
#include <geos/geom/Envelope.h>
#include <geos/index/kdtree/KdNodeVisitor.h>
#include <geos/index/kdtree/KdNode.h>

#include <memory>
#include <vector>
#include <string>
#include <deque>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif


namespace geos {
namespace index { // geos::index
namespace kdtree { // geos::index::kdtree

/**
 * An implementation of a 2-D KD-Tree. KD-trees provide fast range searching on
 * point data.
 * <p>
 * This implementation supports detecting and snapping points which are closer
 * than a given distance tolerance.
 * If the same point (up to tolerance) is inserted
 * more than once, it is snapped to the existing node.
 * In other words, if a point is inserted which lies within the tolerance of a node already in the index,
 * it is snapped to that node.
 * When a point is snapped to a node then a new node is not created but the count of the existing node
 * is incremented.
 * If more than one node in the tree is within tolerance of an inserted point,
 * the closest and then lowest node is snapped to.
 *
 * @author David Skea
 * @author Martin Davis
 */
class GEOS_DLL KdTree {

private:

    std::deque<KdNode> nodeQue;
    KdNode *root;
    size_t numberOfNodes;
    double tolerance;

    KdNode* findBestMatchNode(const geom::Coordinate& p);
    KdNode* insertExact(const geom::Coordinate& p, void* data);

    void queryNode(KdNode* currentNode, const geom::Envelope& queryEnv, bool odd, KdNodeVisitor& visitor);
    KdNode* queryNodePoint(KdNode* currentNode, const geom::Coordinate& queryPt, bool odd);

    /**
    * Create a node on a locally managed deque to allow easy
    * disposal and hopefully faster allocation as well.
    */
    KdNode* createNode(const geom::Coordinate& p, void* data);


    /**
    * BestMatchVisitor used to query the tree for a match
    * within tolerance.
    */
    class BestMatchVisitor : public KdNodeVisitor {
    public:
        BestMatchVisitor(const geom::Coordinate& p_p, double p_tolerance);
        geom::Envelope queryEnvelope();
        KdNode* getNode();
        void visit(KdNode* node) override;

    private:
        // Members
        double tolerance;
        KdNode* matchNode;
        double matchDist;
        const geom::Coordinate& p;
        // Declare type as noncopyable
        BestMatchVisitor(const BestMatchVisitor& other);
        BestMatchVisitor& operator=(const BestMatchVisitor& rhs);
    };

    /**
    * AccumulatingVisitor used to query the tree and get list
    * of matching nodes.
    */
    class AccumulatingVisitor : public KdNodeVisitor {
    public:
        AccumulatingVisitor(std::vector<KdNode*>& p_nodeList) :
            nodeList(p_nodeList) {};
        void visit(KdNode* node) override { nodeList.push_back(node); }

    private:
        // Members
        std::vector<KdNode*>& nodeList;
        // Declare type as noncopyable
        AccumulatingVisitor(const AccumulatingVisitor& other);
        AccumulatingVisitor& operator=(const AccumulatingVisitor& rhs);
    };



public:

    /**
    * Converts a collection of {@link KdNode}s to an vector of {@link geom::Coordinate}s.
    *
    * @param kdnodes a collection of nodes
    * @return an vector of the coordinates represented by the nodes
    */
    static std::unique_ptr<std::vector<geom::Coordinate>> toCoordinates(std::vector<KdNode*>& kdnodes);

    /**
    * Converts a collection of {@link KdNode}s
    * to an vector of {@link geom::Coordinate}s,
    * specifying whether repeated nodes should be represented
    * by multiple coordinates.
    *
    * @param kdnodes a collection of nodes
    * @param includeRepeated true if repeated nodes should
    *   be included multiple times
    * @return an vector of the coordinates represented by the nodes
    */
    static std::unique_ptr<std::vector<geom::Coordinate>> toCoordinates(std::vector<KdNode*>& kdnodes, bool includeRepeated);

    KdTree() :
        root(nullptr),
        numberOfNodes(0),
        tolerance(0.0)
        {};

    KdTree(double p_tolerance) :
        root(nullptr),
        numberOfNodes(0),
        tolerance(p_tolerance)
        {};

    bool isEmpty() { return root == nullptr; }

    /**
    * Inserts a new point in the kd-tree.
    */
    KdNode* insert(const geom::Coordinate& p);
    KdNode* insert(const geom::Coordinate& p, void* data);

    /**
    * Performs a range search of the points in the index and visits all nodes found.
    */
    void query(const geom::Envelope& queryEnv, KdNodeVisitor& visitor);

    /**
    * Performs a range search of the points in the index.
    */
    std::unique_ptr<std::vector<KdNode*>> query(const geom::Envelope& queryEnv);

    /**
    * Performs a range search of the points in the index.
    */
    void query(const geom::Envelope& queryEnv, std::vector<KdNode*>& result);

    /**
    * Searches for a given point in the index and returns its node if found.
    */
    KdNode* query(const geom::Coordinate& queryPt);

};

} // namespace geos::index::kdtree
} // namespace geos::index
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

