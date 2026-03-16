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

#include <geos/index/kdtree/KdTree.h>
#include <geos/geom/Envelope.h>

#include <vector>
#include <algorithm>
#include <stack>

using namespace geos::geom;

namespace geos {
namespace index { // geos.index
namespace kdtree { // geos.index.kdtree


/*public static*/
std::unique_ptr<std::vector<Coordinate>>
KdTree::toCoordinates(std::vector<KdNode*>& kdnodes)
{
    return toCoordinates(kdnodes, false);
}

/*public static*/
std::unique_ptr<std::vector<Coordinate>>
KdTree::toCoordinates(std::vector<KdNode*>& kdnodes, bool includeRepeated)
{
    std::unique_ptr<std::vector<Coordinate>> coord(new std::vector<Coordinate>);
    for (auto node: kdnodes) {
        size_t count = includeRepeated ? node->getCount() : 1;
        for (size_t i = 0; i < count; i++) {
            coord->emplace_back(node->getCoordinate());
        }
    }
    if (!includeRepeated) {
        // Remove duplicate Coordinates from coordList
        coord->erase(std::unique(coord->begin(), coord->end()), coord->end());
    }
    return coord;
}

/*private*/
KdNode*
KdTree::createNode(const Coordinate& p, void* data)
{
    auto it = nodeQue.emplace(nodeQue.end(), p, data);
    return &(*it);
}

/*public*/
KdNode*
KdTree::insert(const Coordinate& p)
{
    return insert(p, nullptr);
}

/*public*/
KdNode*
KdTree::insert(const Coordinate& p, void* data)
{
    if (root == nullptr) {
        root = createNode(p, data);
        return root;
    }

    /**
    * Check if the point is already in the tree, up to tolerance.
    * If tolerance is zero, this phase of the insertion can be skipped.
    */
    if (tolerance > 0) {
        KdNode* matchNode = findBestMatchNode(p);
        if (matchNode != nullptr) {
            // point already in index - increment counter
            matchNode->increment();
            return matchNode;
        }
    }

    return insertExact(p, data);
}

/*private*/
KdNode*
KdTree::findBestMatchNode(const Coordinate& p) {
    BestMatchVisitor visitor(p, tolerance);
    query(visitor.queryEnvelope(), visitor);
    return visitor.getNode();
}

KdNode*
KdTree::insertExact(const geom::Coordinate& p, void* data)
{
    KdNode* currentNode = root;
    KdNode* leafNode = root;
    bool isOddLevel = true;
    bool isLessThan = true;

    /**
    * Traverse the tree, first cutting the plane left-right (by X ordinate)
    * then top-bottom (by Y ordinate)
    */

    while (currentNode != nullptr) {
        // test if point is already a node (not strictly necessary)
        bool isInTolerance = p.distance(currentNode->getCoordinate()) <= tolerance;

        // check if point is already in tree (up to tolerance) and if so simply
        // return existing node
        if (isInTolerance) {
            currentNode->increment();
            return currentNode;
        }

        if (isOddLevel) {
            isLessThan = p.x < currentNode->getX();
        } else {
            isLessThan = p.y < currentNode->getY();
        }
        leafNode = currentNode;
        if (isLessThan) {
            currentNode = currentNode->getLeft();
        } else {
            currentNode = currentNode->getRight();
        }

        isOddLevel = !isOddLevel;
    }

    // no node found, add new leaf node to tree
    numberOfNodes++;
    KdNode* node = createNode(p, data);
    if (isLessThan) {
        leafNode->setLeft(node);
    } else {
        leafNode->setRight(node);
    }
    return node;
}

/*private*/
void
KdTree::queryNode(KdNode* currentNode, const geom::Envelope& queryEnv, bool odd, KdNodeVisitor& visitor)
{
    // Non recursive formulation of in-order traversal from
    // http://web.cs.wpi.edu/~cs2005/common/iterative.inorder
    // Otherwise we may blow up the stack
    // See https://github.com/qgis/QGIS/issues/45226
    typedef std::pair<KdNode*, bool> Pair;
    std::stack<Pair> activeNodes;
    while(true)
    {
        if( currentNode != nullptr )
        {
            double min;
            double discriminant;

            if (odd) {
                min = queryEnv.getMinX();
                discriminant = currentNode->getX();
            } else {
                min = queryEnv.getMinY();
                discriminant = currentNode->getY();
            }
            bool searchLeft = min < discriminant;

            activeNodes.emplace(Pair(currentNode, odd));

            // search is computed via in-order traversal
            KdNode* leftNode = nullptr;
            if (searchLeft ) {
                leftNode = currentNode->getLeft();
            }
            if( leftNode ) {
                currentNode = leftNode;
                odd = !odd;
            } else {
                currentNode = nullptr;
            }
        }
        else if( !activeNodes.empty() )
        {
            currentNode = activeNodes.top().first;
            odd = activeNodes.top().second;
            activeNodes.pop();

            if (queryEnv.contains(currentNode->getCoordinate())) {
                visitor.visit(currentNode);
            }

            double max;
            double discriminant;

            if (odd) {
                max = queryEnv.getMaxX();
                discriminant = currentNode->getX();
            } else {
                max = queryEnv.getMaxY();
                discriminant = currentNode->getY();
            }
            bool searchRight = discriminant <= max;

            if (searchRight) {
                currentNode = currentNode->getRight();
                if( currentNode )
                    odd = !odd;
            } else {
                currentNode = nullptr;
            }
        }
        else
        {
            break;
        }
    }
}

/*private*/
KdNode*
KdTree::queryNodePoint(KdNode* currentNode, const geom::Coordinate& queryPt, bool odd)
{
    while (currentNode != nullptr)
    {
        if (currentNode->getCoordinate().equals2D(queryPt))
            return currentNode;

        double ord;
        double discriminant;
        if (odd) {
            ord = queryPt.x;
            discriminant = currentNode->getX();
        }
        else {
            ord = queryPt.y;
            discriminant = currentNode->getY();
        }

        bool searchLeft = (ord < discriminant);
        odd = !odd;
        if (searchLeft) {
            currentNode = currentNode->getLeft();
        }
        else {
            currentNode = currentNode->getRight();
        }
    }
    return nullptr;
}


/*public*/
void
KdTree::query(const geom::Envelope& queryEnv, KdNodeVisitor& visitor)
{
    queryNode(root, queryEnv, true, visitor);
}

/*public*/
std::unique_ptr<std::vector<KdNode*>>
KdTree::query(const geom::Envelope& queryEnv)
{
    std::unique_ptr<std::vector<KdNode*>> result(new std::vector<KdNode*>);
    query(queryEnv, *result);
    return result;
}

/*public*/
void
KdTree::query(const geom::Envelope& queryEnv, std::vector<KdNode*>& result)
{
    AccumulatingVisitor visitor(result);
    queryNode(root, queryEnv, true, visitor);
}

/*public*/
KdNode*
KdTree::query(const geom::Coordinate& queryPt) {
    return queryNodePoint(root, queryPt, true);
}


/**********************************************************************/

/*private*/
KdTree::BestMatchVisitor::BestMatchVisitor(const geom::Coordinate& p_p, double p_tolerance)
    : tolerance(p_tolerance)
    , matchNode(nullptr)
    , matchDist(0.0)
    , p(p_p) {}

/*private*/
Envelope
KdTree::BestMatchVisitor::queryEnvelope() {
    geom::Envelope queryEnv(p);
    queryEnv.expandBy(tolerance);
    return queryEnv;
}

/*private*/
KdNode*
KdTree::BestMatchVisitor::getNode() {
    return matchNode;
}

/*private*/
void
KdTree::BestMatchVisitor::visit(KdNode* node) {
    double dist = p.distance(node->getCoordinate());
    if (! (dist <= tolerance)) return;
    bool update = false;
    if (matchNode == nullptr || dist < matchDist
        // if distances are the same, record the lesser coordinate
        || (matchNode != nullptr && dist == matchDist
        && node->getCoordinate().compareTo(matchNode->getCoordinate()) < 1)) {
        update = true;
    }

    if (update) {
        matchNode = node;
        matchDist = dist;
    }
}




} // namespace geos.index.kdtree
} // namespace geos.index
} // namespace geos
