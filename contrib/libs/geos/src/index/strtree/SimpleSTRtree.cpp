/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/index/strtree/SimpleSTRtree.h>
#include <geos/index/strtree/SimpleSTRdistance.h>
#include <geos/index/ItemVisitor.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Geometry.h>
#include <geos/util.h>

#include <vector>
#include <cassert>
#include <cmath>
#include <algorithm> // std::sort
#include <iostream> // for debugging
#include <limits>
#include <geos/util/GEOSException.h>

using namespace geos::geom;

namespace geos {
namespace index { // geos.index
namespace strtree { // geos.index.strtree

/* private */
SimpleSTRnode*
SimpleSTRtree::createNode(int newLevel, const geom::Envelope* itemEnv, void* item)
{
    nodesQue.emplace_back(newLevel, itemEnv, item, nodeCapacity);
    SimpleSTRnode& node = nodesQue.back();
    return &node;
}

/* private */
SimpleSTRnode*
SimpleSTRtree::createNode(int newLevel)
{
    return createNode(newLevel, nullptr, nullptr);
}

/* public */
void
SimpleSTRtree::insert(geom::Geometry* geom)
{
    insert(geom->getEnvelopeInternal(), static_cast<void*>(geom));
}

/* public */
void
SimpleSTRtree::insert(const geom::Envelope* itemEnv, void* item)
{
    if (itemEnv->isNull())
        return;
    SimpleSTRnode *node = createNode(0, itemEnv, item);
    nodes.push_back(node);
}

/* private static */
void
SimpleSTRtree::sortNodesY(std::vector<SimpleSTRnode*>& nodeList)
{
    struct {
        bool operator()(SimpleSTRnode* a, SimpleSTRnode* b) const
        {
            const geom::Envelope& ea = a->getEnvelope();
            const geom::Envelope& eb = b->getEnvelope();
            double ya = (ea.getMinY() + ea.getMaxY()) / 2.0;
            double yb = (eb.getMinY() + eb.getMaxY()) / 2.0;
            return ya < yb;
        }
    } nodeSortByY;

    std::sort(nodeList.begin(), nodeList.end(), nodeSortByY);
}

/* private static */
void
SimpleSTRtree::sortNodesX(std::vector<SimpleSTRnode*>& nodeList)
{
    struct {
        bool operator()(SimpleSTRnode* a, SimpleSTRnode* b) const
        {
            const geom::Envelope& ea = a->getEnvelope();
            const geom::Envelope& eb = b->getEnvelope();
            double xa = (ea.getMinX() + ea.getMaxX()) / 2.0;
            double xb = (eb.getMinX() + eb.getMaxX()) / 2.0;
            return xa < xb;
        }
    } nodeSortByX;

    std::sort(nodeList.begin(), nodeList.end(), nodeSortByX);
}

/* private */
std::vector<SimpleSTRnode*>
SimpleSTRtree::createParentNodes(
    std::vector<SimpleSTRnode*>& childNodes,
    int newLevel)
{
    assert(!childNodes.empty());

    std::size_t minLeafCount = (std::size_t)std::ceil((double)(childNodes.size()) / (double)nodeCapacity);
    std::size_t sliceCount = (std::size_t)std::ceil(std::sqrt((double)minLeafCount));
    std::size_t sliceCapacity = (std::size_t)std::ceil((double)(childNodes.size()) / (double)sliceCount);

    sortNodesX(childNodes);

    std::size_t i = 0;
    std::size_t nChildren = childNodes.size();
    std::vector<SimpleSTRnode*> parentNodes;
    std::vector<SimpleSTRnode*> verticalSlice(sliceCapacity);
    for (std::size_t j = 0; j < sliceCount; j++) {
        verticalSlice.clear();
        std::size_t nodesAddedToSlice = 0;
        while(i < nChildren && nodesAddedToSlice < sliceCapacity) {
            verticalSlice.push_back(childNodes[i++]);
            ++nodesAddedToSlice;
        }
        addParentNodesFromVerticalSlice(verticalSlice, newLevel, parentNodes);
    }
    return parentNodes;
}

/* private */
void
SimpleSTRtree::addParentNodesFromVerticalSlice(
    std::vector<SimpleSTRnode*>& verticalSlice,
    int newLevel,
    std::vector<SimpleSTRnode*>& parentNodes)
{
    sortNodesY(verticalSlice);

    SimpleSTRnode* parent = nullptr;
    for (auto* node: verticalSlice) {
        if (!parent) {
            parent = createNode(newLevel);
        }
        parent->addChildNode(node);
        if (parent->size() == nodeCapacity) {
            parentNodes.push_back(parent);
            parent = nullptr;
        }
    }
    if (parent)
        parentNodes.push_back(parent);

    return;
}

/* private */
std::vector<SimpleSTRnode*>
SimpleSTRtree::createHigherLevels(
    std::vector<SimpleSTRnode*>& nodesOfALevel, int level)
{
    int nextLevel = level+1;
    std::vector<SimpleSTRnode*> parentNodes = createParentNodes(nodesOfALevel, nextLevel);
    if (parentNodes.size() == 1) {
        return parentNodes;
    }
    return createHigherLevels(parentNodes, nextLevel);
}

/* private */
void
SimpleSTRtree::build()
{
    if (built) return;

    if (nodes.empty()) {
        root = nullptr;
    }
    else {
        std::vector<SimpleSTRnode*> nodeTree = createHigherLevels(nodes, 0);
        assert(nodeTree.size()==1);
        root = nodeTree[0];
    }
    built = true;
}

void
SimpleSTRtree::iterate(ItemVisitor& visitor)
{
    for(auto* leafNode: nodes) {
        visitor.visitItem(leafNode->getItem());
    }
}

/* public */
void
SimpleSTRtree::query(const geom::Envelope* searchEnv, ItemVisitor& visitor)
{
    build();

    if(nodes.empty() || !root) {
        return;
    }

    if(root->getEnvelope().intersects(searchEnv)) {
        query(searchEnv, root, visitor);
    }
}

/* private */
void
SimpleSTRtree::query(const geom::Envelope* searchEnv,
    const SimpleSTRnode* node, ItemVisitor& visitor)
{
    for(auto* childNode: node->getChildNodes()) {

        if(!childNode->getEnvelope().intersects(searchEnv)) {
            continue;
        }

        if (childNode->isLeaf()) {
            visitor.visitItem(childNode->getItem());
        } else {
            query(searchEnv, childNode, visitor);
        }
    }
}

/* public */
void
SimpleSTRtree::query(const geom::Envelope* searchEnv, std::vector<void*>& matches)
{
    build();

    if(nodes.empty() || !root) {
        return;
    }

    if(root->getEnvelope().intersects(searchEnv)) {
        query(searchEnv, root, matches);
    }
}

/* private */
void
SimpleSTRtree::query(const geom::Envelope* searchEnv,
    const SimpleSTRnode* node, std::vector<void*>& matches)
{
    assert(node);

    for(auto* childNode: node->getChildNodes()) {

        if(!childNode->getEnvelope().intersects(searchEnv)) {
            continue;
        }

        if (childNode->isLeaf()) {
            matches.push_back(childNode->getItem());
        } else {
            query(searchEnv, childNode, matches);
        }
    }
}

/* public */
bool
SimpleSTRtree::remove(const geom::Envelope* searchBounds, void* item)
{
    build();
    if(root->getEnvelope().intersects(searchBounds)) {
        return remove(searchBounds, root, item);
    }
    return false;
}

/* private */
bool
SimpleSTRtree::remove(const geom::Envelope* searchBounds,
    SimpleSTRnode* node, void* item)
{
    bool found = node->removeItem(item);
    if (found)
        return true;

    SimpleSTRnode* childToPrune = nullptr;
    auto childNodes = node->getChildNodes();
    for (auto* child: childNodes) {
        if (! searchBounds->intersects(child->getEnvelope())) {
            continue;
        }
        if (!child->isLeaf()) {
            found = remove(searchBounds, child, item);
            if (found) {
                childToPrune = child;
                break;
            }
        }
    }
    if (childToPrune != nullptr) {
        // Only remove empty child nodes
        if (childToPrune->getChildNodes().empty()) {
            node->removeChild(childToPrune);
        }
    }
    return found;
}

/*public static*/
std::ostream&
operator<<(std::ostream& os, const SimpleSTRtree& tree)
{

    os << "nodeCapacity: " << tree.getNodeCapacity() << std::endl;
    os << "nodes.size(): " << tree.getNumLeafNodes() << std::endl;
    os << "built: " << tree.getBuilt() << std::endl;

    if (tree.root) {
        os << "tree: " << std::endl;
        tree.root->toString(os, 1);
    }
    else {
        os << "tree: empty" << std::endl;
    }
    return os;
}

/*********************************************************************************/

/*public*/
std::pair<const void*, const void*>
SimpleSTRtree::nearestNeighbour(ItemDistance* itemDist)
{
    if (!this->getRoot()) {
        return std::pair<const void*, const void*>(nullptr, nullptr);
    }
    SimpleSTRdistance strDist(this->getRoot(), this->getRoot(), itemDist);
    return strDist.nearestNeighbour();
}


/*public*/
const void*
SimpleSTRtree::nearestNeighbour(const geom::Envelope* p_env, const void* p_item, ItemDistance* itemDist)
{
    if (!this->getRoot()) {
        return nullptr;
    }
    std::unique_ptr<SimpleSTRnode> ssn(new SimpleSTRnode(0, p_env, (void*)p_item));
    SimpleSTRdistance strDist(getRoot(), ssn.get(), itemDist);
    std::pair<const void*, const void*> result = strDist.nearestNeighbour();
    return result.first;
}


/*public*/
std::pair<const void*, const void*>
SimpleSTRtree::nearestNeighbour(SimpleSTRtree& tree, ItemDistance* itemDist)
{
    if (!(this->getRoot() && tree.getRoot())) {
        return std::pair<const void*, const void*>(nullptr, nullptr);
    }
    SimpleSTRdistance strDist(this->getRoot(), tree.getRoot(), itemDist);
    return strDist.nearestNeighbour();
}

/*public*/
bool
SimpleSTRtree::isWithinDistance(SimpleSTRtree& tree, ItemDistance* itemDist, double maxDistance)
{
    if (!(this->getRoot() && tree.getRoot())) {
        return false;
    }
    SimpleSTRdistance strDist(this->getRoot(), tree.getRoot(), itemDist);
    return strDist.isWithinDistance(maxDistance);
}




} // namespace geos.index.strtree
} // namespace geos.index
} // namespace geos
