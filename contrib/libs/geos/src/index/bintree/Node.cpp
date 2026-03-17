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
 **********************************************************************/

#include <cstddef>
#include <cassert>

#include <geos/index/bintree/Node.h>
#include <geos/index/bintree/Key.h>
#include <geos/index/bintree/Interval.h>

namespace geos {
namespace index { // geos.index
namespace bintree { // geos.index.bintree

Node*
Node::createNode(Interval* itemInterval)
{
    Key* key = new Key(itemInterval);
    //System.out.println("input: " + env + "  binaryEnv: " + key.getEnvelope());
    Node* node = new Node(new Interval(key->getInterval()), key->getLevel());
    delete key;
    return node;
}

Node*
Node::createExpanded(Node* node, Interval* addInterval)
{
    Interval* expandInt = new Interval(addInterval);
    if(node != nullptr) {
        expandInt->expandToInclude(node->interval);
    }
    Node* largerNode = createNode(expandInt);
    if(node != nullptr) {
        largerNode->insert(node);
    }
    delete expandInt;
    return largerNode;
}

Node::Node(Interval* newInterval, int newLevel)
{
    interval = newInterval;
    level = newLevel;
    centre = (interval->getMin() + interval->getMax()) / 2;
}

Node::~Node()
{
    delete interval;
}

Interval*
Node::getInterval()
{
    return interval;
}

bool
Node::isSearchMatch(Interval* itemInterval)
{
    return itemInterval->overlaps(interval);
}

/**
 * Returns the subnode containing the envelope.
 * Creates the node if
 * it does not already exist.
 */
Node*
Node::getNode(Interval* searchInterval)
{
    int subnodeIndex = getSubnodeIndex(searchInterval, centre);
    // if index is -1 searchEnv is not contained in a subnode
    if(subnodeIndex != -1) {
        // create the node if it does not exist
        Node* node = getSubnode(subnodeIndex);
        // recursively search the found/created node
        return node->getNode(searchInterval);
    }
    else {
        return this;
    }
}

/**
 * Returns the smallest <i>existing</i>
 * node containing the envelope.
 */
NodeBase*
Node::find(Interval* searchInterval)
{
    int subnodeIndex = getSubnodeIndex(searchInterval, centre);
    if(subnodeIndex == -1) {
        return this;
    }
    if(subnode[subnodeIndex] != nullptr) {
        // query lies in subnode, so search it
        Node* node = subnode[subnodeIndex];
        return node->find(searchInterval);
    }
    // no existing subnode, so return this one anyway
    return this;
}

void
Node::insert(Node* node)
{
    assert(interval == nullptr || interval->contains(node->interval));
    int index = getSubnodeIndex(node->interval, centre);
    assert(index >= 0);
    if(node->level == level - 1) {
        subnode[index] = node;
    }
    else {
        // the node is not a direct child, so make a new child node to contain it
        // and recursively insert the node
        Node* childNode = createSubnode(index);
        childNode->insert(node);
        subnode[index] = childNode;
    }
}

/**
 * get the subnode for the index.
 * If it doesn't exist, create it
 */
Node*
Node::getSubnode(int index)
{
    if(subnode[index] == nullptr) {
        subnode[index] = createSubnode(index);
    }
    return subnode[index];
}

Node*
Node::createSubnode(int index)
{
    // create a new subnode in the appropriate interval
    double min = 0.0;
    double max = 0.0;
    switch(index) {
    case 0:
        min = interval->getMin();
        max = centre;
        break;
    case 1:
        min = centre;
        max = interval->getMax();
        break;
    }
    Interval* subInt = new Interval(min, max);
    Node* node = new Node(subInt, level - 1);
    return node;
}

} // namespace geos.index.bintree
} // namespace geos.index
} // namespace geos
