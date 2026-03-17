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
 **********************************************************************
 *
 * Last port: index/quadtree/Node.java rev 1.8 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/index/quadtree/Node.h>
#include <geos/index/quadtree/Key.h>
#include <geos/geom/Envelope.h>

#include <string>
#include <sstream>
#include <cassert>
#include <memory>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#if GEOS_DEBUG
#include <iostream>
#endif

using namespace std;
using namespace geos::geom;

namespace geos {
namespace index { // geos.index
namespace quadtree { // geos.index.quadtree

/* public static */
std::unique_ptr<Node>
Node::createNode(const Envelope& env)
{
    Key key(env);
    std::unique_ptr<Envelope> envelope(new Envelope(key.getEnvelope()));
    std::unique_ptr<Node> node(
        new Node(std::move(envelope), key.getLevel())
    );
    return node;
}

/* static public */
std::unique_ptr<Node>
Node::createExpanded(std::unique_ptr<Node> node, const Envelope& addEnv)
{
    Envelope expandEnv(addEnv);
    if(node.get()) {  // should this be asserted ?
        expandEnv.expandToInclude(node->getEnvelope());
    }

#if GEOS_DEBUG
    cerr << "Node::createExpanded computed " << expandEnv.toString() << endl;
#endif

    std::unique_ptr<Node> largerNode = createNode(expandEnv);
    if(node.get()) {  // should this be asserted ?
        largerNode->insertNode(std::move(node));
    }

    return largerNode;
}

/*public*/
Node*
Node::getNode(const Envelope* searchEnv)
{
    int subnodeIndex = getSubnodeIndex(searchEnv, centre);
    // if subquadIndex is -1 searchEnv is not contained in a subquad
    if(subnodeIndex != -1) {
        // create the quad if it does not exist
        Node* node = getSubnode(subnodeIndex);
        // recursively search the found/created quad
        return node->getNode(searchEnv);
    }
    else {
        return this;
    }
}

/*public*/
NodeBase*
Node::find(const Envelope* searchEnv)
{
    int subnodeIndex = getSubnodeIndex(searchEnv, centre);
    if(subnodeIndex == -1) {
        return this;
    }
    if(subnodes[subnodeIndex] != nullptr) {
        // query lies in subquad, so search it
        Node* node = subnodes[subnodeIndex];
        return node->find(searchEnv);
    }
    // no existing subquad, so return this one anyway
    return this;
}

void
Node::insertNode(std::unique_ptr<Node> node)
{
    assert(env->contains(node->getEnvelope()));

    int index = getSubnodeIndex(node->getEnvelope(), centre);
    assert(index >= 0);

    if(node->level == level - 1) {
        // We take ownership of node
        delete subnodes[index];
        subnodes[index] = node.release();

        //System.out.println("inserted");
    }
    else {
        // the quad is not a direct child, so make a new child
        // quad to contain it and recursively insert the quad
        std::unique_ptr<Node> childNode(createSubnode(index));

        // childNode takes ownership of node
        childNode->insertNode(std::move(node));

        // We take ownership of childNode
        delete subnodes[index];
        subnodes[index] = childNode.release();
    }
}

Node*
Node::getSubnode(int index)
{
    assert(index >= 0 && index < 4);
    if(subnodes[index] == nullptr) {
        subnodes[index] = createSubnode(index).release();
    }
    return subnodes[index];
}

std::unique_ptr<Node>
Node::createSubnode(int index)
{
    // create a new subquad in the appropriate quadrant
    double minx = 0.0;
    double maxx = 0.0;
    double miny = 0.0;
    double maxy = 0.0;

    switch(index) {
    case 0:
        minx = env->getMinX();
        maxx = centre.x;
        miny = env->getMinY();
        maxy = centre.y;
        break;
    case 1:
        minx = centre.x;
        maxx = env->getMaxX();
        miny = env->getMinY();
        maxy = centre.y;
        break;
    case 2:
        minx = env->getMinX();
        maxx = centre.x;
        miny = centre.y;
        maxy = env->getMaxY();
        break;
    case 3:
        minx = centre.x;
        maxx = env->getMaxX();
        miny = centre.y;
        maxy = env->getMaxY();
        break;
    }
    std::unique_ptr<Envelope> sqEnv(new Envelope(minx, maxx, miny, maxy));
    std::unique_ptr<Node> node(new Node(std::move(sqEnv), level - 1));
    return node;
}

string
Node::toString() const
{
    ostringstream os;
    os << "L" << level << " " << env->toString() << " Ctr[" << centre.toString() << "]";
    os << " " + NodeBase::toString();
    return os.str();
}


} // namespace geos.index.quadtree
} // namespace geos.index
} // namespace geos
