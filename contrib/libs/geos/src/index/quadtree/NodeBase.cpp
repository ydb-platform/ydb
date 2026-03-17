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
 * Last port: index/quadtree/NodeBase.java rev 1.9 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/index/quadtree/NodeBase.h>
#include <geos/index/quadtree/Node.h>
#include <geos/index/ItemVisitor.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Coordinate.h>
#include <geos/util.h>

#include <sstream>
#include <vector>
#include <algorithm>

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

int
NodeBase::getSubnodeIndex(const Envelope* env, const Coordinate& centre)
{
    int subnodeIndex = -1;
    if(env->getMinX() >= centre.x) {
        if(env->getMinY() >= centre.y) {
            subnodeIndex = 3;
        }
        if(env->getMaxY() <= centre.y) {
            subnodeIndex = 1;
        }
    }
    if(env->getMaxX() <= centre.x) {
        if(env->getMinY() >= centre.y) {
            subnodeIndex = 2;
        }
        if(env->getMaxY() <= centre.y) {
            subnodeIndex = 0;
        }
    }
#if GEOS_DEBUG
    cerr << "getSubNodeIndex(" << env->toString() << ", " << centre.toString() << ") returning " << subnodeIndex << endl;
#endif
    return subnodeIndex;
}

NodeBase::NodeBase()
{
    subnodes[0] = nullptr;
    subnodes[1] = nullptr;
    subnodes[2] = nullptr;
    subnodes[3] = nullptr;
}

NodeBase::~NodeBase()
{
    delete subnodes[0];
    delete subnodes[1];
    delete subnodes[2];
    delete subnodes[3];
    subnodes[0] = nullptr;
    subnodes[1] = nullptr;
    subnodes[2] = nullptr;
    subnodes[3] = nullptr;
}

vector<void*>&
NodeBase::getItems()
{
    return items;
}

void
NodeBase::add(void* item)
{
    items.push_back(item);
    //GEOS_DEBUG itemCount++;
    //GEOS_DEBUG System.out.print(itemCount);
}

vector<void*>&
NodeBase::addAllItems(vector<void*>& resultItems) const
{
    // this node may have items as well as subnodes (since items may not
    // be wholly contained in any single subnode
    resultItems.insert(resultItems.end(), items.begin(), items.end());

    for(const auto& subnode : subnodes) {
        if(subnode != nullptr) {
            subnode->addAllItems(resultItems);
        }
    }
    return resultItems;
}

void
NodeBase::addAllItemsFromOverlapping(const Envelope& searchEnv,
                                     vector<void*>& resultItems) const
{
    if(!isSearchMatch(searchEnv)) {
        return;
    }

    // this node may have items as well as subnodes (since items may not
    // be wholly contained in any single subnode
    resultItems.insert(resultItems.end(), items.begin(), items.end());

    for(const auto& subnode : subnodes) {
        if(subnode != nullptr) {
            subnode->addAllItemsFromOverlapping(searchEnv,
                                                resultItems);
        }
    }
}

//<<TODO:RENAME?>> In Samet's terminology, I think what we're returning here is
//actually level+1 rather than depth. (See p. 4 of his book) [Jon Aquino]
unsigned int
NodeBase::depth() const
{
    unsigned int maxSubDepth = 0;

    for(const auto& subnode : subnodes) {
        if(subnode != nullptr) {
            unsigned int sqd = subnode->depth();
            if(sqd > maxSubDepth) {
                maxSubDepth = sqd;
            }
        }
    }
    return maxSubDepth + 1;
}

size_t
NodeBase::size() const
{
    size_t subSize = 0;
    for(const auto& subnode : subnodes) {
        if(subnode != nullptr) {
            subSize += subnode->size();
        }
    }
    return subSize + items.size();
}

size_t
NodeBase::getNodeCount() const
{
    size_t subSize = 0;
    for(const auto& subnode : subnodes) {
        if(subnode != nullptr) {
            subSize += subnode->size();
        }
    }

    return subSize + 1;
}

string
NodeBase::toString() const
{
    ostringstream s;
    s << "ITEMS:" << items.size() << endl;
    for(size_t i = 0; i < subnodes.size(); i++) {
        s << "subnode[" << i << "] ";
        if(subnodes[i] == nullptr) {
            s << "NULL";
        }
        else {
            s << subnodes[i]->toString();
        }
        s << endl;
    }
    return s.str();
}

/*public*/
void
NodeBase::visit(const Envelope* searchEnv, ItemVisitor& visitor)
{
    if(! isSearchMatch(*searchEnv)) {
        return;
    }

    // this node may have items as well as subnodes (since items may not
    // be wholly contained in any single subnode
    visitItems(searchEnv, visitor);

    for(const auto& subnode : subnodes) {
        if(subnode != nullptr) {
            subnode->visit(searchEnv, visitor);
        }
    }
}

/*private*/
void
NodeBase::visitItems(const Envelope* searchEnv, ItemVisitor& visitor)
{
    ::geos::ignore_unused_variable_warning(searchEnv);

    // would be nice to filter items based on search envelope, but can't
    // until they contain an envelope
    for(auto& item : items) {
        visitor.visitItem(item);
    }
}

/*public*/
bool
NodeBase::remove(const Envelope* itemEnv, void* item)
{
    // use envelope to restrict nodes scanned
    if(! isSearchMatch(*itemEnv)) {
        return false;
    }

    bool found = false;
    for(auto& subnode : subnodes) {
        if(subnode != nullptr) {
            found = subnode->remove(itemEnv, item);
            if(found) {
                // trim subtree if empty
                if(subnode->isPrunable()) {
                    delete subnode;
                    subnode = nullptr;
                }
                break;
            }
        }
    }
    // if item was found lower down, don't need to search for it here
    if(found) {
        return found;
    }

    // otherwise, try and remove the item from the list of items
    // in this node
    auto foundIter = find(items.begin(), items.end(), item);
    if(foundIter != items.end()) {
        items.erase(foundIter);
        return true;
    }
    else {
        return false;
    }
}


} // namespace geos.index.quadtree
} // namespace geos.index
} // namespace geos
