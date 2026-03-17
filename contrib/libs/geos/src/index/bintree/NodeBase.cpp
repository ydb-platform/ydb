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

#include <geos/index/bintree/NodeBase.h>
#include <geos/index/bintree/Interval.h>
#include <geos/index/bintree/Node.h>

#include <vector>

using namespace std;

namespace geos {
namespace index { // geos.index
namespace bintree { // geos.index.bintree

/**
 * Returns the index of the subnode that wholely contains the given interval.
 * If none does, returns -1.
 */
int
NodeBase::getSubnodeIndex(Interval* interval, double centre)
{
    int subnodeIndex = -1;
    if(interval->min >= centre) {
        subnodeIndex = 1;
    }
    if(interval->max <= centre) {
        subnodeIndex = 0;
    }
    return subnodeIndex;
}

NodeBase::NodeBase()
{
    items = new vector<void*>();
    subnode[0] = nullptr;
    subnode[1] = nullptr;
}

NodeBase::~NodeBase()
{
    delete items;
    delete subnode[0];
    delete subnode[1];
    subnode[0] = nullptr;
    subnode[1] = nullptr;
}

vector<void*>*
NodeBase::getItems()
{
    return items;
}

void
NodeBase::add(void* item)
{
    items->push_back(item);
}

vector<void*>*
NodeBase::addAllItems(vector<void*>* newItems)
{
    items->insert(items->end(), newItems->begin(), newItems->end());
    for(int i = 0; i < 2; i++) {
        if(subnode[i] != nullptr) {
            subnode[i]->addAllItems(newItems);
        }
    }
    return items;
}

vector<void*>*
NodeBase::addAllItemsFromOverlapping(Interval* interval, vector<void*>* resultItems)
{
    if(!isSearchMatch(interval)) {
        return items;
    }
    resultItems->insert(resultItems->end(), items->begin(), items->end());
    for(int i = 0; i < 2; i++) {
        if(subnode[i] != nullptr) {
            subnode[i]->addAllItemsFromOverlapping(interval, resultItems);
        }
    }
    return items;
}

int
NodeBase::depth()
{
    int maxSubDepth = 0;
    for(int i = 0; i < 2; i++) {
        if(subnode[i] != nullptr) {
            int sqd = subnode[i]->depth();
            if(sqd > maxSubDepth) {
                maxSubDepth = sqd;
            }
        }
    }
    return maxSubDepth + 1;
}

int
NodeBase::size()
{
    int subSize = 0;
    for(int i = 0; i < 2; i++) {
        if(subnode[i] != nullptr) {
            subSize += subnode[i]->size();
        }
    }
    return subSize + (int)items->size();
}

int
NodeBase::nodeSize()
{
    int subSize = 0;
    for(int i = 0; i < 2; i++) {
        if(subnode[i] != nullptr) {
            subSize += subnode[i]->nodeSize();
        }
    }
    return subSize + 1;
}


} // namespace geos.index.bintree
} // namespace geos.index
} // namespace geos
