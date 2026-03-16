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
#include <geos/index/bintree/Bintree.h>
#include <geos/index/bintree/Root.h>
#include <geos/index/bintree/Interval.h>
#include <vector>

namespace geos {
namespace index { // geos.index
namespace bintree { // geos.index.bintree

using namespace std;

Interval*
Bintree::ensureExtent(const Interval* itemInterval, double minExtent)
{
    double min = itemInterval->getMin();
    double max = itemInterval->getMax();
    // has a non-zero extent
    if(min != max) {
        // GEOS forces a copy here to be predictable wrt
        // memory management. May change in the future.
        return new Interval(*itemInterval);
    }

    // pad extent
    if(min == max) {
        min = min - minExtent / 2.0;
        max = min + minExtent / 2.0;
    }

    return new Interval(min, max);
}



Bintree::Bintree()
{
    minExtent = 1.0;
    root = new Root();
}

Bintree::~Bintree()
{
    for(unsigned int i = 0; i < newIntervals.size(); i++) {
        delete newIntervals[i];
    }
    delete root;
}

int
Bintree::depth()
{
    if(root != nullptr) {
        return root->depth();
    }
    return 0;
}

int
Bintree::size()
{
    if(root != nullptr) {
        return root->size();
    }
    return 0;
}

/**
 * Compute the total number of nodes in the tree
 *
 * @return the number of nodes in the tree
 */
int
Bintree::nodeSize()
{
    if(root != nullptr) {
        return root->nodeSize();
    }
    return 0;
}

void
Bintree::insert(Interval* itemInterval, void* item)
{
    collectStats(itemInterval);
    Interval* insertInterval = ensureExtent(itemInterval, minExtent);
    if(insertInterval != itemInterval) {
        newIntervals.push_back(insertInterval);
    }
    //int oldSize=size();
    root->insert(insertInterval, item);

    /* GEOS_DEBUG
    int newSize=size();
    System.out.println("BinTree: size="+newSize+"   node size="+nodeSize());
    if (newSize <= oldSize) {
    System.out.println("Lost item!");
    root.insert(insertInterval, item);
    System.out.println("reinsertion size="+size());
    }
    */
}

vector<void*>*
Bintree::iterator()
{
    vector<void*>* foundItems = new vector<void*>();
    root->addAllItems(foundItems);
    return foundItems;
}

vector<void*>*
Bintree::query(double x)
{
    return query(new Interval(x, x));
}

/**
 * min and max may be the same value
 */
vector<void*>*
Bintree::query(Interval* interval)
{
    /*
     * the items that are matched are all items in intervals
     * which overlap the query interval
     */
    vector<void*>* foundItems = new vector<void*>();
    query(interval, foundItems);
    return foundItems;
}

void
Bintree::query(Interval* interval, vector<void*>* foundItems)
{
    root->addAllItemsFromOverlapping(interval, foundItems);
}

void
Bintree::collectStats(Interval* interval)
{
    double del = interval->getWidth();
    if(del < minExtent && del > 0.0) {
        minExtent = del;
    }
}

} // namespace geos.index.bintree
} // namespace geos.index
} // namespace geos
