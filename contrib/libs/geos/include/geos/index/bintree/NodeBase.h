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
 **********************************************************************/

#ifndef GEOS_IDX_BINTREE_NODEBASE_H
#define GEOS_IDX_BINTREE_NODEBASE_H

#include <geos/export.h>
#include <vector>

// Forward declarations
namespace geos {
namespace index {
namespace bintree {
class Node;
class Interval;
}
}
}

namespace geos {
namespace index { // geos::index
namespace bintree { // geos::index::bintree

/// The base class for nodes in a Bintree.
class GEOS_DLL NodeBase {

public:

    static int getSubnodeIndex(Interval* interval, double centre);

    NodeBase();

    virtual ~NodeBase();

    virtual std::vector<void*>* getItems();

    virtual void add(void* item);

    virtual std::vector<void*>* addAllItems(std::vector<void*>* newItems);

    virtual std::vector<void*>* addAllItemsFromOverlapping(Interval* interval,
            std::vector<void*>* resultItems);

    virtual int depth();

    virtual int size();

    virtual int nodeSize();

protected:

    std::vector<void*>* items;

    /**
     * subnodes are numbered as follows:
     *
     *  0 | 1
     */
    Node* subnode[2];

    virtual bool isSearchMatch(Interval* interval) = 0;

private:

    NodeBase(const NodeBase&) = delete;
    NodeBase& operator=(const NodeBase&) = delete;

};

} // namespace geos::index::bintree
} // namespace geos::index
} // namespace geos

#endif // GEOS_IDX_BINTREE_NODEBASE_H

