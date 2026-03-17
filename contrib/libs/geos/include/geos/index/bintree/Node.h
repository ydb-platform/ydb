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

#ifndef GEOS_IDX_BINTREE_NODE_H
#define GEOS_IDX_BINTREE_NODE_H

#include <geos/export.h>
#include <geos/index/bintree/NodeBase.h> // for inheritance

// Forward declarations
namespace geos {
namespace index {
namespace bintree {
class Interval;
}
}
}

namespace geos {
namespace index { // geos::index
namespace bintree { // geos::index::bintree

/// A node of a Bintree.
class GEOS_DLL Node: public NodeBase {

public:

    static Node* createNode(Interval* itemInterval);

    static Node* createExpanded(Node* node, Interval* addInterval);

    Node(Interval* newInterval, int newLevel);

    ~Node() override;

    Interval* getInterval();

    Node* getNode(Interval* searchInterval);

    NodeBase* find(Interval* searchInterval);

    void insert(Node* node);

private:

    Interval* interval;

    double centre;

    int level;

    Node* getSubnode(int index);

    Node* createSubnode(int index);

protected:

    bool isSearchMatch(Interval* itemInterval) override;
};

} // namespace geos::index::bintree
} // namespace geos::index
} // namespace geos

#endif // GEOS_IDX_BINTREE_NODE_H

