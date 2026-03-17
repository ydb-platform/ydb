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
 *
 **********************************************************************/

#ifndef GEOS_INDEX_INTERVALRTREE_INTERVALRTREEBRANCHNODE_H
#define GEOS_INDEX_INTERVALRTREE_INTERVALRTREEBRANCHNODE_H

#include <geos/index/intervalrtree/IntervalRTreeNode.h> // inherited

#include <algorithm>

// forward declarations
namespace geos {
namespace index {
class ItemVisitor;
}
}


namespace geos {
namespace index {
namespace intervalrtree {

class IntervalRTreeBranchNode : public IntervalRTreeNode {
private:
    const IntervalRTreeNode* node1;
    const IntervalRTreeNode* node2;

protected:
public:
    IntervalRTreeBranchNode(const IntervalRTreeNode* n1, const IntervalRTreeNode* n2)
        :	IntervalRTreeNode(std::min(n1->getMin(), n2->getMin()), std::max(n1->getMax(), n2->getMax())),
          node1(n1),
          node2(n2)
    { }

    void query(double queryMin, double queryMax, index::ItemVisitor* visitor) const override;
};

} // geos::intervalrtree
} // geos::index
} // geos

#endif // GEOS_INDEX_INTERVALRTREE_INTERVALRTREEBRANCHNODE_H

