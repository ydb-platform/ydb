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

#ifndef GEOS_INDEX_INTERVALRTREE_INTERVALRTREELEAFNODE_H
#define GEOS_INDEX_INTERVALRTREE_INTERVALRTREELEAFNODE_H


#include <geos/index/intervalrtree/IntervalRTreeNode.h> // inherited


// forward declarations
namespace geos {
namespace index {
class ItemVisitor;
}
}


namespace geos {
namespace index {
namespace intervalrtree {

class IntervalRTreeLeafNode : public IntervalRTreeNode {
private:
    /// externally owned
    void* item;

protected:
public:

    /// @param p_item externally owned
    IntervalRTreeLeafNode(double p_min, double p_max, void* p_item)
        :	IntervalRTreeNode(p_min, p_max),
          item(p_item)
    { }

    ~IntervalRTreeLeafNode() override
    {
    }

    void query(double queryMin, double queryMax, index::ItemVisitor* visitor) const override;

};

} // geos::intervalrtree
} // geos::index
} // geos

#endif // GEOS_INDEX_INTERVALRTREE_INTERVALRTREELEAFNODE_H
