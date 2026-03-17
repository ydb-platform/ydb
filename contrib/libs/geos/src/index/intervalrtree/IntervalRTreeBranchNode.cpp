/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/index/intervalrtree/IntervalRTreeBranchNode.h>
#include <geos/index/ItemVisitor.h>


namespace geos {
namespace index {
namespace intervalrtree {
//
// private:
//

//
// protected:
//

//
// public:
//


void
IntervalRTreeBranchNode::query(double queryMin, double queryMax, index::ItemVisitor* visitor) const
{
    if(! intersects(queryMin, queryMax)) {
        return;
    }

    if(node1) {
        node1->query(queryMin, queryMax, visitor);
    }

    if(node2) {
        node2->query(queryMin, queryMax, visitor);
    }
}


} // geos::intervalrtree
} // geos::index
} // geos
