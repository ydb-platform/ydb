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

#ifndef GEOS_INDEX_INTERVALRTREE_SORTEDPACKEDINTERVALRTREE_H
#define GEOS_INDEX_INTERVALRTREE_SORTEDPACKEDINTERVALRTREE_H

#include <geos/index/intervalrtree/IntervalRTreeNode.h>
#include <geos/index/intervalrtree/IntervalRTreeBranchNode.h>
#include <geos/index/intervalrtree/IntervalRTreeLeafNode.h>
#include <geos/util/UnsupportedOperationException.h>

// forward declarations
namespace geos {
namespace index {
class ItemVisitor;
}
}

namespace geos {
namespace index {
namespace intervalrtree {

/** \brief
 * A static index on a set of 1-dimensional intervals,
 * using an R-Tree packed based on the order of the interval midpoints.
 *
 * It supports range searching,
 * where the range is an interval of the real line (which may be a single point).
 * A common use is to index 1-dimensional intervals which
 * are the projection of 2-D objects onto an axis of the coordinate system.
 *
 * This index structure is <i>static</i>
 * - items cannot be added or removed once the first query has been made.
 * The advantage of this characteristic is that the index performance
 * can be optimized based on a fixed set of items.
 *
 * @author Martin Davis
 *
 */
class SortedPackedIntervalRTree {
private:
    std::vector<IntervalRTreeLeafNode> leaves;
    std::vector<IntervalRTreeBranchNode> branches;

    /**
    * If root is null that indicates
    * that the tree has not yet been built,
    * OR nothing has been added to the tree.
    * In both cases, the tree is still open for insertions.
    */
    const IntervalRTreeNode* root = nullptr;
    int level = 0;

    void init();
    void buildLevel(IntervalRTreeNode::ConstVect& src, IntervalRTreeNode::ConstVect& dest);
    const IntervalRTreeNode* buildTree();

protected:
public:
    SortedPackedIntervalRTree() {}

    SortedPackedIntervalRTree(std::size_t initialCapacity)
    {
        leaves.reserve(initialCapacity);
    }

    /**
     * Adds an item to the index which is associated with the given interval
     *
     * @param min the lower bound of the item interval
     * @param max the upper bound of the item interval
     * @param item the item to insert, ownership left to caller
     *
     * @throw IllegalStateException if the index has already been queried
     */
    void insert(double min, double max, void* item) {
        if(root != nullptr) {
            throw util::UnsupportedOperationException("Index cannot be added to once it has been queried");
        }

        leaves.emplace_back(min, max, item);
    }

    /**
     * Search for intervals in the index which intersect the given closed interval
     * and apply the visitor to them.
     *
     * @param min the lower bound of the query interval
     * @param max the upper bound of the query interval
     * @param visitor the visitor to pass any matched items to
     */
    void query(double min, double max, index::ItemVisitor* visitor);

};

} // geos::intervalrtree
} // geos::index
} // geos

#endif // GEOS_INDEX_INTERVALRTREE_SORTEDPACKEDINTERVALRTREE_H

