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

#include <geos/index/intervalrtree/SortedPackedIntervalRTree.h>
#include <geos/index/intervalrtree/IntervalRTreeNode.h>
#include <geos/index/intervalrtree/IntervalRTreeLeafNode.h>
#include <geos/index/intervalrtree/IntervalRTreeBranchNode.h>
#include <geos/index/ItemVisitor.h>

#include <algorithm>

#ifdef _MSC_VER
#pragma warning(disable : 4127)
#endif

namespace geos {
namespace index {
namespace intervalrtree {
//
// private:
//
void
SortedPackedIntervalRTree::init()
{
    // Already built
    if(root != nullptr)
        return;

    /*
     * if leaves is empty then nothing has been inserted.
     * In this case it is safe to leave the tree in an open state
     */
    if (leaves.empty()) return;

    root = buildTree();
}

const IntervalRTreeNode*
SortedPackedIntervalRTree::buildTree()
{
    branches.reserve(leaves.size() - 1);

    // sort the leaf nodes
    std::sort(leaves.begin(), leaves.end(),
            [](const IntervalRTreeLeafNode & n1, const IntervalRTreeLeafNode & n2) {
                double mid1 = n1.getMin() + n1.getMax();
                double mid2 = n2.getMin() + n2.getMax();

                return mid1 > mid2;
            });

    // now group nodes into blocks of two and build tree up recursively
    std::vector<const IntervalRTreeNode*> src{leaves.size()};
    std::vector<const IntervalRTreeNode*> dest;
    std::transform(leaves.begin(), leaves.end(), src.begin(), [](const IntervalRTreeLeafNode & n) {
        return &n;
    });

    while(true) {
        buildLevel(src, dest);

        if(dest.size() == 1) {
            return dest[0];
        }

        std::swap(src, dest);
    }
}

void
SortedPackedIntervalRTree::buildLevel(IntervalRTreeNode::ConstVect& src, IntervalRTreeNode::ConstVect& dest)
{
    level++;

    dest.clear();

    for(size_t i = 0, ni = src.size(); i < ni; i += 2) {
        const IntervalRTreeNode* n1 = src[i];

        if(i + 1 < ni) {
            const IntervalRTreeNode* n2 = src[i + 1];

            branches.emplace_back(n1, n2);
            dest.push_back(&branches.back());
        }
        else {
            dest.push_back(n1);
        }
    }
}

//
// protected:
//

//
// public:
//


void
SortedPackedIntervalRTree::query(double min, double max, index::ItemVisitor* visitor)
{
    init();

    // if root is null tree must be empty
    if (root == nullptr)
      return;

    root->query(min, max, visitor);
}

} // geos::intervalrtree
} // geos::index
} // geos
