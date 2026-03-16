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
 **********************************************************************
 *
 * Last port: index/quadtree/NodeBase.java rev 1.9 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_IDX_QUADTREE_NODEBASE_H
#define GEOS_IDX_QUADTREE_NODEBASE_H

#include <geos/export.h>
#include <array>
#include <vector>
#include <string>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class Envelope;
}
namespace index {
class ItemVisitor;
namespace quadtree {
class Node;
}
}
}

namespace geos {
namespace index { // geos::index
namespace quadtree { // geos::index::quadtree

/**
 * \brief
 * The base class for nodes in a Quadtree.
 *
 */
class GEOS_DLL NodeBase {

private:

    void visitItems(const geom::Envelope* searchEnv,
                    ItemVisitor& visitor);

public:

    static int getSubnodeIndex(const geom::Envelope* env,
                               const geom::Coordinate& centre);

    NodeBase();

    virtual ~NodeBase();

    std::vector<void*>& getItems();

    /// Add an item to this node.
    /// Ownership of the item is left to caller.
    void add(void* item);

    /// Push all node items to the given vector, return the argument
    std::vector<void*>& addAllItems(std::vector<void*>& resultItems) const;

    virtual void addAllItemsFromOverlapping(const geom::Envelope& searchEnv,
                                            std::vector<void*>& resultItems) const;

    unsigned int depth() const;

    size_t size() const;

    size_t getNodeCount() const;

    virtual std::string toString() const;

    virtual void visit(const geom::Envelope* searchEnv, ItemVisitor& visitor);

    /**
     * Removes a single item from this subtree.
     *
     * @param itemEnv the envelope containing the item
     * @param item the item to remove
     * @return <code>true</code> if the item was found and removed
     */
    bool remove(const geom::Envelope* itemEnv, void* item);

    bool hasItems() const;

    bool hasChildren() const;

    bool isPrunable() const;

protected:

    /// Actual items are NOT owned by this class
    std::vector<void*> items;

    /**
     * subquads are numbered as follows:
     * <pre>
     *  2 | 3
     *  --+--
     *  0 | 1
     * </pre>
     *
     * Nodes are owned by this class
     */
    std::array<Node*, 4> subnodes;

    virtual bool isSearchMatch(const geom::Envelope& searchEnv) const = 0;
};


// INLINES, To be moved in NodeBase.inl

inline bool
NodeBase::hasChildren() const
{
    for(const auto& subnode : subnodes) {
        if(subnode != nullptr) {
            return true;
        }
    }

    return false;
}

inline bool
NodeBase::isPrunable() const
{
    return !(hasChildren() || hasItems());
}

inline bool
NodeBase::hasItems() const
{
    return ! items.empty();
}

} // namespace geos::index::quadtree
} // namespace geos::index
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_IDX_QUADTREE_NODEBASE_H
