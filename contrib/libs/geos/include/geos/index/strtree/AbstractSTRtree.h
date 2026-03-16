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

#ifndef GEOS_INDEX_STRTREE_ABSTRACTSTRTREE_H
#define GEOS_INDEX_STRTREE_ABSTRACTSTRTREE_H

#include <geos/export.h>

#include <geos/index/strtree/AbstractNode.h> // for inlines

#include <vector>
#include <list>
#include <memory> // for unique_ptr
#include <cassert> // for inlines
#include <algorithm>

// Forward declarations
namespace geos {
namespace index {
class ItemVisitor;
namespace strtree {
class Boundable;
class AbstractNode;
}
}
}

namespace geos {
namespace index { // geos::index
namespace strtree { // geos::index::strtree

/// A list of boundables. TODO: use a list
typedef std::vector<Boundable*> BoundableList;

/// list contains boundables or lists of boundables. The lists are owned by
/// this class, the plain boundables are held by reference only.
class ItemsList;

class ItemsListItem {
public:
    enum type {
        item_is_geometry,
        item_is_list
    };

    ItemsListItem(void* item_)
        : t(item_is_geometry)
    {
        item.g = item_;
    }
    ItemsListItem(ItemsList* item_)
        : t(item_is_list)
    {
        item.l = item_;
    }

    type
    get_type() const
    {
        return t;
    }

    void*
    get_geometry() const
    {
        assert(t == item_is_geometry);
        return item.g;
    }
    ItemsList*
    get_itemslist() const
    {
        assert(t == item_is_list);
        return item.l;
    }

    type t;
    union {
        void* g;
        ItemsList* l;
    } item;
};

class ItemsList : public std::vector<ItemsListItem> {
private:
    typedef std::vector<ItemsListItem> base_type;

    static void
    delete_item(ItemsListItem& item)
    {
        if(ItemsListItem::item_is_list == item.t) {
            delete item.item.l;
        }
    }

public:
    ~ItemsList()
    {
        std::for_each(begin(), end(), &ItemsList::delete_item);
    }

    // lists of items need to be deleted in the end
    void
    push_back(void* item)
    {
        this->base_type::push_back(ItemsListItem(item));
    }

    // lists of items need to be deleted in the end
    void
    push_back_owned(ItemsList* itemList)
    {
        this->base_type::push_back(ItemsListItem(itemList));
    }
};

/** \brief
 * Base class for STRtree and SIRtree.
 *
 * STR-packed R-trees are described in:
 * P. Rigaux, Michel Scholl and Agnes Voisard. Spatial Databases With
 * Application To GIS. Morgan Kaufmann, San Francisco, 2002.
 *
 * This implementation is based on Boundables rather than just AbstractNodes,
 * because the STR algorithm operates on both nodes and
 * data, both of which are treated here as Boundables.
 *
 */
class GEOS_DLL AbstractSTRtree {

private:
    bool built;
    BoundableList* itemBoundables;

    /**
     * Creates the levels higher than the given level
     *
     * @param boundablesOfALevel
     *            the level to build on
     * @param level
     *            the level of the Boundables, or -1 if the boundables are item
     *            boundables (that is, below level 0)
     * @return the root, which may be a ParentNode or a LeafNode
     */
    virtual AbstractNode* createHigherLevels(
        BoundableList* boundablesOfALevel,
        int level);

    std::unique_ptr<BoundableList> sortBoundablesY(const BoundableList* input);

    bool remove(const void* searchBounds, AbstractNode& node, void* item);
    bool removeItem(AbstractNode& node, void* item);

    ItemsList* itemsTree(AbstractNode* node);

    AbstractSTRtree(const AbstractSTRtree&) = delete;
    AbstractSTRtree& operator=(const AbstractSTRtree&) = delete;

protected:

    /** \brief
     * A test for intersection between two bounds, necessary because
     * subclasses of AbstractSTRtree have different implementations of
     * bounds.
     */
    class GEOS_DLL IntersectsOp {
    public:
        /**
         * For STRtrees, the bounds will be Envelopes; for
         * SIRtrees, Intervals; for other subclasses of
         * AbstractSTRtree, some other class.
         * @param aBounds the bounds of one spatial object
         * @param bBounds the bounds of another spatial object
         * @return whether the two bounds intersect
         */
        virtual bool intersects(const void* aBounds,
                                const void* bBounds) = 0;

        virtual
        ~IntersectsOp() {}
    };

    AbstractNode* root;

    std::vector <AbstractNode*>* nodes;

    // Ownership to caller (TODO: return by unique_ptr)
    virtual AbstractNode* createNode(int level) = 0;

    /** \brief
     * Sorts the childBoundables then divides them into groups of size M, where
     * M is the node capacity.
     */
    virtual std::unique_ptr<BoundableList> createParentBoundables(
        BoundableList* childBoundables, int newLevel);

    virtual AbstractNode*
    lastNode(BoundableList* nodeList)
    {
        assert(!nodeList->empty());
        // Cast from Boundable to AbstractNode
        return static_cast<AbstractNode*>(nodeList->back());
    }

    virtual AbstractNode*
    getRoot()
    {
        assert(built);
        return root;
    }

    ///  Also builds the tree, if necessary.
    virtual void insert(const void* bounds, void* item);

    ///  Also builds the tree, if necessary.
    void query(const void* searchBounds, std::vector<void*>& foundItems);

    ///  Also builds the tree, if necessary.
    void query(const void* searchBounds, ItemVisitor& visitor);

    void query(const void* searchBounds, const AbstractNode& node, ItemVisitor& visitor);

    ///  Also builds the tree, if necessary.
    bool remove(const void* itemEnv, void* item);

    std::unique_ptr<BoundableList> boundablesAtLevel(int level);

    // @@ should be size_t, probably
    std::size_t nodeCapacity;

    /**
     * @return a test for intersection between two bounds,
     * necessary because subclasses
     * of AbstractSTRtree have different implementations of bounds.
     * @see IntersectsOp
     */
    virtual IntersectsOp* getIntersectsOp() = 0;


public:

    /** \brief
     * Constructs an AbstractSTRtree with the specified maximum number of child
     * nodes that a node may have.
     */
    AbstractSTRtree(std::size_t newNodeCapacity)
        :
        built(false),
        itemBoundables(new BoundableList()),
        nodes(new std::vector<AbstractNode *>()),
        nodeCapacity(newNodeCapacity)
    {
        assert(newNodeCapacity > 1);
    }

    virtual ~AbstractSTRtree();

    /** \brief
     * Creates parent nodes, grandparent nodes, and so forth up to the root
     * node, for the data that has been inserted into the tree.
     *
     * Can only be
     * called once, and thus can be called only after all of the data has been
     * inserted into the tree.
     */
    virtual void build();

    /** \brief
     * Returns the maximum number of child nodes that a node may have.
     */
    virtual std::size_t
    getNodeCapacity()
    {
        return nodeCapacity;
    }

    virtual void query(const void* searchBounds, const AbstractNode* node, std::vector<void*>* matches);

    /**
     * Iterate over all items added thus far.  Explicitly does not build
     * the tree.
     */
    void iterate(ItemVisitor& visitor);


    /**
     * @param level -1 to get items
     * @param top an AbstractNode
     * @param boundables a BoundableList
     */
    virtual void boundablesAtLevel(int level, AbstractNode* top,
                                   BoundableList* boundables);

    /** \brief
     * Gets a tree structure (as a nested list) corresponding to the structure
     * of the items and nodes in this tree.
     *
     * The returned Lists contain either Object items, or Lists which
     * correspond to subtrees of the tree Subtrees which do not contain
     * any items are not included.
     *
     * Builds the tree if necessary.
     *
     * @note The caller is responsible for releasing the list
     *
     * @return a List of items and/or Lists
     */
    ItemsList* itemsTree();
};


} // namespace geos::index::strtree
} // namespace geos::index
} // namespace geos

#endif // GEOS_INDEX_STRTREE_ABSTRACTSTRTREE_H
