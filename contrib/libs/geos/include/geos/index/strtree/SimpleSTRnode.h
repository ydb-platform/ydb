/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#pragma once

#include <cassert>
#include <geos/export.h>
#include <geos/geom/Envelope.h>
#include <geos/index/strtree/ItemBoundable.h>

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace index { // geos::index
namespace strtree { // geos::index::strtree

/** \brief
 * A node of the STR tree.
 *
 */
class GEOS_DLL SimpleSTRnode : public ItemBoundable {

private:

    std::vector<SimpleSTRnode*> childNodes;
    void *item;
    geom::Envelope bounds;
    std::size_t level;

public:

    /*
     * Constructs an AbstractNode at the given level in the tree
     */
    SimpleSTRnode(std::size_t newLevel, const geom::Envelope *p_env, void* p_item, size_t capacity = 10)
        : ItemBoundable(p_env, p_item)
        , item(p_item)
        , bounds()
        , level(newLevel)
    {
        childNodes.reserve(capacity);
        if (p_env) {
            bounds = *p_env;
        }

    }

    SimpleSTRnode(std::size_t newLevel)
        : SimpleSTRnode(newLevel, nullptr, nullptr)
    {}

    void toString(std::ostream& os, int indentLevel) const;

    std::size_t getNumNodes() const;
    std::size_t getNumLeafNodes() const;

    const std::vector<SimpleSTRnode*>&
    getChildNodes() const
    {
        return childNodes;
    }

    void* getItem() const {
        return item;
    }

    bool removeItem(void *item);
    bool removeChild(SimpleSTRnode *child);

    /**
     * Returns a representation of space that encloses this Node
     */
    const inline geom::Envelope& getEnvelope() const {
        return bounds;
    }

    const void* getBounds() const override {
        return &bounds;
    }

    /**
    * Returns 0 if this node is a leaf, 1 if a parent of a leaf, and so on; the
    * root node will have the highest level
    */
    std::size_t getLevel() const {
        return level;
    }

    std::size_t size() const {
        return childNodes.size();
    }

    /**
     * Adds either an AbstractNode, or if this is a leaf node, a data object
     * (wrapped in an ItemBoundable)
     */
    void addChildNode(SimpleSTRnode* childNode);

    bool isLeaf() const override
    {
        return level == 0;
    }

    bool isComposite() const
    {
        return ! isLeaf();
    }

    double area() const
    {
        return bounds.getArea();
    }


};


} // namespace geos::index::strtree
} // namespace geos::index
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

