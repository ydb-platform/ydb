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
#ifndef GEOS_INDEX_STRTREE_ITEMBOUNDABLE_H
#define GEOS_INDEX_STRTREE_ITEMBOUNDABLE_H

#include <geos/export.h>

#include <geos/index/strtree/Boundable.h> // for inheritance

namespace geos {
namespace index { // geos::index
namespace strtree { // geos::index::strtree

/**
 * \brief
 * Boundable wrapper for a non-Boundable spatial object.
 * Used internally by AbstractSTRtree.
 *
 * \todo TODO: It's unclear who takes ownership of passed newBounds and newItem objects.
 */
class GEOS_DLL ItemBoundable: public Boundable {
public:

    ItemBoundable(const void* newBounds, void* newItem) : bounds(newBounds), item(newItem) {}
    ~ItemBoundable() override = default;

    bool isLeaf() const override {
        return true;
    }

    const void* getBounds() const override {
        return bounds;
    }

    void* getItem() const {
        return item;
    }

private:

    const void* bounds;
    void* item;
};

} // namespace geos::index::strtree
} // namespace geos::index
} // namespace geos

#endif // GEOS_INDEX_STRTREE_ITEMBOUNDABLE_H
