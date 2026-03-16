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

#ifndef GEOS_INDEX_STRTREE_BOUNDABLE_H
#define GEOS_INDEX_STRTREE_BOUNDABLE_H

#include <geos/export.h>

namespace geos {
namespace index { // geos::index
namespace strtree { // geos::index::strtree

/// A spatial object in an AbstractSTRtree.
class GEOS_DLL Boundable {
public:
    /**
     * Returns a representation of space that encloses this Boundable,
     * preferably not much bigger than this Boundable's boundary yet
     * fast to test for intersection with the bounds of other Boundables.
     *
     * The class of object returned depends
     * on the subclass of AbstractSTRtree.
     *
     * @return an Envelope (for STRtrees), an Interval (for SIRtrees),
     * or other object (for other subclasses of AbstractSTRtree)
     *
     * @see AbstractSTRtree::IntersectsOp
     */
    virtual const void* getBounds() const = 0;

    virtual bool isLeaf() const = 0;
    virtual ~Boundable() {}
};


} // namespace geos::index::strtree
} // namespace geos::index
} // namespace geos

#endif // GEOS_INDEX_STRTREE_BOUNDABLE_H
