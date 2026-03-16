/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009  Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: index/quadtree/Key.java rev 1.8 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_IDX_QUADTREE_KEY_H
#define GEOS_IDX_QUADTREE_KEY_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h> // for composition
#include <geos/geom/Envelope.h> // for composition

// Forward declarations
// ...

namespace geos {
namespace index { // geos::index
namespace quadtree { // geos::index::quadtree

/**
 * \brief
 * A Key is a unique identifier for a node in a quadtree.
 *
 * It contains a lower-left point and a level number. The level number
 * is the power of two for the size of the node envelope
 */
class GEOS_DLL Key {
public:

    // Doesn't touch the Envelope, might as well be const
    static int computeQuadLevel(const geom::Envelope& env);

    // Reference to argument won't be used after construction
    Key(const geom::Envelope& itemEnv);

    // used to be virtual, but I don't see subclasses...
    ~Key() = default;

    /// Returned object ownership retained by this class
    const geom::Coordinate& getPoint() const;

    int getLevel() const;

    /// Returned object ownership retained by this class
    const geom::Envelope& getEnvelope() const;

    /// Returns newly allocated object (ownership transferred)
    geom::Coordinate* getCentre() const;

    /**
     * return a square envelope containing the argument envelope,
     * whose extent is a power of two and which is based at a power of 2
     */
    void computeKey(const geom::Envelope& itemEnv);

private:
    // the fields which make up the key

    // Owned by this class
    geom::Coordinate pt;

    int level;

    // auxiliary data which is derived from the key for use in computation
    geom::Envelope env;

    void computeKey(int level, const geom::Envelope& itemEnv);
};

} // namespace geos::index::quadtree
} // namespace geos::index
} // namespace geos

#endif // GEOS_IDX_QUADTREE_KEY_H
