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

#ifndef GEOS_INDEX_SPATIALINDEX_H
#define GEOS_INDEX_SPATIALINDEX_H

#include <geos/export.h>

#include <vector>

// Forward declarations
namespace geos {
namespace geom {
class Envelope;
}
namespace index {
class ItemVisitor;
}
}

namespace geos {
namespace index {

/** \brief
 * Abstract class defines basic insertion and query operations supported by
 * classes implementing spatial index algorithms.
 *
 * A spatial index typically provides a primary filter for range rectangle queries. A
 * secondary filter is required to test for exact intersection. Of course, this
 * secondary filter may consist of other tests besides intersection, such as
 * testing other kinds of spatial relationships.
 *
 * Last port: index/SpatialIndex.java rev. 1.11 (JTS-1.7)
 *
 */
class GEOS_DLL SpatialIndex {
public:

    virtual
    ~SpatialIndex() {}

    /** \brief
     * Adds a spatial item with an extent specified by the given Envelope
     * to the index
     *
     * @param itemEnv
     *    Envelope of the item, ownership left to caller.
     *    TODO: Reference hold by this class ?
     *
     * @param item
     *    Opaque item, ownership left to caller.
     *    Reference hold by this class.
     */
    virtual void insert(const geom::Envelope* itemEnv, void* item) = 0;

    /** \brief
     * Queries the index for all items whose extents intersect the given search Envelope
     *
     * Note that some kinds of indexes may also return objects which do not in fact
     * intersect the query envelope.
     *
     * @param searchEnv the envelope to query for
     */
    virtual void query(const geom::Envelope* searchEnv, std::vector<void*>&) = 0;

    /** \brief
     * Queries the index for all items whose extents intersect the given search Envelope
     * and applies an ItemVisitor to them.
     *
     * Note that some kinds of indexes may also return objects which do not in fact
     * intersect the query envelope.
     *
     * @param searchEnv the envelope to query for
     * @param visitor a visitor object to apply to the items found
     */
    virtual void query(const geom::Envelope* searchEnv, ItemVisitor& visitor) = 0;

    /** \brief
     * Removes a single item from the tree.
     *
     * @param itemEnv the Envelope of the item to remove
     * @param item the item to remove
     * @return <code>true</code> if the item was found
     */
    virtual bool remove(const geom::Envelope* itemEnv, void* item) = 0;

};


} // namespace geos.index
} // namespace geos

#endif // GEOS_INDEX_SPATIALINDEX_H

