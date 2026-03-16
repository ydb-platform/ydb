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

#ifndef GEOS_IDX_BINTREE_BINTREE_H
#define GEOS_IDX_BINTREE_BINTREE_H

#include <geos/export.h>
#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace index {
namespace bintree {
class Interval;
class Root;
}
}
}

namespace geos {
namespace index { // geos::index
namespace bintree { // geos::index::bintree

/** \brief
 * A BinTree (or "Binary Interval Tree") is a 1-dimensional version of a quadtree.
 *
 * It indexes 1-dimensional intervals (which of course may be the projection
 * of 2-D objects on an axis). It supports range searching (where the range
 * may be a single point).
 *
 * This implementation does not require specifying the extent of the inserted
 * items beforehand. It will automatically expand to accomodate any extent
 * of dataset.
 *
 * This index is different to the "Interval Tree of Edelsbrunner"
 * or the "Segment Tree of Bentley".
 */
class GEOS_DLL Bintree {

public:

    /** \brief
     * Ensure that the Interval for the inserted item has non-zero extents.
     *
     * Use the current minExtent to pad it, if necessary.
     *
     * @note In GEOS this function always return a newly allocated object
     *       with ownership transferred to caller. TODO: change this ?
     *
     * @param itemInterval source interval, ownership left to caller, no references hold
     * @param minExtent minimal extent
     */
    static Interval* ensureExtent(const Interval* itemInterval,
                                  double minExtent);

    Bintree();

    ~Bintree();

    int depth();

    int size();

    int nodeSize();

    /// @param itemInterval
    ///     Ownership left to caller, NO reference hold by this class.
    ///
    /// @param item
    ///     Ownership left to caller, reference kept by this class.
    ///
    void insert(Interval* itemInterval, void* item);

    std::vector<void*>* iterator();

    std::vector<void*>* query(double x);

    std::vector<void*>* query(Interval* interval);

    void query(Interval* interval,
               std::vector<void*>* foundItems);

private:

    std::vector<Interval*>newIntervals;

    Root* root;

    /**
     *  Statistics
     *
     * minExtent is the minimum extent of all items
     * inserted into the tree so far. It is used as a heuristic value
     * to construct non-zero extents for features with zero extent.
     * Start with a non-zero extent, in case the first feature inserted has
     * a zero extent in both directions.  This value may be non-optimal, but
     * only one feature will be inserted with this value.
     */
    double minExtent;

    void collectStats(Interval* interval);

    Bintree(const Bintree&) = delete;
    Bintree& operator=(const Bintree&) = delete;
};

} // namespace geos::index::bintree
} // namespace geos::index
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_IDX_BINTREE_BINTREE_H

