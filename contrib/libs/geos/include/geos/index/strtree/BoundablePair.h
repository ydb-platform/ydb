/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2016 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: index/strtree/BoundablePair.java (JTS-1.14)
 *
 **********************************************************************/

#ifndef GEOS_INDEX_STRTREE_BOUNDABLEPAIR_H
#define GEOS_INDEX_STRTREE_BOUNDABLEPAIR_H

#include <geos/index/strtree/Boundable.h>
#include <geos/index/strtree/ItemDistance.h>
#include <queue>

namespace geos {
namespace index {
namespace strtree {

/**
 * @brief A pair of [Boundables](@ref Boundable), whose
 *        leaf items support a distance metric between them.
 *
 * Used to compute the distance between the members,
 * and to expand a member relative to the other
 * in order to produce new branches of the
 * Branch-and-Bound evaluation tree.
 * Provides an ordering based on the distance between the members,
 * which allows building a priority queue by minimum distance.
 *
 * @author Martin Davis
 *
 */
class BoundablePair {
private:
    const Boundable* boundable1;
    const Boundable* boundable2;
    ItemDistance* itemDistance;
    double mDistance;

public:
    struct BoundablePairQueueCompare {
        bool
        operator()(const BoundablePair* a, const BoundablePair* b)
        {
            return a->getDistance() > b->getDistance();
        }
    };

    typedef std::priority_queue<BoundablePair*, std::vector<BoundablePair*>, BoundablePairQueueCompare> BoundablePairQueue;
    BoundablePair(const Boundable* boundable1, const Boundable* boundable2, ItemDistance* itemDistance);

    /**
     * Gets one of the member {@link Boundable}s in the pair
     * (indexed by [0, 1]).
     *
     * @param i the index of the member to return (0 or 1)
     * @return the chosen member
     */
    const Boundable* getBoundable(int i) const;

    /** \brief
     * Computes the distance between the {@link Boundable}s in this pair.
     * The boundables are either composites or leaves.
     *
     * If either is composite, the distance is computed as the minimum distance
     * between the bounds.
     * If both are leaves, the distance is computed by
     * ItemDistance::distance(const ItemBoundable* item1, const ItemBoundable* item2).
     *
     * @return the distance between the items
     */
    double distance() const;

    /** \brief
     * Gets the minimum possible distance between the Boundables in this pair.
     *
     * If the members are both items, this will be the
     * exact distance between them.
     * Otherwise, this distance will be a lower bound on
     * the distances between the items in the members.
     *
     * @return the exact or lower bound distance for this pair
     */
    double getDistance() const;

    /**
     * Tests if both elements of the pair are leaf nodes
     *
     * @return true if both pair elements are leaf nodes
     */
    bool isLeaves() const;

    /** \brief
     * Computes the maximum distance between any
     * two items in the pair of nodes.
     *
     * @return the maximum distance between items in the pair
     */
    double maximumDistance();

    static bool isComposite(const Boundable* item);

    static double area(const Boundable* b);

    void expandToQueue(BoundablePairQueue&, double minDistance);
    void expand(const Boundable* bndComposite, const Boundable* bndOther, bool isFlipped, BoundablePairQueue& priQ,
                double minDistance);
};
}
}
}

#endif

