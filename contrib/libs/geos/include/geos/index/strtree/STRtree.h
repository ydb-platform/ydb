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
 * Last port: index/strtree/STRtree.java rev. 1.11
 *
 **********************************************************************/

#ifndef GEOS_INDEX_STRTREE_STRTREE_H
#define GEOS_INDEX_STRTREE_STRTREE_H

#include <geos/export.h>
#include <geos/index/strtree/ItemDistance.h>
#include <geos/index/strtree/BoundablePair.h>
#include <geos/index/strtree/AbstractSTRtree.h> // for inheritance
#include <geos/index/SpatialIndex.h> // for inheritance
#include <geos/geom/Envelope.h> // for inlines

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace index {
namespace strtree {
class Boundable;
}
}
}

namespace geos {
namespace index { // geos::index
namespace strtree { // geos::index::strtree

/**
 * \brief
 * A query-only R-tree created using the Sort-Tile-Recursive (STR) algorithm.
 * For two-dimensional spatial data.
 *
 * The STR packed R-tree is simple to implement and maximizes space
 * utilization; that is, as many leaves as possible are filled to capacity.
 * Overlap between nodes is far less than in a basic R-tree. However, once the
 * tree has been built (explicitly or on the first call to #query), items may
 * not be added or removed.
 *
 * Described in: P. Rigaux, Michel Scholl and Agnes Voisard. Spatial
 * Databases With Application To GIS. Morgan Kaufmann, San Francisco, 2002.
 *
 */
class GEOS_DLL STRtree: public AbstractSTRtree, public SpatialIndex {
    using AbstractSTRtree::insert;
    using AbstractSTRtree::query;

private:
    class GEOS_DLL STRIntersectsOp: public AbstractSTRtree::IntersectsOp {
    public:
        bool intersects(const void* aBounds, const void* bBounds) override;
    };

    /**
     * Creates the parent level for the given child level. First, orders the items
     * by the x-values of the midpoints, and groups them into vertical slices.
     * For each slice, orders the items by the y-values of the midpoints, and
     * group them into runs of size M (the node capacity). For each run, creates
     * a new (parent) node.
     */
    std::unique_ptr<BoundableList> createParentBoundables(BoundableList* childBoundables, int newLevel) override;

    std::unique_ptr<BoundableList> createParentBoundablesFromVerticalSlices(std::vector<BoundableList*>* verticalSlices,
            int newLevel);

    STRIntersectsOp intersectsOp;

    std::unique_ptr<BoundableList> sortBoundablesY(const BoundableList* input);
    std::unique_ptr<BoundableList> sortBoundablesX(const BoundableList* input);

    std::unique_ptr<BoundableList> createParentBoundablesFromVerticalSlice(
        BoundableList* childBoundables,
        int newLevel);

    /**
     * @param childBoundables Must be sorted by the x-value of
     *        the envelope midpoints
     * @return
     */
    std::vector<BoundableList*>* verticalSlices(
        BoundableList* childBoundables,
        size_t sliceCount);

    bool isWithinDistance(BoundablePair* initBndPair, double maxDistance);

protected:

    AbstractNode* createNode(int level) override;

    IntersectsOp*
    getIntersectsOp() override
    {
        return &intersectsOp;
    }

public:

    ~STRtree() override = default;

    /**
     * Constructs an STRtree with the given maximum number of child nodes that
     * a node may have
     */
    STRtree(std::size_t nodeCapacity = 10);

    void insert(const geom::Envelope* itemEnv, void* item) override;

    //static double centreX(const geom::Envelope *e);

    static double
    avg(double a, double b)
    {
        return (a + b) / 2.0;
    }

    static double
    centreY(const geom::Envelope* e)
    {
        return STRtree::avg(e->getMinY(), e->getMaxY());
    }

    void
    query(const geom::Envelope* searchEnv, std::vector<void*>& matches) override
    {
        AbstractSTRtree::query(searchEnv, matches);
    }

    void
    query(const geom::Envelope* searchEnv, ItemVisitor& visitor) override
    {
        return AbstractSTRtree::query(searchEnv, visitor);
    }

    std::pair<const void*, const void*> nearestNeighbour(ItemDistance* itemDist);
    const void* nearestNeighbour(const geom::Envelope* env, const void* item, ItemDistance* itemDist);
    std::pair<const void*, const void*> nearestNeighbour(STRtree* tree, ItemDistance* itemDist);
    std::pair<const void*, const void*> nearestNeighbour(BoundablePair* initBndPair);
    std::pair<const void*, const void*> nearestNeighbour(BoundablePair* initBndPair, double maxDistance);

    bool
    remove(const geom::Envelope* itemEnv, void* item) override
    {
        return AbstractSTRtree::remove(itemEnv, item);
    }

    bool isWithinDistance(STRtree* tree, ItemDistance* itemDist, double maxDistance);

};

} // namespace geos::index::strtree
} // namespace geos::index
} // namespace geos


#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_INDEX_STRTREE_STRTREE_H
