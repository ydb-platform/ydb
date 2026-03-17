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

#include <geos/export.h>
#include <geos/index/SpatialIndex.h> // for inheritance
#include <geos/geom/Envelope.h>
#include <geos/index/strtree/SimpleSTRnode.h>

#include <vector>
#include <utility>


#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif


// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class Envelope;
}
namespace index {
namespace strtree {
class ItemDistance;
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
 * tree has been built (explicitly or on the first call to query), items may
 * not be added or removed.
 *
 * Described in: P. Rigaux, Michel Scholl and Agnes Voisard. Spatial
 * Databases With Application To GIS. Morgan Kaufmann, San Francisco, 2002.
 *
 */
class GEOS_DLL SimpleSTRtree: public SpatialIndex {


private:

    /* Members */
    std::deque<SimpleSTRnode> nodesQue;
    std::vector<SimpleSTRnode*> nodes;
    std::size_t nodeCapacity;
    bool built;

    /*
    * Allocate node in nodesQue std::deque for memory locality,
    * return reference to node.
    */
    SimpleSTRnode* createNode(int newLevel, const geom::Envelope* itemEnv, void* item);
    SimpleSTRnode* createNode(int newLevel);


    void build();

    static void sortNodesY(std::vector<SimpleSTRnode*>& nodeList);
    static void sortNodesX(std::vector<SimpleSTRnode*>& nodeList);

    void query(const geom::Envelope* searchEnv, const SimpleSTRnode* node, ItemVisitor& visitor);
    void query(const geom::Envelope* searchEnv, const SimpleSTRnode* node, std::vector<void*>& matches);

    /* Turn off copy constructors for MSVC */
    SimpleSTRtree(const SimpleSTRtree&) = delete;
    SimpleSTRtree& operator=(const SimpleSTRtree&) = delete;

    std::vector<SimpleSTRnode*> createHigherLevels(
        std::vector<SimpleSTRnode*>& nodesOfALevel, int level);

    void addParentNodesFromVerticalSlice(
        std::vector<SimpleSTRnode*>& verticalSlice,
        int newLevel,
        std::vector<SimpleSTRnode*>& parentNodes);

    std::vector<SimpleSTRnode*> createParentNodes(
        std::vector<SimpleSTRnode*>& childNodes,
        int newLevel);

    bool remove(const geom::Envelope* searchBounds, SimpleSTRnode* node, void* item);


public:

    /* Member */
    SimpleSTRnode* root;

    /**
     * Constructs an STRtree with the given maximum number of child nodes that
     * a node may have
     */
    SimpleSTRtree(std::size_t capacity = 10)
        : nodeCapacity(capacity)
        , built(false)
        , root(nullptr)
        {};

    std::size_t getNodeCapacity() const {
        return nodeCapacity;
    }

    std::size_t getNumLeafNodes() const {
        if (!root)
            return 0;
        else
            return root->getNumLeafNodes();
    }



    bool getBuilt() const {
        return built;
    }

    SimpleSTRnode* getRoot() {
        build();
        return root;
    }

    void insert(geom::Geometry* geom);

    void insert(const geom::Envelope* itemEnv, void* item) override;

    void iterate(ItemVisitor& visitor);

    void query(const geom::Envelope* searchEnv, std::vector<void*>& matches) override;

    void query(const geom::Envelope* searchEnv, ItemVisitor& visitor) override;

    bool remove(const geom::Envelope* searchBounds, void* item) override;

    friend std::ostream& operator<<(std::ostream& os, const SimpleSTRtree& tree);


    /*********************************************************************************/
    /* Nearest neighbor searches, public API */

    /* Internal nearest node to node */
    std::pair<const void*, const void*> nearestNeighbour(ItemDistance* itemDist);

    /* Nearest to another geometry/item */
    const void* nearestNeighbour(const geom::Envelope* env, const void* item, ItemDistance* itemDist);

    /* Nearest to another tree */
    std::pair<const void*, const void*> nearestNeighbour(SimpleSTRtree& tree, ItemDistance* itemDist);

    /* Radius test */
    bool isWithinDistance(SimpleSTRtree& tree, ItemDistance* itemDist, double maxDistance);


};

} // namespace geos::index::strtree
} // namespace geos::index
} // namespace geos


#ifdef _MSC_VER
#pragma warning(pop)
#endif

