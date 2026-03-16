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
#include <cstddef>
#include <geos/export.h>
#include <geos/geom/Envelope.h>
#include <geos/index/strtree/SimpleSTRtree.h>
#include <geos/index/strtree/SimpleSTRnode.h>

#include <vector>
#include <queue>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace index { // geos::index
namespace strtree { // geos::index::strtree





class GEOS_DLL SimpleSTRpair {

private:

    SimpleSTRnode* node1;
    SimpleSTRnode* node2;
    ItemDistance* itemDistance;
    double m_distance;

    /** \brief
     * Computes the distance between the {@link SimpleSTRnode}s in this pair.
     * The nodes are either composites or leaves.
     *
     * If either is composite, the distance is computed as the minimum distance
     * between the bounds.
     * If both are leaves, the distance is computed by
     * ItemDistance::distance(const void* item1, const void* item2).
     *
     * @return the distance between the items
     */
    double distance();

public:

    SimpleSTRpair(SimpleSTRnode* p_node1, SimpleSTRnode* p_node2, ItemDistance* p_itemDistance)
        : node1(p_node1)
        , node2(p_node2)
        , itemDistance(p_itemDistance)
        {
            m_distance = distance();
        }

    SimpleSTRnode* getNode(int i) const;

    /** \brief
     * Gets the minimum possible distance between the SimpleSTRnode in this pair.
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

    friend std::ostream& operator<<(std::ostream& os, SimpleSTRpair& pair);


};



class GEOS_DLL SimpleSTRdistance {


public:

    struct STRpairQueueCompare {
        bool
        operator()(const SimpleSTRpair* a, const SimpleSTRpair* b)
        {
            return a->getDistance() > b->getDistance();
        }
    };

    typedef std::priority_queue<SimpleSTRpair*,
        std::vector<SimpleSTRpair*>,
        STRpairQueueCompare> STRpairQueue;


    /* Initialize class */
    SimpleSTRdistance(SimpleSTRnode* root1, SimpleSTRnode* root2, ItemDistance* p_itemDistance);

    /* Turn over the calculation */
    std::pair<const void*, const void*> nearestNeighbour();
    bool isWithinDistance(double maxDistance);


private:

    std::deque<SimpleSTRpair> pairStore;
    SimpleSTRpair* initPair;
    ItemDistance* itemDistance;

    /* Utility method to store allocated pairs */
    SimpleSTRpair* createPair(SimpleSTRnode* p_node1, SimpleSTRnode* p_node2, ItemDistance* p_itemDistance);

    std::pair<const void*, const void*> nearestNeighbour(SimpleSTRpair* p_initPair);
    std::pair<const void*, const void*> nearestNeighbour(SimpleSTRpair* p_initPair, double maxDistance);

    bool isWithinDistance(SimpleSTRpair* p_initPair, double maxDistance);

    void expandToQueue(SimpleSTRpair* pair, STRpairQueue&, double minDistance);
    void expand(SimpleSTRnode* nodeComposite, SimpleSTRnode* nodeOther,
        bool isFlipped, STRpairQueue& priQ, double minDistance);


};





} // namespace geos::index::strtree
} // namespace geos::index
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

