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

#include <geos/index/strtree/SimpleSTRdistance.h>
#include <geos/index/strtree/SimpleSTRnode.h>
#include <geos/index/strtree/ItemDistance.h>
#include <geos/index/strtree/EnvelopeUtil.h>
#include <geos/geom/Envelope.h>
#include <geos/util/GEOSException.h>
#include <geos/util/IllegalArgumentException.h>

#include <iostream>

using namespace geos::geom;

namespace geos {
namespace index { // geos.index
namespace strtree { // geos.index.strtree



SimpleSTRdistance::SimpleSTRdistance(SimpleSTRnode* root1,
    SimpleSTRnode* root2, ItemDistance* p_itemDistance)
    : initPair(createPair(root1, root2, p_itemDistance))
    , itemDistance(p_itemDistance)
    {}


SimpleSTRpair*
SimpleSTRdistance::createPair(SimpleSTRnode* p_node1, SimpleSTRnode* p_node2,
    ItemDistance* p_itemDistance)
{
    pairStore.emplace_back(p_node1, p_node2, p_itemDistance);
    SimpleSTRpair& pair = pairStore.back();
    return &pair;
}


/*public*/
std::pair<const void*, const void*>
SimpleSTRdistance::nearestNeighbour()
{
    return nearestNeighbour(initPair);
}


/*private*/
std::pair<const void*, const void*>
SimpleSTRdistance::nearestNeighbour(SimpleSTRpair* p_initPair)
{
    return nearestNeighbour(p_initPair, std::numeric_limits<double>::infinity());
}


/*private*/
std::pair<const void*, const void*>
SimpleSTRdistance::nearestNeighbour(SimpleSTRpair* p_initPair, double maxDistance)
{
    double distanceLowerBound = maxDistance;
    SimpleSTRpair* minPair = nullptr;

    STRpairQueue priQ;
    priQ.push(p_initPair);

    while(!priQ.empty() && distanceLowerBound > 0.0) {
        SimpleSTRpair* pair = priQ.top();
        double currentDistance = pair->getDistance();

        // std::cout << *pair << std::endl;

        /*
         * If the distance for the first node in the queue
         * is >= the current minimum distance, all other nodes
         * in the queue must also have a greater distance.
         * So the current minDistance must be the true minimum,
         * and we are done.
         */
        if(minPair && currentDistance >= distanceLowerBound) {
            break;
        }

        priQ.pop();

        /*
         * If the pair members are leaves
         * then their distance is the exact lower bound.
         * Update the distanceLowerBound to reflect this
         * (which must be smaller, due to the test
         * immediately prior to this).
         */
        if(pair->isLeaves()) {
            distanceLowerBound = currentDistance;
            minPair = pair;
        }
        else {
            /*
             * Otherwise, expand one side of the pair,
             * (the choice of which side to expand is heuristically determined)
             * and insert the new expanded pairs into the queue
             */
            expandToQueue(pair, priQ, distanceLowerBound);
        }
    }

    /* Free any remaining node pairs in the queue */
    while(!priQ.empty()) {
        priQ.pop();
    }

    if(!minPair) {
        throw util::GEOSException("Error computing nearest neighbor");
    }

    const void* item0 = minPair->getNode(0)->getItem();
    const void* item1 = minPair->getNode(1)->getItem();

    return std::pair<const void*, const void*>(item0, item1);
}


void
SimpleSTRdistance::expandToQueue(SimpleSTRpair* pair, STRpairQueue& priQ, double minDistance)
{

    SimpleSTRnode* node1 = pair->getNode(0);
    SimpleSTRnode* node2 = pair->getNode(1);

    bool isComp1 = node1->isComposite();
    bool isComp2 = node2->isComposite();

    /**
     * HEURISTIC: If both boundable are composite,
     * choose the one with largest area to expand.
     * Otherwise, simply expand whichever is composite.
     */
    if (isComp1 && isComp2) {
      if (node1->area() > node2->area()) {
        expand(node1, node2, false, priQ, minDistance);
        return;
      }
      else {
        expand(node2, node1, true, priQ, minDistance);
        return;
      }
    }
    else if (isComp1) {
      expand(node1, node2, false, priQ, minDistance);
      return;
    }
    else if (isComp2) {
      expand(node2, node1, true, priQ, minDistance);
      return;
    }

    throw util::IllegalArgumentException("neither boundable is composite");
}


void
SimpleSTRdistance::expand(SimpleSTRnode* nodeComposite, SimpleSTRnode* nodeOther,
    bool isFlipped, STRpairQueue& priQ, double minDistance)
{
    auto children = nodeComposite->getChildNodes();
    for (auto* child: children) {
        SimpleSTRpair* sp = nullptr;
        if (isFlipped) {
            sp = createPair(nodeOther, child, itemDistance);
        }
        else {
            sp = createPair(child, nodeOther, itemDistance);
        }
        // only add to queue if this pair might contain the closest points
        // MD - it's actually faster to construct the object rather than called distance(child, nodeOther)!
        if (sp->getDistance() < minDistance) {
            priQ.push(sp);
        }
    }
}

/* public */
bool
SimpleSTRdistance::isWithinDistance(double maxDistance)
{
    return isWithinDistance(initPair, maxDistance);
}

/* private */
bool
SimpleSTRdistance::isWithinDistance(SimpleSTRpair* p_initPair, double maxDistance)
{
    double distanceUpperBound = std::numeric_limits<double>::infinity();

    // initialize search queue
    STRpairQueue priQ;
    priQ.push(p_initPair);

    while (! priQ.empty()) {
        // pop head of queue and expand one side of pair
        SimpleSTRpair* pair = priQ.top();
        double pairDistance = pair->getDistance();

        /**
        * If the distance for the first pair in the queue
        * is > maxDistance, all other pairs
        * in the queue must have a greater distance as well.
        * So can conclude no items are within the distance
        * and terminate with result = false
        */
        if (pairDistance > maxDistance)
            return false;

        priQ.pop();
        /**
        * If the maximum distance between the nodes
        * is less than the maxDistance,
        * than all items in the nodes must be
        * closer than the max distance.
        * Then can terminate with result = true.
        *
        * NOTE: using Envelope MinMaxDistance
        * would provide a tighter bound,
        * but not much performance improvement has been observed
        */
        if (pair->maximumDistance() <= maxDistance)
            return true;
        /**
        * If the pair items are leaves
        * then their actual distance is an upper bound.
        * Update the distanceUpperBound to reflect this
        */
        if (pair->isLeaves()) {
            // assert: currentDistance < minimumDistanceFound
            distanceUpperBound = pairDistance;
            /**
            * If the items are closer than maxDistance
            * can terminate with result = true.
            */
            if (distanceUpperBound <= maxDistance)
                return true;
        }
        else {
            /**
            * Otherwise, expand one side of the pair,
            * and insert the expanded pairs into the queue.
            * The choice of which side to expand is determined heuristically.
            */
            expandToQueue(pair, priQ, distanceUpperBound);
        }
    }
    return false;
}


/*********************************************************************************/

SimpleSTRnode*
SimpleSTRpair::getNode(int i) const
{
    if (i == 0) {
        return node1;
    }
    return node2;
}


/* private */
double
SimpleSTRpair::distance()
{
    // if items, compute exact distance
    if (isLeaves()) {
        return itemDistance->distance(node1, node2);
    }

    // otherwise compute distance between bounds of nodes
    const geom::Envelope& e1 = node1->getEnvelope();
    const geom::Envelope& e2 = node2->getEnvelope();
    return e1.distance(e2);
}


double
SimpleSTRpair::getDistance() const
{
    return m_distance;
}

bool
SimpleSTRpair::isLeaves() const
{
    return node1->isLeaf() && node2->isLeaf();
}

double
SimpleSTRpair::maximumDistance()
{
    return EnvelopeUtil::maximumDistance(&(node1->getEnvelope()), &(node2->getEnvelope()));
}

/*public static*/
std::ostream&
operator<<(std::ostream& os, SimpleSTRpair& pair)
{
    const geom::Envelope& e1 = pair.getNode(0)->getEnvelope();
    const geom::Envelope& e2 = pair.getNode(1)->getEnvelope();
    double distance = pair.getDistance();

    os << e1 << " " << e2 << " " << distance;
    return os;
}


} // namespace geos.index.strtree
} // namespace geos.index
} // namespace geos


