/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
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
 * Last port: index/strtree/STRtree.java rev. 1.11
 *
 **********************************************************************/

#include <geos/index/strtree/STRtree.h>
#include <geos/index/strtree/BoundablePair.h>
#include <geos/geom/Envelope.h>

#include <vector>
#include <cassert>
#include <cmath>
#include <algorithm> // std::sort
#include <iostream> // for debugging
#include <limits>
#include <geos/util/GEOSException.h>

using namespace std;
using namespace geos::geom;

namespace geos {
namespace index { // geos.index
namespace strtree { // geos.index.strtree


/*public*/
STRtree::STRtree(size_t p_nodeCapacity): AbstractSTRtree(p_nodeCapacity)
{
}

bool
STRtree::STRIntersectsOp::intersects(const void* aBounds, const void* bBounds)
{
    return ((Envelope*)aBounds)->intersects((Envelope*)bBounds);
}

/*private*/
std::unique_ptr<BoundableList>
STRtree::createParentBoundables(BoundableList* childBoundables, int newLevel)
{
    assert(!childBoundables->empty());
    int minLeafCount = (int) ceil((double)childBoundables->size() / (double)getNodeCapacity());

    std::unique_ptr<BoundableList> sortedChildBoundables(sortBoundablesX(childBoundables));

    std::unique_ptr< vector<BoundableList*> > verticalSlicesV(
        verticalSlices(sortedChildBoundables.get(), (int)ceil(sqrt((double)minLeafCount)))
    );

    std::unique_ptr<BoundableList> ret(
        createParentBoundablesFromVerticalSlices(verticalSlicesV.get(), newLevel)
    );
    for(size_t i = 0, vssize = verticalSlicesV->size(); i < vssize; ++i) {
        BoundableList* inner = (*verticalSlicesV)[i];
        delete inner;
    }

    return ret;
}

/*private*/
std::unique_ptr<BoundableList>
STRtree::createParentBoundablesFromVerticalSlices(std::vector<BoundableList*>* p_verticalSlices, int newLevel)
{
    assert(!p_verticalSlices->empty());
    std::unique_ptr<BoundableList> parentBoundables(new BoundableList());

    for(size_t i = 0, vssize = p_verticalSlices->size(); i < vssize; ++i) {
        std::unique_ptr<BoundableList> toAdd(
            createParentBoundablesFromVerticalSlice(
                (*p_verticalSlices)[i], newLevel)
        );
        assert(!toAdd->empty());

        parentBoundables->insert(
            parentBoundables->end(),
            toAdd->begin(),
            toAdd->end());
    }
    return parentBoundables;
}

/*protected*/
std::unique_ptr<BoundableList>
STRtree::createParentBoundablesFromVerticalSlice(BoundableList* childBoundables, int newLevel)
{
    return AbstractSTRtree::createParentBoundables(childBoundables, newLevel);
}

/*private*/
std::vector<BoundableList*>*
STRtree::verticalSlices(BoundableList* childBoundables, size_t sliceCount)
{
    size_t sliceCapacity = (size_t) ceil((double)childBoundables->size() / (double) sliceCount);
    vector<BoundableList*>* slices = new vector<BoundableList*>(sliceCount);

    size_t i = 0, nchilds = childBoundables->size();

    for(size_t j = 0; j < sliceCount; j++) {
        (*slices)[j] = new BoundableList();
        (*slices)[j]->reserve(sliceCapacity);
        size_t boundablesAddedToSlice = 0;
        while(i < nchilds && boundablesAddedToSlice < sliceCapacity) {
            Boundable* childBoundable = (*childBoundables)[i];
            ++i;
            (*slices)[j]->push_back(childBoundable);
            ++boundablesAddedToSlice;
        }
    }
    return slices;
}

/*public*/
std::pair<const void*, const void*>
STRtree::nearestNeighbour(ItemDistance* itemDist)
{
    BoundablePair bp(this->getRoot(), this->getRoot(), itemDist);
    return nearestNeighbour(&bp);
}

/*public*/
const void*
STRtree::nearestNeighbour(const Envelope* env, const void* item, ItemDistance* itemDist)
{
    build();

    ItemBoundable bnd = ItemBoundable(env, (void*) item);
    BoundablePair bp(getRoot(), &bnd, itemDist);

    return nearestNeighbour(&bp).first;
}

/*public*/
std::pair<const void*, const void*>
STRtree::nearestNeighbour(STRtree* tree, ItemDistance* itemDist)
{
    BoundablePair bp(getRoot(), tree->getRoot(), itemDist);
    return nearestNeighbour(&bp);
}

/*public*/
std::pair<const void*, const void*>
STRtree::nearestNeighbour(BoundablePair* initBndPair)
{
    return nearestNeighbour(initBndPair, std::numeric_limits<double>::infinity());
}

/*public*/
std::pair<const void*, const void*>
STRtree::nearestNeighbour(BoundablePair* initBndPair, double maxDistance)
{
    double distanceLowerBound = maxDistance;
    BoundablePair* minPair = nullptr;

    BoundablePair::BoundablePairQueue priQ;
    priQ.push(initBndPair);

    while(!priQ.empty() && distanceLowerBound > 0.0) {
        BoundablePair* bndPair = priQ.top();
        double currentDistance = bndPair->getDistance();

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
        if(bndPair->isLeaves()) {
            distanceLowerBound = currentDistance;
            minPair = bndPair;
        }
        else {
            /*
             * Otherwise, expand one side of the pair,
             * (the choice of which side to expand is heuristically determined)
             * and insert the new expanded pairs into the queue
             */
            bndPair->expandToQueue(priQ, distanceLowerBound);
        }

        if(bndPair != initBndPair && bndPair != minPair) {
            delete bndPair;
        }
    }

    /* Free any remaining BoundablePairs in the queue */
    while(!priQ.empty()) {
        BoundablePair* bndPair = priQ.top();
        priQ.pop();
        if(bndPair != initBndPair) {
            delete bndPair;
        }
    }

    if(!minPair) {
        throw util::GEOSException("Error computing nearest neighbor");
    }

    const void* item0 = dynamic_cast<const ItemBoundable*>(minPair->getBoundable(0))->getItem();
    const void* item1 = dynamic_cast<const ItemBoundable*>(minPair->getBoundable(1))->getItem();
    if(minPair != initBndPair) {
        delete minPair;
    }

    return std::pair<const void*, const void*>(item0, item1);
}

/*public*/
bool
STRtree::isWithinDistance(STRtree* tree, ItemDistance* itemDist, double maxDistance)
{
    BoundablePair bp(getRoot(), tree->getRoot(), itemDist);
    return isWithinDistance(&bp, maxDistance);
}

/*private*/
bool STRtree::isWithinDistance(BoundablePair* initBndPair, double maxDistance)
{
    double distanceUpperBound = std::numeric_limits<double>::infinity();

    // initialize search queue
    BoundablePair::BoundablePairQueue priQ;
    priQ.push(initBndPair);

    while(!priQ.empty()) {
        BoundablePair* bndPair = priQ.top();
        double currentDistance = bndPair->getDistance();

        /*
         * If the distance for the first pair in the queue
         * is >= maxDistance, other pairs
         * in the queue must also have a greater distance.
         * So can conclude no items are within the distance
         * and terminate with false
         */
        if (currentDistance > maxDistance)
            return false;

        /*
         * There must be some pair of items in the nodes which
         * are closer than the max distance,
         * so can terminate with true.
         *
         * NOTE: using the Envelope MinMaxDistance would provide a tighter bound,
         * but not sure how to compute this!
         */
        if (bndPair->maximumDistance() <= maxDistance)
            return true;

        /*
         * If the pair members are leaves
         * then their distance is an upper bound.
         * Update the distanceUpperBound to reflect this
         */
        if (bndPair->isLeaves()) {
            distanceUpperBound = currentDistance;

            // Current pair is closer than maxDistance
            // so can terminate with true
            if (distanceUpperBound <= maxDistance)
                return true;
        }
        else {
            /*
             * Otherwise, expand one side of the pair,
             * and insert the expanded pairs into the queue.
             * The choice of which side to expand is determined heuristically.
             */
            bndPair->expandToQueue(priQ, distanceUpperBound);
        }
    }
    return false;
}


class STRAbstractNode: public AbstractNode {
public:

    STRAbstractNode(int p_level, size_t capacity)
        :
        AbstractNode(p_level, capacity)
    {}

    ~STRAbstractNode() override
    {
        delete(Envelope*)bounds;
    }

protected:

    void*
    computeBounds() const override
    {
        Envelope* p_bounds = nullptr;
        const BoundableList& b = *getChildBoundables();

        if(b.empty()) {
            return nullptr;
        }

        BoundableList::const_iterator i = b.begin();
        BoundableList::const_iterator e = b.end();

        p_bounds = new Envelope(* static_cast<const Envelope*>((*i)->getBounds()));
        for(; i != e; ++i) {
            const Boundable* childBoundable = *i;
            p_bounds->expandToInclude((Envelope*)childBoundable->getBounds());
        }
        return p_bounds;
    }

};

/*protected*/
AbstractNode*
STRtree::createNode(int level)
{
    AbstractNode* an = new STRAbstractNode(level, nodeCapacity);
    nodes->push_back(an);
    return an;
}

/*public*/
void
STRtree::insert(const Envelope* itemEnv, void* item)
{
    if(itemEnv->isNull()) {
        return;
    }
    AbstractSTRtree::insert(itemEnv, item);
}

// HISTORICAL NOTES - pramsey - 2020-11-17
// http://trac.osgeo.org/geos/ticket/293
// 10 years ago, a bug in MinGW resulted in a simple < comparison
// not returning an expected result, and a lot of fiddling
// with alternatives. I have returned to simpler logic
// now but am retaining the comment history for posterity.

// NOTE - mloskot:
// The problem of instability is directly related to mathematical definition of
// "strict weak ordering" as a fundamental requirement for binary predicate:
//
// if a is less than b then b is not less than a,
// if a is less than b and b is less than c
// then a is less than c,
// and so on.
//
// For some very low values, this predicate does not fulfill this requiremnet,

// NOTE - strk:
// It seems that the '<' comparison here gives unstable results.
// In particular, when inlines are on (for Envelope::getMinY and getMaxY)
// things are fine, but when they are off we can even get a memory corruption !!
// return STRtree::centreY(aEnv) < STRtree::centreY(bEnv);

// NOTE - mloskot:
// This comparison does not answer if a is "lower" than b
// what is required for sorting. This comparison only answeres
// if a and b are "almost the same" or different

/*NOTE - cfis
  In debug mode VC++ checks the predicate in both directions.

  If !_Pred(_Left, _Right)
  Then an exception is thrown if _Pred(_Right, _Left).
  See xutility around line 320:

    bool __CLRCALL_OR_CDECL _Debug_lt_pred(_Pr _Pred, _Ty1& _Left, _Ty2& _Right,
    const wchar_t *_Where, unsigned int _Line)*/

// return std::fabs( STRtree::centreY(aEnv) - STRtree::centreY(bEnv) ) < 1e-30

// NOTE - strk:
// See http://trac.osgeo.org/geos/ticket/293
// as for why simple comparison (<) isn't used here


/*private*/
std::unique_ptr<BoundableList>
STRtree::sortBoundablesX(const BoundableList* input)
{
    assert(input);
    std::unique_ptr<BoundableList> output(new BoundableList(*input));
    assert(output->size() == input->size());

    struct {
        bool operator()(Boundable* a, Boundable* b) const
        {
            const geom::Envelope* ea = static_cast<const geom::Envelope*>(a->getBounds());
            const geom::Envelope* eb = static_cast<const geom::Envelope*>(b->getBounds());
            double xa = (ea->getMinX() + ea->getMaxX()) / 2.0;
            double xb = (eb->getMinX() + eb->getMaxX()) / 2.0;
            return xa < xb ? true : false;
        }
    } nodeSortByX;

    sort(output->begin(), output->end(), nodeSortByX);
    return output;
}

/*private*/
std::unique_ptr<BoundableList>
STRtree::sortBoundablesY(const BoundableList* input)
{
    assert(input);
    std::unique_ptr<BoundableList> output(new BoundableList(*input));
    assert(output->size() == input->size());

    struct {
        bool operator()(Boundable* a, Boundable* b) const
        {
            const geom::Envelope* ea = static_cast<const geom::Envelope*>(a->getBounds());
            const geom::Envelope* eb = static_cast<const geom::Envelope*>(b->getBounds());
            double ya = (ea->getMinY() + ea->getMaxY()) / 2.0;
            double yb = (eb->getMinY() + eb->getMaxY()) / 2.0;
            return ya < yb ? true : false;
        }
    } nodeSortByY;

    sort(output->begin(), output->end(), nodeSortByY);
    return output;
}

} // namespace geos.index.strtree
} // namespace geos.index
} // namespace geos

