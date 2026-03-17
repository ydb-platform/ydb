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

#include <geos/index/strtree/BoundablePair.h>
#include <geos/index/strtree/EnvelopeUtil.h>
#include <geos/geom/Envelope.h>
#include <geos/index/strtree/AbstractNode.h>
#include <geos/util/IllegalArgumentException.h>

namespace geos {
namespace index {
namespace strtree {

BoundablePair::BoundablePair(const Boundable* p_boundable1, const Boundable* p_boundable2,
                             ItemDistance* p_itemDistance) :
    boundable1(p_boundable1),
    boundable2(p_boundable2),
    itemDistance(p_itemDistance)
{
    mDistance = distance();
}

const Boundable*
BoundablePair::getBoundable(int i) const
{
    if (i == 0) {
        return boundable1;
    }
    return boundable2;
}

double
BoundablePair::distance() const
{
    // if items, compute exact distance
    if (isLeaves()) {
        return itemDistance->distance((ItemBoundable*) boundable1, (ItemBoundable*) boundable2);
    }

    // otherwise compute distance between bounds of boundables
    const geom::Envelope* e1 = (const geom::Envelope*) boundable1->getBounds();
    const geom::Envelope* e2 = (const geom::Envelope*) boundable2->getBounds();

    if (!e1 || !e2) {
        throw util::GEOSException("Can't compute envelope of item in BoundablePair");
    }
    return e1->distance(*e2);
}

double
BoundablePair::getDistance() const
{
    return mDistance;
}

bool
BoundablePair::isLeaves() const
{
    return !(isComposite(boundable1) || isComposite(boundable2));
}

bool
BoundablePair::isComposite(const Boundable* item)
{
    return !(item->isLeaf());
    // return dynamic_cast<const AbstractNode*>(item) != nullptr;
}

double
BoundablePair::area(const Boundable* b)
{
    return ((const geos::geom::Envelope*) b->getBounds())->getArea();
}

void
BoundablePair::expandToQueue(BoundablePairQueue& priQ, double minDistance)
{
    bool isComp1 = isComposite(boundable1);
    bool isComp2 = isComposite(boundable2);

    /*
     * HEURISTIC: If both boundables are composite,
     * choose the one with largest area to expand.
     * Otherwise, simply expand whichever is composite.
     */
    if (isComp1 && isComp2) {
        if (area(boundable1) > area(boundable2)) {
            expand(boundable1, boundable2, false, priQ, minDistance);
            return;
        }
        else {
            expand(boundable2, boundable1, true, priQ, minDistance);
            return;
        }
    }
    else if (isComp1) {
        expand(boundable1, boundable2, false, priQ, minDistance);
        return;
    }
    else if (isComp2) {
        expand(boundable2, boundable1, true, priQ, minDistance);
        return;
    }

    throw geos::util::IllegalArgumentException("neither boundable is composite");
}

void
BoundablePair::expand(const Boundable* bndComposite, const Boundable* bndOther,
                      bool isFlipped, BoundablePairQueue& priQ,
                      double minDistance)
{
    std::vector<Boundable*>* children = ((AbstractNode*) bndComposite)->getChildBoundables();
    for (auto& child : *children) {

        std::unique_ptr<BoundablePair> bp;
        if (isFlipped) {
            bp.reset(new BoundablePair(bndOther, child, itemDistance));
        }
        else {
            bp.reset(new BoundablePair(child, bndOther, itemDistance));
        }

        if (minDistance == std::numeric_limits<double>::infinity() || bp->getDistance() < minDistance) {
            priQ.push(bp.release());
        }

    }
}

double
BoundablePair::maximumDistance()
{
    return EnvelopeUtil::maximumDistance(
        (const geom::Envelope*) boundable1->getBounds(),
        (const geom::Envelope*) boundable2->getBounds());
}



}
}
}

