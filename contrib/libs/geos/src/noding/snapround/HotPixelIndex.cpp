/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 *********************************************************************/

#include <geos/noding/snapround/HotPixelIndex.h>
#include <geos/index/kdtree/KdTree.h>
#include <geos/index/ItemVisitor.h>
#include <geos/geom/CoordinateSequence.h>

#include <random>
#include <algorithm> // for std::min and std::max
#include <cassert>
#include <memory>

using namespace geos::algorithm;
using namespace geos::geom;
using geos::index::kdtree::KdTree;
using geos::index::ItemVisitor;

namespace geos {
namespace noding { // geos.noding
namespace snapround { // geos.noding.snapround

/*public*/
HotPixelIndex::HotPixelIndex(const PrecisionModel* p_pm)
    :
    pm(p_pm),
    scaleFactor(p_pm->getScale()),
    index(new KdTree())
{
}


/*public*/
HotPixel*
HotPixelIndex::add(const Coordinate& p)
{
    Coordinate pRound = round(p);
    HotPixel* hp = find(pRound);

    /**
     * Hot Pixels which are added more than once
     * must have more than one vertex in them
     * and thus must be nodes.
     */
    if (hp != nullptr) {
        hp->setToNode();
        return hp;
    }
    /**
     * A pixel containing the point was not found, so create a new one.
     * It is initially set to NOT be a node
     * (but may become one later on).
     */

    // Store the HotPixel in a std::deque to avoid individually
    // allocating a pile of HotPixels on the heap and to
    // get them freed automatically as the std::deque
    // goes away when this HotPixelIndex is deleted.
    hotPixelQue.emplace_back(pRound, scaleFactor);

    // Pick up a pointer to the most recently added
    // HotPixel.
    hp = &(hotPixelQue.back());

    index->insert(hp->getCoordinate(), (void*)hp);
    return hp;
}

/*public*/
void
HotPixelIndex::add(const CoordinateSequence *pts)
{
    /*
    * Add the points to the tree in random order
    * to avoid getting an unbalanced tree from
    * spatially autocorrelated coordinates
    */
    std::vector<std::size_t> idxs;
    for (size_t i = 0, sz = pts->size(); i < sz; i++)
        idxs.push_back(i);

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(idxs.begin(), idxs.end(), g);

    for (auto i : idxs) {
        add(pts->getAt(i));
    }
}

/*public*/
void
HotPixelIndex::add(const std::vector<geom::Coordinate>& pts)
{
    std::vector<std::size_t> idxs;
    for (size_t i = 0, sz = pts.size(); i < sz; i++)
        idxs.push_back(i);

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(idxs.begin(), idxs.end(), g);

    for (auto i : idxs) {
        add(pts[i]);
    }
}

/*public*/
void
HotPixelIndex::addNodes(const CoordinateSequence *pts)
{
    for (size_t i = 0, sz = pts->size(); i < sz; i++) {
        HotPixel* hp = add(pts->getAt(i));
        hp->setToNode();
    }
}

/*public*/
void
HotPixelIndex::addNodes(const std::vector<geom::Coordinate>& pts)
{
    for (auto pt: pts) {
        HotPixel* hp = add(pt);
        hp->setToNode();
    }
}

/*private*/
HotPixel*
HotPixelIndex::find(const geom::Coordinate& pixelPt)
{
    index::kdtree::KdNode *kdNode = index->query(pixelPt);
    if (kdNode == nullptr) {
        return nullptr;
    }
    return (HotPixel*)(kdNode->getData());
}

/*private*/
Coordinate
HotPixelIndex::round(const Coordinate& pt)
{
    Coordinate p2 = pt;
    pm->makePrecise(p2);
    return p2;
}


/*public*/
void
HotPixelIndex::query(const Coordinate& p0, const Coordinate& p1, index::kdtree::KdNodeVisitor& visitor)
{
    Envelope queryEnv(p0, p1);
    queryEnv.expandBy(1.0 / scaleFactor);
    index->query(queryEnv, visitor);
}


} // namespace geos.noding.snapround
} // namespace geos.noding
} // namespace geos



