/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/MCIndexSegmentSetMutualIntersector.java r388 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/noding/MCIndexSegmentSetMutualIntersector.h>
#include <geos/noding/SegmentSetMutualIntersector.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/SegmentIntersector.h>
#include <geos/index/SpatialIndex.h>
#include <geos/index/chain/MonotoneChain.h>
#include <geos/index/chain/MonotoneChainBuilder.h>
#include <geos/index/chain/MonotoneChainOverlapAction.h>
#include <geos/index/strtree/SimpleSTRtree.h>
// std
#include <cstddef>

using namespace geos::index::chain;

namespace geos {
namespace noding { // geos::noding

/*private*/
void
MCIndexSegmentSetMutualIntersector::addToIndex(SegmentString* segStr)
{
    MonoChains segChains;
    MonotoneChainBuilder::getChains(segStr->getCoordinates(),
                                    segStr, segChains);

    MonoChains::size_type n = segChains.size();
    chainStore.reserve(chainStore.size() + n);
    for(auto& mc : segChains) {
        mc->setId(indexCounter++);
        index->insert(&(mc->getEnvelope()), mc.get());
        chainStore.push_back(std::move(mc));
    }
}


/*private*/
void
MCIndexSegmentSetMutualIntersector::intersectChains()
{
    MCIndexSegmentSetMutualIntersector::SegmentOverlapAction overlapAction(*segInt);

    std::vector<void*> overlapChains;
    for(const auto& queryChain : monoChains) {
        overlapChains.clear();

        index->query(&(queryChain->getEnvelope()), overlapChains);

        for(std::size_t j = 0, nj = overlapChains.size(); j < nj; j++) {
            MonotoneChain* testChain = (MonotoneChain*)(overlapChains[j]);

            queryChain->computeOverlaps(testChain, &overlapAction);
            nOverlaps++;
            if(segInt->isDone()) {
                return;
            }
        }
    }
}

/*private*/
void
MCIndexSegmentSetMutualIntersector::addToMonoChains(SegmentString* segStr)
{
    if (segStr->size() == 0)
        return;
    MonoChains segChains;
    MonotoneChainBuilder::getChains(segStr->getCoordinates(),
                                    segStr, segChains);

    MonoChains::size_type n = segChains.size();
    monoChains.reserve(monoChains.size() + n);
    for(auto& mc : segChains) {
        mc->setId(processCounter++);
        monoChains.push_back(std::move(mc));
    }
}

/* public */
MCIndexSegmentSetMutualIntersector::MCIndexSegmentSetMutualIntersector()
    :	monoChains(),
      index(new geos::index::strtree::SimpleSTRtree()),
      indexCounter(0),
      processCounter(0),
      nOverlaps(0)
{
}

/* public */
MCIndexSegmentSetMutualIntersector::~MCIndexSegmentSetMutualIntersector()
{
    delete index;
}

/* public */
void
MCIndexSegmentSetMutualIntersector::setBaseSegments(SegmentString::ConstVect* segStrings)
{
    // NOTE - mloskot: const qualifier is removed silently, dirty.

    for(std::size_t i = 0, n = segStrings->size(); i < n; i++) {
        const SegmentString* css = (*segStrings)[i];
        if (css->size() == 0)
            continue;
        SegmentString* ss = const_cast<SegmentString*>(css);
        addToIndex(ss);
    }
}

/*public*/
void
MCIndexSegmentSetMutualIntersector::process(SegmentString::ConstVect* segStrings)
{
    processCounter = indexCounter + 1;
    nOverlaps = 0;

    monoChains.clear();

    for(SegmentString::ConstVect::size_type i = 0, n = segStrings->size(); i < n; i++) {
        SegmentString* seg = (SegmentString*)((*segStrings)[i]);
        addToMonoChains(seg);
    }
    intersectChains();
}


/* public */
void
MCIndexSegmentSetMutualIntersector::SegmentOverlapAction::overlap(
    MonotoneChain& mc1, size_t start1, MonotoneChain& mc2, size_t start2)
{
    SegmentString* ss1 = (SegmentString*)(mc1.getContext());
    SegmentString* ss2 = (SegmentString*)(mc2.getContext());

    si.processIntersections(ss1, start1, ss2, start2);
}

} // geos::noding
} // geos
