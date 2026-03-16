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
 **********************************************************************/

#include <geos/noding/MCIndexNoder.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/index/kdtree/KdTree.h>
#include <geos/index/kdtree/KdNode.h>
#include <geos/index/kdtree/KdNodeVisitor.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/NodedSegmentString.h>
#include <geos/noding/snapround/SnapRoundingNoder.h>
#include <geos/noding/snapround/SnapRoundingIntersectionAdder.h>

#include <algorithm> // for std::min and std::max
#include <memory>

using namespace geos::geom;
using namespace geos::index::kdtree;

namespace geos {
namespace noding { // geos.noding
namespace snapround { // geos.noding.snapround


/*public*/
std::vector<SegmentString*>*
SnapRoundingNoder::getNodedSubstrings() const
{
    std::vector<SegmentString*>* nssResult = NodedSegmentString::getNodedSubstrings(snappedResult);

    // Intermediate SegmentStrings are no longer needed
    for (auto nss: snappedResult)
        delete nss;

    return nssResult;
}

/*public*/
void
SnapRoundingNoder::computeNodes(std::vector<SegmentString*>* inputSegStrings)
{
    snapRound(*inputSegStrings, snappedResult);
    return;
}

/*private*/
void
SnapRoundingNoder::snapRound(std::vector<SegmentString*>& inputSegStrings, std::vector<SegmentString*>& resultNodedSegments)
{
    /**
    * Determine hot pixels for intersections and vertices.
    * This is done BEFORE the input lines are rounded,
    * to avoid distorting the line arrangement
    * (rounding can cause vertices to move across edges).
    */
    addIntersectionPixels(inputSegStrings);
    addVertexPixels(inputSegStrings);

    computeSnaps(inputSegStrings, resultNodedSegments);
    return;
}

/*private*/
void
SnapRoundingNoder::addIntersectionPixels(std::vector<SegmentString*>& segStrings)
{
    SnapRoundingIntersectionAdder intAdder(pm);
    MCIndexNoder noder;
    noder.setSegmentIntersector(&intAdder);
    noder.computeNodes(&segStrings);
    std::unique_ptr<std::vector<Coordinate>> intPts = intAdder.getIntersections();
    pixelIndex.addNodes(*intPts);
}

/*private void*/
void
SnapRoundingNoder::addVertexPixels(std::vector<SegmentString*>& segStrings)
{
    for (SegmentString* nss : segStrings) {
        const CoordinateSequence* pts = nss->getCoordinates();
        pixelIndex.add(pts);
    }
}

/*private*/
void
SnapRoundingNoder::round(const Coordinate& pt, Coordinate& ptOut)
{
    ptOut = pt;
    pm->makePrecise(ptOut);
    return;
}

/*private*/
std::unique_ptr<std::vector<Coordinate>>
SnapRoundingNoder::round(const std::vector<Coordinate>& pts)
{
    std::unique_ptr<std::vector<Coordinate>> roundPts(new std::vector<Coordinate>);
    roundPts->reserve(pts.size());
    for (auto pt: pts) {
        Coordinate ptOut;
        round(pt, ptOut);
        roundPts->push_back(ptOut);
    }
    roundPts->erase(std::unique(roundPts->begin(), roundPts->end()), roundPts->end());
    return roundPts;
}

/*private*/
void
SnapRoundingNoder::computeSnaps(const std::vector<SegmentString*>& segStrings, std::vector<SegmentString*>& snapped)
{
    for (SegmentString* ss: segStrings) {
        NodedSegmentString* snappedSS = computeSegmentSnaps(detail::down_cast<NodedSegmentString*>(ss));
        if (snappedSS != nullptr) {
            /**
             * Some intersection hot pixels may have been marked as nodes in the previous
             * loop, so add nodes for them.
             */
            snapped.push_back(snappedSS);
        }
    }
    for (SegmentString* ss: snapped) {
        NodedSegmentString* nss = detail::down_cast<NodedSegmentString*>(ss);
        addVertexNodeSnaps(nss);
    }
    return;
}

/**
* Add snapped vertices to a segment string.
* If the segment string collapses completely due to rounding,
* null is returned.
*
* @param ss the segment string to snap
* @return the snapped segment string, or null if it collapses completely
*/
/*private*/
NodedSegmentString*
SnapRoundingNoder::computeSegmentSnaps(NodedSegmentString* ss)
{
    /**
    * Get edge coordinates, including added intersection nodes.
    * The coordinates are now rounded to the grid,
    * in preparation for snapping to the Hot Pixels
    */
    std::unique_ptr<std::vector<Coordinate>> pts = ss->getNodedCoordinates();
    std::unique_ptr<std::vector<Coordinate>> ptsRoundVec = round(*pts);
    std::unique_ptr<geom::CoordinateArraySequence> ptsRound(new CoordinateArraySequence(ptsRoundVec.release()));

    // if complete collapse this edge can be eliminated
    if (ptsRound->size() <= 1)
        return nullptr;

    // Create new nodedSS to allow adding any hot pixel nodes
    NodedSegmentString* snapSS = new NodedSegmentString(ptsRound.release(), ss->getData());

    size_t snapSSindex = 0;
    for (size_t i = 0, sz = pts->size()-1; i < sz; i++ ) {

        const geom::Coordinate& currSnap = snapSS->getCoordinate(snapSSindex);

        /**
        * If the segment has collapsed completely, skip it
        */
        Coordinate p1 = (*pts)[i+1];
        Coordinate p1Round;
        round(p1, p1Round);
        if (p1Round.equals2D(currSnap))
            continue;

        Coordinate p0 = (*pts)[i];

        /**
        * Add any Hot Pixel intersections with *original* segment to rounded segment.
        * (It is important to check original segment because rounding can
        * move it enough to intersect other hot pixels not intersecting original segment)
        */
        snapSegment(p0, p1, snapSS, snapSSindex);
        snapSSindex++;
    }
    return snapSS;
}

/**
* Snaps a segment in a segmentString to HotPixels that it intersects.
*
* @param p0 the segment start coordinate
* @param p1 the segment end coordinate
* @param ss the segment string to add intersections to
* @param segIndex the index of the segment
*/
/*private*/
void
SnapRoundingNoder::snapSegment(Coordinate& p0, Coordinate& p1, NodedSegmentString* ss, size_t segIndex)
{
    /* First define a visitor to use in the pixelIndex.query() */
    struct SnapRoundingVisitor : KdNodeVisitor {
        const Coordinate& p0;
        const Coordinate& p1;
        NodedSegmentString* ss;
        size_t segIndex;

        SnapRoundingVisitor(const Coordinate& pp0, const Coordinate& pp1, NodedSegmentString* pss, size_t psegIndex)
            : p0(pp0), p1(pp1), ss(pss), segIndex(psegIndex) {};

        void visit(KdNode* node) override {
            HotPixel* hp = static_cast<HotPixel*>(node->getData());
            /**
            * If the hot pixel is not a node, and it contains one of the segment vertices,
            * then that vertex is the source for the hot pixel.
            * To avoid over-noding a node is not added at this point.
            * The hot pixel may be subsequently marked as a node,
            * in which case the intersection will be added during the final vertex noding phase.
            */
            if (! hp->isNode()) {
                if (hp->intersects(p0) || hp->intersects(p1)) {
                    return;
                }
            }
            /**
            * Add a node if the segment intersects the pixel.
            * Mark the HotPixel as a node (since it may not have been one before).
            * This ensures the vertex for it is added as a node during the final vertex noding phase.
            */
            if (hp->intersects(p0, p1)) {
                ss->addIntersection(hp->getCoordinate(), segIndex);
                hp->setToNode();
            }
        }
    };

    /* Then run the query with the visitor */
    SnapRoundingVisitor srv(p0, p1, ss, segIndex);
    pixelIndex.query(p0, p1, srv);
}


/*private*/
void
SnapRoundingNoder::addVertexNodeSnaps(NodedSegmentString* ss)
{
    const CoordinateSequence* pts = ss->getCoordinates();
    for (std::size_t i = 1; i < pts->size() - 1; i++) {
        const Coordinate& p0 = pts->getAt(i);
        snapVertexNode(p0, ss, i);
    }
}

void
SnapRoundingNoder::snapVertexNode(const Coordinate& p0, NodedSegmentString* ss, size_t segIndex)
{

    /* First define a visitor to use in the pixelIndex.query() */
    struct SnapRoundingVertexNodeVisitor : KdNodeVisitor {

        const Coordinate& p0;
        NodedSegmentString* ss;
        size_t segIndex;

        SnapRoundingVertexNodeVisitor(const Coordinate& pp0, NodedSegmentString* pss, size_t psegIndex)
            : p0(pp0), ss(pss), segIndex(psegIndex) {};

        void visit(KdNode* node) override {
            HotPixel* hp = static_cast<HotPixel*>(node->getData());

            /**
            * If vertex pixel is a node, add it.
            */
            if (hp->isNode() && hp->getCoordinate().equals2D(p0)) {
                ss->addIntersection(p0, segIndex);
            }
        }
    };

    /* Then run the query with the visitor */
    SnapRoundingVertexNodeVisitor srv(p0, ss, segIndex);
    pixelIndex.query(p0, p0, srv);
}





} // namespace geos.noding.snapround
} // namespace geos.noding
} // namespace geos
