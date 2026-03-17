/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/SubgraphDepthLocater.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <vector>
#include <cassert>
#include <algorithm>

#include <geos/operation/buffer/BufferSubgraph.h>
#include <geos/operation/buffer/SubgraphDepthLocater.h>

#include <geos/algorithm/Orientation.h>

#include <geos/geom/Envelope.h>
#include <geos/geom/CoordinateSequence.h>

#include <geos/geomgraph/DirectedEdge.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geom/Position.h>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

using namespace std;
using namespace geos::geomgraph;
using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/*
 * A segment from a directed edge which has been assigned a depth value
 * for its sides.
 */
class DepthSegment {

private:

    geom::LineSegment upwardSeg;

    /*
     * Compare two collinear segments for left-most ordering.
     * If segs are vertical, use vertical ordering for comparison.
     * If segs are equal, return 0.
     * Segments are assumed to be directed so that the second
     * coordinate is >= to the first
     * (e.g. up and to the right).
     *
     * @param seg0 a segment to compare
     * @param seg1 a segment to compare
     * @return
     */
    static int
    compareX(const geom::LineSegment* seg0, const geom::LineSegment* seg1)
    {
        int compare0 = seg0->p0.compareTo(seg1->p0);
        if(compare0 != 0) {
            return compare0;
        }
        return seg0->p1.compareTo(seg1->p1);
    }

public:

    int leftDepth;

    /// @param seg will be copied to private space
    DepthSegment(const geom::LineSegment& seg, int depth)
        :
        upwardSeg(seg),
        leftDepth(depth)
    {
        // input seg is assumed to be normalized
        //upwardSeg.normalize();
    }

    /**
     * Defines a comparision operation on DepthSegments
     * which orders them left to right
     *
     * <pre>
     * DS1 < DS2   if   DS1.seg is left of DS2.seg
     * DS1 > DS2   if   DS1.seg is right of DS2.seg
     * </pre>
     *
     * @param other
     * @return
     */
    int
    compareTo(const DepthSegment& other) const
    {
        /*
         * try and compute a determinate orientation for the segments.
         * Test returns 1 if other is left of this (i.e. this > other)
         */
        int orientIndex = upwardSeg.orientationIndex(&(other.upwardSeg));

        /*
         * If comparison between this and other is indeterminate,
         * try the opposite call order.
         * orientationIndex value is 1 if this is left of other,
         * so have to flip sign to get proper comparison value of
         * -1 if this is leftmost
         */
        if(orientIndex == 0) {
            orientIndex = -1 * other.upwardSeg.orientationIndex(&upwardSeg);
        }

        // if orientation is determinate, return it
        if(orientIndex != 0) {
            return orientIndex;
        }

        // otherwise, segs must be collinear - sort based on minimum X value
        return compareX(&upwardSeg, &(other.upwardSeg));
    }
};

struct DepthSegmentLessThen {
    bool
    operator()(const DepthSegment* first, const DepthSegment* second)
    {
        assert(first);
        assert(second);
        if(first->compareTo(*second) < 0) {
            return true;
        }
        else {
            return false;
        }
    }
};



/*public*/
int
SubgraphDepthLocater::getDepth(const Coordinate& p)
{
    vector<DepthSegment*> stabbedSegments;
    findStabbedSegments(p, stabbedSegments);

    // if no segments on stabbing line subgraph must be outside all others
    if(stabbedSegments.empty()) {
        return 0;
    }

    DepthSegment *ds = *std::min_element(stabbedSegments.begin(),
        stabbedSegments.end(), DepthSegmentLessThen());
    int ret = ds->leftDepth;

#if GEOS_DEBUG
    cerr << "SubgraphDepthLocater::getDepth(" << p.toString() << "): " << ret << endl;
#endif

    for(vector<DepthSegment*>::iterator
            it = stabbedSegments.begin(), itEnd = stabbedSegments.end();
            it != itEnd;
            ++it) {
        delete *it;
    }

    return ret;
}

/*private*/
void
SubgraphDepthLocater::findStabbedSegments(const Coordinate& stabbingRayLeftPt,
        std::vector<DepthSegment*>& stabbedSegments)
{
    size_t size = subgraphs->size();
    for(size_t i = 0; i < size; ++i) {
        BufferSubgraph* bsg = (*subgraphs)[i];

        // optimization - don't bother checking subgraphs
        // which the ray does not intersect
        Envelope* env = bsg->getEnvelope();
        if(stabbingRayLeftPt.y < env->getMinY()
                || stabbingRayLeftPt.y > env->getMaxY()
                || stabbingRayLeftPt.x < env->getMinX()
                || stabbingRayLeftPt.x > env->getMaxX()) {
            continue;
        }

        findStabbedSegments(stabbingRayLeftPt, bsg->getDirectedEdges(),
                            stabbedSegments);
    }
}

/*private*/
void
SubgraphDepthLocater::findStabbedSegments(
    const Coordinate& stabbingRayLeftPt,
    vector<DirectedEdge*>* dirEdges,
    vector<DepthSegment*>& stabbedSegments)
{

    /*
     * Check all forward DirectedEdges only. This is still general,
     * because each Edge has a forward DirectedEdge.
     */
    for(size_t i = 0, n = dirEdges->size(); i < n; ++i) {
        DirectedEdge* de = (*dirEdges)[i];
        if(!de->isForward()) {
            continue;
        }
        findStabbedSegments(stabbingRayLeftPt, de, stabbedSegments);
    }
}

/*private*/
void
SubgraphDepthLocater::findStabbedSegments(
    const Coordinate& stabbingRayLeftPt,
    DirectedEdge* dirEdge,
    vector<DepthSegment*>& stabbedSegments)
{
    const CoordinateSequence* pts = dirEdge->getEdge()->getCoordinates();

// It seems that LineSegment is *very* slow... undef this
// to see yourself
// LineSegment has been refactored to be mostly inline, still
// it makes copies of the given coordinates, while the 'non-LineSemgent'
// based code below uses pointers instead. I'll kip the SKIP_LS
// defined until LineSegment switches to Coordinate pointers instead.
//
#define SKIP_LS 1

    auto n = pts->getSize() - 1;
    for(size_t i = 0; i < n; ++i) {
#ifndef SKIP_LS
        seg.p0 = pts->getAt(i);
        seg.p1 = pts->getAt(i + 1);
#if GEOS_DEBUG
        cerr << " SubgraphDepthLocater::findStabbedSegments: segment " << i
             << " (" << seg << ") ";
#endif

#else
        const Coordinate* low = &(pts->getAt(i));
        const Coordinate* high = &(pts->getAt(i + 1));
        const Coordinate* swap = nullptr;

#endif

#ifndef SKIP_LS
        // ensure segment always points upwards
        //if (seg.p0.y > seg.p1.y)
        {
            seg.reverse();
#if GEOS_DEBUG
            cerr << " reverse (" << seg << ") ";
#endif
        }
#else
        if(low->y > high->y) {
            swap = low;
            low = high;
            high = swap;
        }
#endif

        // skip segment if it is left of the stabbing line
        // skip if segment is above or below stabbing line
#ifndef SKIP_LS
        double maxx = max(seg.p0.x, seg.p1.x);
#else
        double maxx = max(low->x, high->x);
#endif
        if(maxx < stabbingRayLeftPt.x) {
#if GEOS_DEBUG
            cerr << " segment is left to stabbing line, skipping " << endl;
#endif
            continue;
        }

        // skip horizontal segments (there will be a non-horizontal
        // one carrying the same depth info
#ifndef SKIP_LS
        if(seg.isHorizontal())
#else
        if(low->y == high->y)
#endif
        {
#if GEOS_DEBUG
            cerr << " segment is horizontal, skipping " << endl;
#endif
            continue;
        }

        // skip if segment is above or below stabbing line
#ifndef SKIP_LS
        if(stabbingRayLeftPt.y < seg.p0.y ||
                stabbingRayLeftPt.y > seg.p1.y)
#else
        if(stabbingRayLeftPt.y < low->y ||
                stabbingRayLeftPt.y > high->y)
#endif
        {
#if GEOS_DEBUG
            cerr << " segment above or below stabbing line, skipping " << endl;
#endif
            continue;
        }

        // skip if stabbing ray is right of the segment
#ifndef SKIP_LS
        if(Orientation::index(seg.p0, seg.p1,
#else
        if(Orientation::index(*low, *high,
#endif
                              stabbingRayLeftPt) == Orientation::RIGHT) {
#if GEOS_DEBUG
            cerr << " stabbing ray right of segment, skipping" << endl;
#endif
            continue;
        }

#ifndef SKIP_LS
        // stabbing line cuts this segment, so record it
        int depth = dirEdge->getDepth(Position::LEFT);
        // if segment direction was flipped, use RHS depth instead
        if(!(seg.p0 == pts->getAt(i)))
            depth = dirEdge->getDepth(Position::RIGHT);
#else
        int depth = swap ?
                    dirEdge->getDepth(Position::RIGHT)
                    :
                    dirEdge->getDepth(Position::LEFT);
#endif

#if GEOS_DEBUG
        cerr << " depth: " << depth << endl;
#endif

#ifdef SKIP_LS
        seg.p0 = *low;
        seg.p1 = *high;
#endif

        DepthSegment* ds = new DepthSegment(seg, depth);
        stabbedSegments.push_back(ds);
    }
}

} // namespace geos.operation.buffer
} // namespace geos.operation
} // namespace geos
