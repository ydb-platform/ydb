/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: simplify/TaggedLineStringSimplifier.java r536 (JTS-1.12+)
 *
 **********************************************************************/

#include <geos/simplify/TaggedLineStringSimplifier.h>
#include <geos/simplify/LineSegmentIndex.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/simplify/TaggedLineString.h>
#include <geos/simplify/TaggedLineSegment.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/LineString.h>
//#include <geos/geom/Geometry.h> // for unique_ptr destructor
//#include <geos/geom/GeometryFactory.h>
//#include <geos/geom/CoordinateSequenceFactory.h>

#include <algorithm>
#include <cassert>
#include <memory>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

using namespace geos::geom;
using std::pair;
using std::unique_ptr;
using std::vector;

namespace geos {
namespace simplify { // geos::simplify

/*public*/
TaggedLineStringSimplifier::TaggedLineStringSimplifier(
    LineSegmentIndex* nInputIndex,
    LineSegmentIndex* nOutputIndex)
    :
    inputIndex(nInputIndex),
    outputIndex(nOutputIndex),
    li(new algorithm::LineIntersector()),
    line(nullptr),
    linePts(nullptr),
    distanceTolerance(0.0)
{
}

/*public*/
void
TaggedLineStringSimplifier::simplify(TaggedLineString* nLine)
{
    assert(nLine);
    line = nLine;

    linePts = line->getParentCoordinates();
    assert(linePts);

#if GEOS_DEBUG
    std::cerr << "TaggedLineStringSimplifier[" << this << "] "
              << " TaggedLineString[" << line << "] "
              << " has " << linePts->size() << " coords in input"
              << std::endl;
#endif

    if(linePts->isEmpty()) {
        return;
    }
    simplifySection(0, linePts->size() - 1, 0);

}


/*private*/
void
TaggedLineStringSimplifier::simplifySection(std::size_t i,
        std::size_t j, std::size_t depth)
{
    depth += 1;

#if GEOS_DEBUG
    std::cerr << "TaggedLineStringSimplifier[" << this << "] "
              << " simplifying section " << i << "-" << j
              << std::endl;
#endif

    if((i + 1) == j) {

#if GEOS_DEBUG
        std::cerr << "single segment, no flattening"
                  << std::endl;
#endif
        unique_ptr<TaggedLineSegment> newSeg(new
                                             TaggedLineSegment(*(line->getSegment(i))));

        line->addToResult(std::move(newSeg));
        // leave this segment in the input index, for efficiency
        return;
    }

    bool isValidToSimplify = true;

    /*
     * Following logic ensures that there is enough points in the
     * output line.
     * If there is already more points than the minimum, there's
     * nothing to check.
     * Otherwise, if in the worst case there wouldn't be enough points,
     * don't flatten this segment (which avoids the worst case scenario)
     */
    if(line->getResultSize() < line->getMinimumSize()) {
        std::size_t worstCaseSize = depth + 1;
        if(worstCaseSize < line->getMinimumSize()) {
            isValidToSimplify = false;
        }
    }

    double distance;

    // pass distance by ref
    std::size_t furthestPtIndex = findFurthestPoint(linePts, i, j, distance);

#if GEOS_DEBUG
    std::cerr << "furthest point " << furthestPtIndex
              << " at distance " << distance
              << std::endl;
#endif

    // flattening must be less than distanceTolerance
    if(distance > distanceTolerance) {
        isValidToSimplify = false;
    }

    // test if flattened section would cause intersection
    LineSegment candidateSeg(linePts->getAt(i), linePts->getAt(j));

    if(hasBadIntersection(line, std::make_pair(i, j), candidateSeg)) {
        isValidToSimplify = false;
    }

    if(isValidToSimplify) {

        unique_ptr<TaggedLineSegment> newSeg = flatten(i, j);

#if GEOS_DEBUG
        std::cerr << "isValidToSimplify, adding seg "
                  << newSeg->p0 << ", " << newSeg->p1
                  << " to TaggedLineSegment[" << line << "] result "
                  << std::endl;
#endif

        line->addToResult(std::move(newSeg));
        return;
    }

    simplifySection(i, furthestPtIndex, depth);
    simplifySection(furthestPtIndex, j, depth);

}


/*private*/
unique_ptr<TaggedLineSegment>
TaggedLineStringSimplifier::flatten(std::size_t start, std::size_t end)
{
    // make a new segment for the simplified geometry
    const Coordinate& p0 = linePts->getAt(start);
    const Coordinate& p1 = linePts->getAt(end);
    unique_ptr<TaggedLineSegment> newSeg(new TaggedLineSegment(p0, p1));
    // update the indexes
    remove(line, start, end);
    outputIndex->add(newSeg.get());
    return newSeg;
}

/*private*/
bool
TaggedLineStringSimplifier::hasBadIntersection(
    const TaggedLineString* parentLine,
    const pair<size_t, size_t>& sectionIndex,
    const LineSegment& candidateSeg)
{
    if(hasBadOutputIntersection(candidateSeg)) {
        return true;
    }

    if(hasBadInputIntersection(parentLine, sectionIndex, candidateSeg)) {
        return true;
    }

    return false;
}

/*private*/
bool
TaggedLineStringSimplifier::hasBadOutputIntersection(
    const LineSegment& candidateSeg)
{
    unique_ptr< vector<LineSegment*> > querySegs =
        outputIndex->query(&candidateSeg);

    for(const LineSegment* querySeg : *querySegs) {
        if(hasInteriorIntersection(*querySeg, candidateSeg)) {
            return true;
        }
    }

    return false;
}

/*private*/
bool
TaggedLineStringSimplifier::hasInteriorIntersection(
    const LineSegment& seg0,
    const LineSegment& seg1) const
{
    li->computeIntersection(seg0.p0, seg0.p1, seg1.p0, seg1.p1);
    return li->isInteriorIntersection();
}

/*private*/
bool
TaggedLineStringSimplifier::hasBadInputIntersection(
    const TaggedLineString* parentLine,
    const pair<std::size_t, std::size_t>& sectionIndex,
    const LineSegment& candidateSeg)
{
    unique_ptr< vector<LineSegment*> > querySegs =
        inputIndex->query(&candidateSeg);

    for(const LineSegment* ls : *querySegs) {
        const TaggedLineSegment* querySeg = static_cast<const TaggedLineSegment*>(ls);

        if(!isInLineSection(parentLine, sectionIndex, querySeg) && hasInteriorIntersection(*querySeg, candidateSeg)) {

            return true;
        }
    }

    return false;
}

/*static private*/
bool
TaggedLineStringSimplifier::isInLineSection(
    const TaggedLineString* line,
    const pair<size_t, size_t>& sectionIndex,
    const TaggedLineSegment* seg)
{
    // not in this line
    if(seg->getParent() != line->getParent()) {
        return false;
    }

    std::size_t segIndex = seg->getIndex();
    if(segIndex >= sectionIndex.first && segIndex < sectionIndex.second) {
        return true;
    }

    return false;
}

/*private*/
void
TaggedLineStringSimplifier::remove(const TaggedLineString* p_line,
                                   std::size_t start,
                                   std::size_t end)
{
    assert(end <= p_line->getSegments().size());
    assert(start < end); // I'm not sure this should always be true

    for(std::size_t i = start; i < end; i++) {
        const TaggedLineSegment* seg = p_line->getSegment(i);
        inputIndex->remove(seg);
    }
}

/*private static*/
std::size_t
TaggedLineStringSimplifier::findFurthestPoint(
    const geom::CoordinateSequence* pts,
    std::size_t i, std::size_t j,
    double& maxDistance)
{
    LineSegment seg(pts->getAt(i), pts->getAt(j));
#if GEOS_DEBUG
    std::cerr << __FUNCTION__ << "segment " << seg
              << std::endl;
#endif
    double maxDist = -1.0;
    std::size_t maxIndex = i;
    for(std::size_t k = i + 1; k < j; k++) {
        const Coordinate& midPt = pts->getAt(k);
        double distance = seg.distance(midPt);
#if GEOS_DEBUG
        std::cerr << "dist to " << midPt
                  << ": " << distance
                  << std::endl;
#endif
        if(distance > maxDist) {
#if GEOS_DEBUG
            std::cerr << "this is max"
                      << std::endl;
#endif
            maxDist = distance;
            maxIndex = k;
        }
    }
    maxDistance = maxDist;
    return maxIndex;

}

} // namespace geos::simplify
} // namespace geos
