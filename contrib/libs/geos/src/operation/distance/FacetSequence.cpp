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
 * Last port: operation/distance/FacetSequence.java (f6187ee2 JTS-1.14)
 *
 **********************************************************************/

#include <geos/algorithm/Distance.h>
#include <geos/operation/distance/FacetSequence.h>

#include <memory>

using namespace geos::geom;
using namespace geos::operation::distance;
using namespace geos::algorithm;

FacetSequence::FacetSequence(const Geometry *p_geom, const CoordinateSequence* p_pts, size_t p_start, size_t p_end) :
    pts(p_pts),
    start(p_start),
    end(p_end),
    geom(p_geom)
{
    computeEnvelope();
}

FacetSequence::FacetSequence(const CoordinateSequence* p_pts, size_t p_start, size_t p_end) :
    pts(p_pts),
    start(p_start),
    end(p_end),
    geom(nullptr)
{
    computeEnvelope();
}

size_t
FacetSequence::size() const
{
    return end - start;
}

bool
FacetSequence::isPoint() const
{
    return end - start == 1;
}

double
FacetSequence::distance(const FacetSequence& facetSeq) const
{
    bool isPointThis = isPoint();
    bool isPointOther = facetSeq.isPoint();

    if(isPointThis && isPointOther) {
        const Coordinate& pt = pts->getAt(start);
        const Coordinate& seqPt = facetSeq.pts->getAt(facetSeq.start);
        return pt.distance(seqPt);
    }
    else if(isPointThis) {
        const Coordinate& pt = pts->getAt(start);
        return computeDistancePointLine(pt, facetSeq, nullptr);
    }
    else if(isPointOther) {
        const Coordinate& seqPt = facetSeq.pts->getAt(facetSeq.start);
        return computeDistancePointLine(seqPt, *this, nullptr);
    }
    else {
        return computeDistanceLineLine(facetSeq, nullptr);
    }
}

/*
* Rather than get bent out of shape about returning a pointer
* just return the whole mess, since it only ends up holding two
* locations.
*/
std::vector<GeometryLocation>
FacetSequence::nearestLocations(const FacetSequence& facetSeq)  const
{
    bool isPointThis = isPoint();
    bool isPointOther = facetSeq.isPoint();
    std::vector<GeometryLocation> locs;
    if (isPointThis && isPointOther) {
        const Coordinate& pt = pts->getAt(start);
        const Coordinate& seqPt = facetSeq.pts->getAt(facetSeq.start);
        GeometryLocation gl1(geom, start, pt);
        GeometryLocation gl2(facetSeq.geom, facetSeq.start, seqPt);
        locs.clear();
        locs.push_back(gl1);
        locs.push_back(gl2);
    }
    else if (isPointThis) {
        const Coordinate& pt = pts->getAt(start);
        computeDistancePointLine(pt, facetSeq, &locs);
    }
    else if (isPointOther) {
        const Coordinate& seqPt = facetSeq.pts->getAt(facetSeq.start);
        computeDistancePointLine(seqPt, *this, &locs);
        // unflip the locations
        GeometryLocation tmp = locs[0];
        locs[0] = locs[1];
        locs[1] = tmp;
    }
    else {
        computeDistanceLineLine(facetSeq, &locs);
    }
    return locs;
}

double
FacetSequence::computeDistancePointLine(const Coordinate& pt,
                                        const FacetSequence& facetSeq,
                                        std::vector<GeometryLocation> *locs) const
{
    double minDistance = std::numeric_limits<double>::infinity();

    for(size_t i = facetSeq.start; i < facetSeq.end - 1; i++) {
        const Coordinate& q0 = facetSeq.pts->getAt(i);
        const Coordinate& q1 = facetSeq.pts->getAt(i + 1);
        double dist = Distance::pointToSegment(pt, q0, q1);
        if(dist < minDistance || (locs != nullptr && locs->empty())) {
            minDistance = dist;
            if (locs != nullptr) {
                updateNearestLocationsPointLine(pt, facetSeq, i, q0, q1, locs);
            }
            if(minDistance <= 0.0) {
                return minDistance;
            }
        }
    }

    return minDistance;
}

void
FacetSequence::updateNearestLocationsPointLine(const Coordinate& pt,
        const FacetSequence& facetSeq, size_t i,
        const Coordinate& q0, const Coordinate &q1,
        std::vector<GeometryLocation> *locs) const
{
    geom::LineSegment seg(q0, q1);
    Coordinate segClosestPoint;
    seg.closestPoint(pt, segClosestPoint);
    locs->clear();
    locs->emplace_back(geom, start, pt);
    locs->emplace_back(facetSeq.geom, i, segClosestPoint);
    return;
}

double
FacetSequence::computeDistanceLineLine(const FacetSequence& facetSeq, std::vector<GeometryLocation> *locs) const
{
    double minDistance = std::numeric_limits<double>::infinity();

    for(size_t i = start; i < end - 1; i++) {
        const Coordinate& p0 = pts->getAt(i);
        const Coordinate& p1 = pts->getAt(i + 1);

        // Avoid calculating distance from zero-length segment
        if (p0 == p1)
            continue;

        Envelope pEnv(p0, p1);
        if (pEnv.distanceSquared(*facetSeq.getEnvelope()) > minDistance*minDistance) {
            continue;
        }

        for(size_t j = facetSeq.start; j < facetSeq.end - 1; j++) {
            const Coordinate& q0 = facetSeq.pts->getAt(j);
            const Coordinate& q1 = facetSeq.pts->getAt(j + 1);

            // Avoid calculating distance to zero-length segment
            if (q0 == q1)
                continue;

            Envelope qEnv(q0, q1);
            if (pEnv.distanceSquared(qEnv) > minDistance*minDistance) {
                continue;
            }

            double dist = Distance::segmentToSegment(p0, p1, q0, q1);
            if(dist <= minDistance) {
                minDistance = dist;
                if(locs != nullptr) {
                    updateNearestLocationsLineLine(i, p0, p1, facetSeq, j, q0, q1, locs);
                }
                if(minDistance <= 0.0) return minDistance;
            }
        }
    }

    return minDistance;
}

void
FacetSequence::updateNearestLocationsLineLine(size_t i, const Coordinate& p0, const Coordinate& p1,
        const FacetSequence& facetSeq,
        size_t j, const Coordinate& q0, const Coordinate &q1,
        std::vector<GeometryLocation> *locs) const
{
    LineSegment seg0(p0, p1);
    LineSegment seg1(q0, q1);

    auto closestPts = seg0.closestPoints(seg1);

    locs->clear();
    locs->emplace_back(geom, i, closestPts[0]);
    locs->emplace_back(facetSeq.geom, j, closestPts[1]);
}

void
FacetSequence::computeEnvelope()
{
    env = Envelope();
    for(size_t i = start; i < end; i++) {
        env.expandToInclude(pts->getAt(i));
    }
}

const Envelope*
FacetSequence::getEnvelope() const
{
    return &env;
}

const Coordinate*
FacetSequence::getCoordinate(size_t index) const
{
    return &(pts->getAt(start + index));
}

