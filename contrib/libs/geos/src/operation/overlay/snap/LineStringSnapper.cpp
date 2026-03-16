/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009-2010  Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/snap/LineStringSnapper.java r320 (JTS-1.12)
 *
 * NOTE: algorithm changed to improve output quality by reducing
 *       probability of self-intersections
 *
 **********************************************************************/

#include <geos/operation/overlay/snap/LineStringSnapper.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateList.h>
#include <geos/geom/Envelope.h>
#include <geos/util/UniqueCoordinateArrayFilter.h>
#include <geos/geom/LineSegment.h>
#include <geos/util/Interrupt.h>

#include <vector>
#include <memory>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#if GEOS_DEBUG
#include <iostream>
#include <iomanip>
using std::cerr;
using std::endl;
#endif

//using namespace std;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay
namespace snap { // geos.operation.overlay.snap

/*public*/
std::unique_ptr<Coordinate::Vect>
LineStringSnapper::snapTo(const geom::Coordinate::ConstVect& snapPts)
{
    geom::CoordinateList coordList(srcPts);

    snapVertices(coordList, snapPts);
    snapSegments(coordList, snapPts);

    return coordList.toCoordinateArray();
}

/*private*/
CoordinateList::iterator
LineStringSnapper::findVertexToSnap(
    const Coordinate& snapPt,
    CoordinateList::iterator from,
    CoordinateList::iterator too_far)
{
    double minDist = snapTolerance; // make sure the first closer then
    // snapTolerance is accepted
    CoordinateList::iterator match = too_far;

    for(; from != too_far; ++from) {
        Coordinate& c0 = *from;

#if GEOS_DEBUG
        cerr << " Checking vertex " << c0 << endl;
#endif

        double dist = c0.distance(snapPt);
        if(dist >= minDist) {
#if GEOS_DEBUG
            cerr << "   snap point distance " << dist
                 << " not smaller than tolerance "
                 << snapTolerance << " or previous closest "
                 << minDist << endl;
#endif
            continue;
        }

#if GEOS_DEBUG
        cerr << "   snap point distance " << dist << " within tolerance "
             << snapTolerance << " and closer than previous candidate "
             << minDist << endl;
#endif

        if(dist == 0.0) {
            return from;    // can't find any closer
        }

        match = from;
        minDist = dist;

    }

    return match;
}


/*private*/
void
LineStringSnapper::snapVertices(geom::CoordinateList& srcCoords,
                                const geom::Coordinate::ConstVect& snapPts)
{
    // nothing to do if there are no source coords..
    if(srcCoords.empty()) {
        return;
    }

#if GEOS_DEBUG
    cerr << "Snapping vertices of: " << srcCoords << endl;
#endif

    for(Coordinate::ConstVect::const_iterator
            it = snapPts.begin(), end = snapPts.end();
            it != end;
            ++it) {
        GEOS_CHECK_FOR_INTERRUPTS();
        assert(*it);
        const Coordinate& snapPt = *(*it);

#if GEOS_DEBUG
        cerr << "Checking for a vertex to snap to snapPt " << snapPt << endl;
#endif

        CoordinateList::iterator too_far = srcCoords.end();
        if(isClosed) {
            --too_far;
        }
        CoordinateList::iterator vertpos =
            findVertexToSnap(snapPt, srcCoords.begin(), too_far);
        if(vertpos == too_far) {
#if GEOS_DEBUG
            cerr << " No vertex to snap" << endl;
#endif
            continue;
        }

#if GEOS_DEBUG
        cerr << " Vertex to be snapped found, snapping" << endl;
#endif
        *vertpos = snapPt;

        // keep final closing point in synch (rings only)
        if(vertpos == srcCoords.begin() && isClosed) {
            vertpos = srcCoords.end();
            --vertpos;
#if GEOS_DEBUG
            cerr << " Snapped vertex was first in a closed line, also snapping last" << endl;
#endif
            *vertpos = snapPt;
        }

#if GEOS_DEBUG
        cerr << " After snapping of vertex " << snapPt << ", srcCoors are: " << srcCoords << endl;
#endif

    }

#if GEOS_DEBUG
    cerr << " After vertices snapping, srcCoors are: " << srcCoords << endl;
#endif

}

/*private*/
Coordinate::ConstVect::const_iterator
LineStringSnapper::findSnapForVertex(const Coordinate& pt,
                                     const Coordinate::ConstVect& snapPts)
{
    Coordinate::ConstVect::const_iterator end = snapPts.end();
    Coordinate::ConstVect::const_iterator candidate = end;
    double minDist = snapTolerance;

    // TODO: use std::find_if
    for(Coordinate::ConstVect::const_iterator
            it = snapPts.begin();
            it != end;
            ++it) {
        assert(*it);
        const Coordinate& snapPt = *(*it);

        if(snapPt.equals2D(pt)) {
#if GEOS_DEBUG
            cerr << " points are equal, returning not-found " << endl;
#endif
            return end;
        }

        double dist = snapPt.distance(pt);
#if GEOS_DEBUG
        cerr << " distance from snap point " << snapPt << ": " << dist << endl;
#endif

        if(dist < minDist) {
            minDist = dist;
            candidate = it;
        }
    }

#if GEOS_DEBUG
    if(candidate == end) {
        cerr << " no snap point within distance, returning not-found" << endl;
    }
#endif

    return candidate;
}


/*private*/
void
LineStringSnapper::snapSegments(geom::CoordinateList& srcCoords,
                                const geom::Coordinate::ConstVect& snapPts)
{

    // nothing to do if there are no source coords..
    if(srcCoords.empty()) {
        return;
    }

    GEOS_CHECK_FOR_INTERRUPTS();

#if GEOS_DEBUG
    cerr << "Snapping segments of: " << srcCoords << endl;
#endif

    for(Coordinate::ConstVect::const_iterator
            it = snapPts.begin(), end = snapPts.end();
            it != end;
            ++it) {
        assert(*it);
        const Coordinate& snapPt = *(*it);

#if GEOS_DEBUG
        cerr << "Checking for a segment to snap to snapPt " << snapPt << endl;
#endif

        CoordinateList::iterator too_far = srcCoords.end();
        --too_far;
        CoordinateList::iterator segpos =
            findSegmentToSnap(snapPt, srcCoords.begin(), too_far);
        if(segpos == too_far) {
#if GEOS_DEBUG
            cerr << " No segment to snap" << endl;
#endif
            continue;
        }

        /* Check if the snap point falls outside of the segment */
        // If the snap point is outside, this means that an endpoint
        // was not snap where it should have been
        // so what we should do is re-snap the endpoint to this
        // snapPt and then snap the closest between this and
        // previous (for pf < 0.0) or next (for pf > 1.0) segment
        // to the old endpoint.
        //     --strk May 2013
        //
        // TODO: simplify this code, make more readable
        //
        CoordinateList::iterator to = segpos;
        ++to;
        LineSegment seg(*segpos, *to);
        double pf = seg.projectionFactor(snapPt);
        if(pf >= 1.0) {
#if GEOS_DEBUG
            cerr << " Segment to be snapped is closer on his end point" << endl;
#endif
            Coordinate newSnapPt = seg.p1;
            *to = seg.p1 = snapPt;
            // now snap from-to (segpos) or to-next (segpos++) to newSnapPt
            if(to == too_far) {
                if(isClosed) {
#if GEOS_DEBUG
                    cerr << " His end point is the last one, but is closed " << endl;
#endif
                    *(srcCoords.begin()) = snapPt; // sync to start point
                    to = srcCoords.begin();
                }
                else {
#if GEOS_DEBUG
                    cerr << " His end point is the last one, inserting " << newSnapPt << " before it" << endl;
#endif
                    srcCoords.insert(to, newSnapPt);
                    continue;
                }
            }
            ++to;
            LineSegment nextSeg(seg.p1, *to);
            if(nextSeg.distance(newSnapPt) < seg.distance(newSnapPt)) {
#if GEOS_DEBUG
                cerr << " Next segment closer, inserting " << newSnapPt << " into " << nextSeg << endl;
#endif
                // insert into next segment
                srcCoords.insert(to, newSnapPt);
            }
            else {
#if GEOS_DEBUG
                cerr << " This segment closer, inserting " << newSnapPt << " into " << seg << endl;
#endif
                // insert must happen one-past first point (before next point)
                ++segpos;
                srcCoords.insert(segpos, newSnapPt);
            }
        }
        else if(pf <= 0.0) {
#if GEOS_DEBUG
            cerr << " Segment to be snapped is closer on his start point" << endl;
#endif
            Coordinate newSnapPt = seg.p0;
            *segpos = seg.p0 = snapPt;
            // now snap prev-from (--segpos) or from-to (segpos) to newSnapPt
            if(segpos == srcCoords.begin()) {
                if(isClosed) {
#if GEOS_DEBUG
                    cerr << " His start point is the first one, but is closed " << endl;
#endif
                    segpos = srcCoords.end();
                    --segpos;
                    *segpos = snapPt; // sync to end point
                }
                else {
#if GEOS_DEBUG
                    cerr << " His start point is the first one, inserting " << newSnapPt << " before it" << endl;
#endif
                    ++segpos;
                    srcCoords.insert(segpos, newSnapPt);
                    continue;
                }
            }

#if GEOS_DEBUG
            cerr << " Before seg-snapping, srcCoors are: " << srcCoords << endl;
#endif

            --segpos;
            LineSegment prevSeg(*segpos, seg.p0);
            if(prevSeg.distance(newSnapPt) < seg.distance(newSnapPt)) {
#if GEOS_DEBUG
                cerr << " Prev segment closer, inserting " << newSnapPt << " into "
                     << prevSeg << endl;
#endif
                // insert into prev segment
                ++segpos;
                srcCoords.insert(segpos, newSnapPt);
            }
            else {
#if GEOS_DEBUG
                cerr << " This segment closer, inserting " << newSnapPt << " into " << seg << endl;
#endif
                // insert must happen one-past first point (before next point)
                srcCoords.insert(to, newSnapPt);
            }
        }
        else {
            //assert(pf != 0.0);
#if GEOS_DEBUG
            cerr << " Segment to be snapped found, projection factor is " << pf << ", inserting point" << endl;
#endif
            // insert must happen one-past first point (before next point)
            ++segpos;
            srcCoords.insert(segpos, snapPt);
        }
    }

#if GEOS_DEBUG
    cerr << " After segment snapping, srcCoors are: " << srcCoords << endl;
#endif

}

/*private*/
/* NOTE: this is called findSegmentIndexToSnap in JTS */
CoordinateList::iterator
LineStringSnapper::findSegmentToSnap(
    const Coordinate& snapPt,
    CoordinateList::iterator from,
    CoordinateList::iterator too_far)
{
    LineSegment seg;
    double minDist = snapTolerance; // make sure the first closer then
    // snapTolerance is accepted
    CoordinateList::iterator match = too_far;

    // TODO: use std::find_if
    for(; from != too_far; ++from) {
        seg.p0 = *from;
        CoordinateList::iterator to = from;
        ++to;
        seg.p1 = *to;

#if GEOS_DEBUG
        cerr << " Checking segment " << seg << endl;
#endif

        /*
         * Check if the snap pt is equal to one of
         * the segment endpoints.
         *
         * If the snap pt is already in the src list,
         * don't snap at all (unless allowSnappingToSourceVertices
         * is set to true)
         */
        if(seg.p0.equals2D(snapPt) || seg.p1.equals2D(snapPt)) {

            if(allowSnappingToSourceVertices) {
#if GEOS_DEBUG
                cerr << "   snap point matches a segment endpoint, checking next segment"
                     << endl;
#endif
                continue;
            }
            else {
#if GEOS_DEBUG
                cerr << "   snap point matches a segment endpoint, giving up seek" << endl;
#endif
                return too_far;
            }
        }

        if (Envelope::distanceSquaredToCoordinate(snapPt, seg.p0, seg.p1) >= minDist*minDist) {
            continue;
        }

        double dist = seg.distance(snapPt);
        if(dist >= minDist) {
#if GEOS_DEBUG
            cerr << "   snap point distance " << dist
                 << " not smaller than tolerance "
                 << snapTolerance << " or previous closest "
                 << minDist << endl;
#endif
            continue;
        }

#if GEOS_DEBUG
        cerr << "   snap point distance " << dist << " within tolerance "
             << snapTolerance << " and closer than previous candidate "
             << minDist << endl;
#endif

        if(dist == 0.0) {
            return from;    // can't find any closer
        }

        match = from;
        minDist = dist;

    }

    return match;
}

} // namespace geos.operation.snap
} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

