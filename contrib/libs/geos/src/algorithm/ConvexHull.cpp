/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/ConvexHull.java r407 (JTS-1.12+)
 *
 **********************************************************************/

#include <geos/algorithm/ConvexHull.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/algorithm/Orientation.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/LineString.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/util/Interrupt.h>

#include <typeinfo>
#include <algorithm>

#ifndef GEOS_INLINE
# include "geos/algorithm/ConvexHull.inl"
#endif

using namespace geos::geom;

namespace geos {
namespace algorithm { // geos.algorithm

namespace {

/**
 * This is the class used to sort in radial order.
 * It's defined here as it's a static one.
 */
class RadiallyLessThen {

private:

    const Coordinate* origin;

    int
    polarCompare(const Coordinate* o, const Coordinate* p,
                 const Coordinate* q)
    {
        int orient = Orientation::index(*o, *p, *q);

        if(orient == Orientation::COUNTERCLOCKWISE) {
            return 1;
        }
        if(orient == Orientation::CLOCKWISE) {
            return -1;
        }

        /**
         * The points are collinear,
         * so compare based on distance from the origin.
         * The points p and q are >= to the origin,
         * so they lie in the closed half-plane above the origin.
         * If they are not in a horizontal line,
         * the Y ordinate can be tested to determine distance.
         * This is more robust than computing the distance explicitly.
         */
        if (p->y > q->y) return 1;
        if (p->y < q->y) return -1;

        /**
         * The points lie in a horizontal line, which should also contain the origin
         * (since they are collinear).
         * Also, they must be above the origin.
         * Use the X ordinate to determine distance.
         */
        if (p->x > q->x) return 1;
        if (p->x < q->x) return -1;

      // Assert: p = q
        return 0;
    }

public:

    RadiallyLessThen(const Coordinate* c): origin(c) {}

    bool
    operator()(const Coordinate* p1, const Coordinate* p2)
    {
        return (polarCompare(origin, p1, p2) == -1);
    }

};

} // unnamed namespace

/* private */
std::unique_ptr<CoordinateSequence>
ConvexHull::toCoordinateSequence(Coordinate::ConstVect& cv)
{
    const CoordinateSequenceFactory* csf =
        geomFactory->getCoordinateSequenceFactory();

    std::vector<Coordinate> vect(cv.size());

    for(size_t i = 0; i < cv.size(); ++i) {
        vect[i] = *(cv[i]); // Coordinate copy
    }

    return csf->create(std::move(vect)); // takes ownership of the vector
}

/* private */
void
ConvexHull::computeOctPts(const Coordinate::ConstVect& p_inputPts,
                          Coordinate::ConstVect& pts)
{
    // Initialize all slots with first input coordinate
    pts = Coordinate::ConstVect(8, p_inputPts[0]);

    for(size_t i = 1, n = p_inputPts.size(); i < n; ++i) {
        if(p_inputPts[i]->x < pts[0]->x) {
            pts[0] = p_inputPts[i];
        }
        if(p_inputPts[i]->x - p_inputPts[i]->y < pts[1]->x - pts[1]->y) {
            pts[1] = p_inputPts[i];
        }
        if(p_inputPts[i]->y > pts[2]->y) {
            pts[2] = p_inputPts[i];
        }
        if(p_inputPts[i]->x + p_inputPts[i]->y > pts[3]->x + pts[3]->y) {
            pts[3] = p_inputPts[i];
        }
        if(p_inputPts[i]->x > pts[4]->x) {
            pts[4] = p_inputPts[i];
        }
        if(p_inputPts[i]->x - p_inputPts[i]->y > pts[5]->x - pts[5]->y) {
            pts[5] = p_inputPts[i];
        }
        if(p_inputPts[i]->y < pts[6]->y) {
            pts[6] = p_inputPts[i];
        }
        if(p_inputPts[i]->x + p_inputPts[i]->y < pts[7]->x + pts[7]->y) {
            pts[7] = p_inputPts[i];
        }
    }

}


/* private */
bool
ConvexHull::computeOctRing(const Coordinate::ConstVect& p_inputPts,
                           Coordinate::ConstVect& dest)
{
    computeOctPts(p_inputPts, dest);

    // Remove consecutive equal Coordinates
    // unique() returns an iterator to the end of the resulting
    // sequence, we erase from there to the end.
    dest.erase(std::unique(dest.begin(), dest.end()), dest.end());

    // points must all lie in a line
    if(dest.size() < 3) {
        return false;
    }

    // close ring
    dest.push_back(dest[0]);

    return true;
}

/* private */
void
ConvexHull::reduce(Coordinate::ConstVect& pts)
{
    Coordinate::ConstVect polyPts;

    if(! computeOctRing(pts, polyPts)) {
        // unable to compute interior polygon for some reason
        return;
    }

    // add points defining polygon
    Coordinate::ConstSet reducedSet;
    reducedSet.insert(polyPts.begin(), polyPts.end());

    /*
     * Add all unique points not in the interior poly.
     * CGAlgorithms.isPointInRing is not defined for points
     * actually on the ring, but this doesn't matter since
     * the points of the interior polygon are forced to be
     * in the reduced set.
     *
     * @@TIP: there should be a std::algo for this
     */
    for(size_t i = 0, n = pts.size(); i < n; ++i) {
        if(!PointLocation::isInRing(*(pts[i]), polyPts)) {
            reducedSet.insert(pts[i]);
        }
    }

    inputPts.assign(reducedSet.begin(), reducedSet.end());

    if(inputPts.size() < 3) {
        padArray3(inputPts);
    }
}

/* private */
void
ConvexHull::padArray3(geom::Coordinate::ConstVect& pts)
{
    for(size_t i = pts.size(); i < 3; ++i) {
        pts.push_back(pts[0]);
    }
}

std::unique_ptr<Geometry>
ConvexHull::getConvexHull()
{
    size_t nInputPts = inputPts.size();

    if(nInputPts == 0) { // Return an empty geometry
        return geomFactory->createEmptyGeometry();
    }

    if(nInputPts == 1) { // Return a Point
        // Copy the Coordinate from the ConstVect
        return std::unique_ptr<Geometry>(geomFactory->createPoint(*(inputPts[0])));
    }

    if(nInputPts == 2) { // Return a LineString
        // Copy all Coordinates from the ConstVect
        auto cs = toCoordinateSequence(inputPts);
        return geomFactory->createLineString(std::move(cs));
    }

    // use heuristic to reduce points, if large
    if(nInputPts > 50) {
        reduce(inputPts);
    }

    GEOS_CHECK_FOR_INTERRUPTS();

    // sort points for Graham scan.
    preSort(inputPts);

    GEOS_CHECK_FOR_INTERRUPTS();

    // Use Graham scan to find convex hull.
    Coordinate::ConstVect cHS;
    grahamScan(inputPts, cHS);

    GEOS_CHECK_FOR_INTERRUPTS();

    return lineOrPolygon(cHS);
}

/* private */
void
ConvexHull::preSort(Coordinate::ConstVect& pts)
{
    // find the lowest point in the set. If two or more points have
    // the same minimum y coordinate choose the one with the minimum x.
    // This focal point is put in array location pts[0].
    for(size_t i = 1, n = pts.size(); i < n; ++i) {
        const Coordinate* p0 = pts[0]; // this will change
        const Coordinate* pi = pts[i];
        if((pi->y < p0->y) || ((pi->y == p0->y) && (pi->x < p0->x))) {
            const Coordinate* t = p0;
            pts[0] = pi;
            pts[i] = t;
        }
    }

    // sort the points radially around the focal point.
    std::sort(pts.begin(), pts.end(), RadiallyLessThen(pts[0]));
}

/*private*/
void
ConvexHull::grahamScan(const Coordinate::ConstVect& c,
                       Coordinate::ConstVect& ps)
{
    ps.push_back(c[0]);
    ps.push_back(c[1]);
    ps.push_back(c[2]);

    for(size_t i = 3, n = c.size(); i < n; ++i) {
        const Coordinate* p = ps.back();
        ps.pop_back();
        while(!ps.empty() &&
                Orientation::index(
                    *(ps.back()), *p, *(c[i])) > 0) {
            p = ps.back();
            ps.pop_back();
        }
        ps.push_back(p);
        ps.push_back(c[i]);
    }
    ps.push_back(c[0]);
}


/*private*/
bool
ConvexHull::isBetween(const Coordinate& c1, const Coordinate& c2, const Coordinate& c3)
{
    if(Orientation::index(c1, c2, c3) != 0) {
        return false;
    }
    if(c1.x != c3.x) {
        if(c1.x <= c2.x && c2.x <= c3.x) {
            return true;
        }
        if(c3.x <= c2.x && c2.x <= c1.x) {
            return true;
        }
    }
    if(c1.y != c3.y) {
        if(c1.y <= c2.y && c2.y <= c3.y) {
            return true;
        }
        if(c3.y <= c2.y && c2.y <= c1.y) {
            return true;
        }
    }
    return false;
}


/* private */
std::unique_ptr<Geometry>
ConvexHull::lineOrPolygon(const Coordinate::ConstVect& input)
{
    Coordinate::ConstVect cleaned;

    cleanRing(input, cleaned);

    if(cleaned.size() == 3) { // shouldn't this be 2 ??
        cleaned.resize(2);
        auto cl1 = toCoordinateSequence(cleaned);
        return geomFactory->createLineString(std::move(cl1));
    }
    auto cl2 = toCoordinateSequence(cleaned);
    std::unique_ptr<LinearRing> linearRing = geomFactory->createLinearRing(std::move(cl2));
    return geomFactory->createPolygon(std::move(linearRing));
}

/*private*/
void
ConvexHull::cleanRing(const Coordinate::ConstVect& original,
                      Coordinate::ConstVect& cleaned)
{
    size_t npts = original.size();

    const Coordinate* last = original[npts - 1];

    //util::Assert::equals(*(original[0]), *last);
    assert(last);
    assert(original[0]->equals2D(*last));

    const Coordinate* prev = nullptr;
    for(size_t i = 0; i < npts - 1; ++i) {
        const Coordinate* curr = original[i];
        const Coordinate* next = original[i + 1];

        // skip consecutive equal coordinates
        if(curr->equals2D(*next)) {
            continue;
        }

        if(prev != nullptr &&  isBetween(*prev, *curr, *next)) {
            continue;
        }

        cleaned.push_back(curr);
        prev = curr;
    }

    cleaned.push_back(last);

}


} // namespace geos.algorithm
} // namespace geos
