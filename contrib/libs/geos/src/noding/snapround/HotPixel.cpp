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
 * Last port: noding/snapround/HotPixel.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/noding/snapround/HotPixel.h>

#include <geos/algorithm/CGAlgorithmsDD.h>
#include <geos/noding/NodedSegmentString.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/geom/Coordinate.h>
#include <geos/util/IllegalArgumentException.h>

#ifndef GEOS_INLINE
# include "geos/noding/snapround/HotPixel.inl"
#endif

#include <algorithm> // for std::min and std::max
#include <cassert>
#include <memory>

using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace noding { // geos.noding
namespace snapround { // geos.noding.snapround

HotPixel::HotPixel(const Coordinate& newPt, double newScaleFactor)
    : originalPt(newPt)
    , scaleFactor(newScaleFactor)
    , hpIsNode(false)
    , hpx(newPt.x)
    , hpy(newPt.y)
{
    if(scaleFactor <= 0.0) {
        throw util::IllegalArgumentException("Scale factor must be non-zero");
    }
    if(scaleFactor != 1.0) {
        hpx = scaleRound(newPt.x);
        hpy = scaleRound(newPt.y);
    }
}

/*public*/
const geom::Coordinate&
HotPixel::getCoordinate() const
{
    return originalPt;
}

/* public */
bool
HotPixel::intersects(const Coordinate& p) const
{
    double x = scale(p.x);
    double y = scale(p.y);
    if (x >= hpx + TOLERANCE) return false;
    // check Left side
    if (x < hpx - TOLERANCE) return false;
    // check Top side
    if (y >= hpy + TOLERANCE) return false;
    // check Bottom side
    if (y < hpy - TOLERANCE) return false;
    // finally
    return true;
}

/*public*/
bool
HotPixel::intersects(const Coordinate& p0,
                     const Coordinate& p1) const
{
    if(scaleFactor == 1.0) {
        return intersectsScaled(p0.x, p0.y, p1.x, p1.y);
    }

    double sp0x = scale(p0.x);
    double sp0y = scale(p0.y);
    double sp1x = scale(p1.x);
    double sp1y = scale(p1.y);
    return intersectsScaled(sp0x, sp0y, sp1x, sp1y);
}

/*private*/
bool
HotPixel::intersectsScaled(double p0x, double p0y, double p1x, double p1y) const
{
    // determine oriented segment pointing in positive X direction
    double px = p0x;
    double py = p0y;
    double qx = p1x;
    double qy = p1y;
    if (px > qx) {
        px = p1x;
        py = p1y;
        qx = p0x;
        qy = p0y;
    }

    /**
    * Report false if segment env does not intersect pixel env.
    * This check reflects the fact that the pixel Top and Right sides
    * are open (not part of the pixel).
    */
    // check Right side
    double maxx = hpx + TOLERANCE;
    double segMinx = std::min(px, qx);
    if (segMinx >= maxx) return false;
    // check Left side
    double minx = hpx - TOLERANCE;
    double segMaxx = std::max(px, qx);
    if (segMaxx < minx) return false;
    // check Top side
    double maxy = hpy + TOLERANCE;
    double segMiny = std::min(py, qy);
    if (segMiny >= maxy) return false;
    // check Bottom side
    double miny = hpy - TOLERANCE;
    double segMaxy = std::max(py, qy);
    if (segMaxy < miny) return false;

    /**
    * Vertical or horizontal segments must now intersect
    * the segment interior or Left or Bottom sides.
    */
    // check vertical segment
    if (px == qx) {
        return true;
    }
    // check horizontal segment
    if (py == qy) {
        return true;
    }

    /**
    * Now know segment is not horizontal or vertical.
    *
    * Compute orientation WRT each pixel corner.
    * If corner orientation == 0,
    * segment intersects the corner.
    * From the corner and whether segment is heading up or down,
    * can determine intersection or not.
    *
    * Otherwise, check whether segment crosses interior of pixel side
    * This is the case if the orientations for each corner of the side are different.
    */
    int orientUL = CGAlgorithmsDD::orientationIndex(px, py, qx, qy, minx, maxy);
    if (orientUL == 0) {
        // upward segment does not intersect pixel interior
        if (py < qy) return false;
        // downward segment must intersect pixel interior
        return true;
    }

    int orientUR = CGAlgorithmsDD::orientationIndex(px, py, qx, qy, maxx, maxy);
    if (orientUR == 0) {
        // downward segment does not intersect pixel interior
        if (py > qy) return false;
        // upward segment must intersect pixel interior
        return true;
    }
    // check crossing Top side
    if (orientUL != orientUR) {
        return true;
    }

    int orientLL = CGAlgorithmsDD::orientationIndex(px, py, qx, qy, minx, miny);
    if (orientLL == 0) {
        // LL corner is the only one in pixel interior
        return true;
    }
    // check crossing Left side
    if (orientLL != orientUL) {
        return true;
    }

    int orientLR = CGAlgorithmsDD::orientationIndex(px, py, qx, qy, maxx, miny);
    if (orientLR == 0) {
        // upward segment does not intersect pixel interior
        if (py < qy) return false;
        // downward segment must intersect pixel interior
        return true;
    }

    // check crossing Bottom side
    if (orientLL != orientLR) {
        return true;
    }
    // check crossing Right side
    if (orientLR != orientUR) {
        return true;
    }

    // segment does not intersect pixel
    return false;
}

/*private*/
bool
HotPixel::intersectsPixelClosure(const Coordinate& p0, const Coordinate& p1) const
{
    LineIntersector li;
    std::array<Coordinate, 4> corner;

    double minx = hpx - TOLERANCE;
    double maxx = hpx + TOLERANCE;
    double miny = hpy - TOLERANCE;
    double maxy = hpy + TOLERANCE;

    corner[UPPER_RIGHT] = Coordinate(maxx, maxy);
    corner[UPPER_LEFT]  = Coordinate(minx, maxy);
    corner[LOWER_LEFT]  = Coordinate(minx, miny);
    corner[LOWER_RIGHT] = Coordinate(maxx, miny);

    li.computeIntersection(p0, p1, corner[UPPER_RIGHT], corner[UPPER_LEFT]);
    if (li.hasIntersection()) {
        return true;
    }
    li.computeIntersection(p0, p1, corner[UPPER_LEFT], corner[LOWER_LEFT]);
    if (li.hasIntersection()) {
        return true;
    }
    li.computeIntersection(p0, p1, corner[LOWER_LEFT], corner[LOWER_RIGHT]);
    if (li.hasIntersection()) {
        return true;
    }
    li.computeIntersection(p0, p1, corner[LOWER_RIGHT], corner[UPPER_RIGHT]);
    if (li.hasIntersection()) {
        return true;
    }

    return false;
}


std::ostream&
HotPixel::operator<< (std::ostream& os)
{
    os << "HP(" << io::WKTWriter::toPoint(originalPt) << ")";
    return os;
}


} // namespace geos.noding.snapround
} // namespace geos.noding
} // namespace geos
