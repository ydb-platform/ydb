/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/CentralEndpointIntersector.java rev. 1.1
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_CENTRALENDPOINTINTERSECTOR_H
#define GEOS_ALGORITHM_CENTRALENDPOINTINTERSECTOR_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>

#include <string>
#include <limits>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
//class PrecisionModel;
}
}

namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Computes an approximate intersection of two line segments
 * by taking the most central of the endpoints of the segments.
 *
 * This is effective in cases where the segments are nearly parallel
 * and should intersect at an endpoint.
 * It is also a reasonable strategy for cases where the
 * endpoint of one segment lies on or almost on the interior of another one.
 * Taking the most central endpoint ensures that the computed intersection
 * point lies in the envelope of the segments.
 * Also, by always returning one of the input points, this should result
 * in reducing segment fragmentation.
 * Intended to be used as a last resort for
 * computing ill-conditioned intersection situations which
 * cause other methods to fail.
 *
 * @author Martin Davis
 * @version 1.8
 */
class GEOS_DLL CentralEndpointIntersector {

public:

    static const geom::Coordinate&
    getIntersection(const geom::Coordinate& p00,
                    const geom::Coordinate& p01, const geom::Coordinate& p10,
                    const geom::Coordinate& p11)
    {
        CentralEndpointIntersector intor(p00, p01, p10, p11);
        return intor.getIntersection();
    }

    CentralEndpointIntersector(const geom::Coordinate& p00,
                               const geom::Coordinate& p01,
                               const geom::Coordinate& p10,
                               const geom::Coordinate& p11)
        :
        _pts(4)
    {
        _pts[0] = p00;
        _pts[1] = p01;
        _pts[2] = p10;
        _pts[3] = p11;
        compute();
    }

    const geom::Coordinate&
    getIntersection() const
    {
        return _intPt;
    }


private:

    // This is likely overkill.. we'll be allocating heap
    // memory at every call !
    std::vector<geom::Coordinate> _pts;

    geom::Coordinate _intPt;

    void
    compute()
    {
        geom::Coordinate centroid = average(_pts);
        _intPt = findNearestPoint(centroid, _pts);
    }

    static geom::Coordinate
    average(
        const std::vector<geom::Coordinate>& pts)
    {
        geom::Coordinate avg(0, 0);
        size_t n = pts.size();
        if(! n) {
            return avg;
        }
        for(std::size_t i = 0; i < n; ++i) {
            avg.x += pts[i].x;
            avg.y += pts[i].y;
        }
        avg.x /= n;
        avg.y /= n;
        return avg;
    }

    /**
     * Determines a point closest to the given point.
     *
     * @param p the point to compare against
     * @param p1 a potential result point
     * @param p2 a potential result point
     * @param q1 a potential result point
     * @param q2 a potential result point
     * @return the point closest to the input point p
     */
    geom::Coordinate
    findNearestPoint(const geom::Coordinate& p,
                     const std::vector<geom::Coordinate>& pts) const
    {
        double minDistSq = DoubleInfinity;
        geom::Coordinate result = geom::Coordinate::getNull();
        for(std::size_t i = 0, n = pts.size(); i < n; ++i) {
            double distSq = p.distanceSquared(pts[i]);
            if(distSq < minDistSq) {
                minDistSq = distSq;
                result = pts[i];
            }
        }
        return result;
    }
};

} // namespace geos::algorithm
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_ALGORITHM_CENTRALENDPOINTINTERSECTOR_H
