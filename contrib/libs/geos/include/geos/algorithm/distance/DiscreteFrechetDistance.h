/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2016 Shinichi SUGIYAMA (shin.sugi@gmail.com)
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: original work
 *
 * Developed by Shinichi SUGIYAMA (shin.sugi@gmail.com)
 * based on http://www.kr.tuwien.ac.at/staff/eiter/et-archive/cdtr9464.pdf
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_DISTANCE_DISCRETEFRECHETDISTANCE_H
#define GEOS_ALGORITHM_DISTANCE_DISCRETEFRECHETDISTANCE_H

#include <geos/export.h>
#include <geos/algorithm/distance/PointPairDistance.h> // for composition
#include <geos/algorithm/distance/DistanceToPoint.h> // for composition
#include <geos/util/IllegalArgumentException.h> // for inlines
#include <geos/geom/Geometry.h> // for inlines
#include <geos/util/math.h> // for inlines
#include <geos/geom/CoordinateFilter.h> // for inheritance
#include <geos/geom/CoordinateSequence.h> // for inheritance

#include <cstddef>
#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace algorithm {
//class RayCrossingCounter;
}
namespace geom {
class Geometry;
class Coordinate;
//class CoordinateSequence;
}
namespace index {
namespace intervalrtree {
//class SortedPackedIntervalRTree;
}
}
}

namespace geos {
namespace algorithm { // geos::algorithm
namespace distance { // geos::algorithm::distance

/** \brief
 * An algorithm for computing a distance metric
 * which is an approximation to the Frechet Distance
 * based on a discretization of the input {@link geom::Geometry}.
 *
 * The algorithm computes the Frechet distance restricted to discrete points
 * for one of the geometries.
 * The points can be either the vertices of the geometries (the default),
 * or the geometries with line segments densified by a given fraction.
 * Also determines two points of the Geometries which are separated by the
 * computed distance.
 *
 * This algorithm is an approximation to the standard Frechet distance.
 * Specifically,
 * <pre>
 *    for all geometries a, b:    DFD(a, b) >= FD(a, b)
 * </pre>
 * The approximation can be made as close as needed by densifying the
 * input geometries.
 * In the limit, this value will approach the true Frechet distance:
 * <pre>
 *    DFD(A, B, densifyFactor) -> FD(A, B) as densifyFactor -> 0.0
 * </pre>
 * The default approximation is exact or close enough for a large subset of
 * useful cases.
 *
 * The difference between DFD and FD is bounded
 * by the length of the longest edge of the polygonal curves.
 *
 * Fréchet distance sweep continuously along their respective curves
 * and the direction of curves is significant.
 * This makes a better measure of similarity than Hausdorff distance.
 *
 * An example showing how different DHD and DFD are:
 * <pre>
 *   A  = LINESTRING (0 0, 50 200, 100 0, 150 200, 200 0)
 *   B  = LINESTRING (0 200, 200 150, 0 100, 200 50, 0 0)
 *   B' = LINESTRING (0 0, 200 50, 0 100, 200 150, 0 200)
 *
 *   DHD(A, B)  = DHD(A, B') = 48.5071250072666
 *   DFD(A, B)  = 200
 *   DFD(A, B') = 282.842712474619
 * </pre>
 */
class GEOS_DLL DiscreteFrechetDistance {
public:

    static double distance(const geom::Geometry& g0,
                           const geom::Geometry& g1);

    static double distance(const geom::Geometry& g0,
                           const geom::Geometry& g1, double densifyFrac);

    DiscreteFrechetDistance(const geom::Geometry& p_g0,
                            const geom::Geometry& p_g1)
        :
        g0(p_g0),
        g1(p_g1),
        ptDist(),
        densifyFrac(0.0)
    {}

    /**
     * Sets the fraction by which to densify each segment.
     * Each segment will be (virtually) split into a number of equal-length
     * subsegments, whose fraction of the total length is closest
     * to the given fraction.
     *
     * @param dFrac
     */
    void
    setDensifyFraction(double dFrac)
    {
        if(dFrac > 1.0 || dFrac <= 0.0) {
            throw util::IllegalArgumentException(
                "Fraction is not in range (0.0 - 1.0]");
        }

        densifyFrac = dFrac;
    }

    double
    distance()
    {
        compute(g0, g1);
        return ptDist.getDistance();
    }

    const std::array<geom::Coordinate, 2>
    getCoordinates() const
    {
        return ptDist.getCoordinates();
    }

private:
    geom::Coordinate getSegementAt(const geom::CoordinateSequence& seq, size_t index);

    PointPairDistance& getFrecheDistance(std::vector< std::vector<PointPairDistance> >& ca, size_t i, size_t j,
                                         const geom::CoordinateSequence& p, const geom::CoordinateSequence& q);

    void compute(const geom::Geometry& discreteGeom, const geom::Geometry& geom);

    const geom::Geometry& g0;

    const geom::Geometry& g1;

    PointPairDistance ptDist;

    /// Value of 0.0 indicates that no densification should take place
    double densifyFrac; // = 0.0;

    // Declare type as noncopyable
    DiscreteFrechetDistance(const DiscreteFrechetDistance& other) = delete;
    DiscreteFrechetDistance& operator=(const DiscreteFrechetDistance& rhs) = delete;
};

} // geos::algorithm::distance
} // geos::algorithm
} // geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_ALGORITHM_DISTANCE_DISCRETEFRECHETDISTANCE_H
