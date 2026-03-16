/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009  Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/distance/DiscreteHausdorffDistance.java 1.5 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_DISTANCE_DISCRETEHAUSDORFFDISTANCE_H
#define GEOS_ALGORITHM_DISTANCE_DISCRETEHAUSDORFFDISTANCE_H

#include <geos/export.h>
#include <geos/algorithm/distance/PointPairDistance.h> // for composition
#include <geos/algorithm/distance/DistanceToPoint.h> // for composition
#include <geos/util/IllegalArgumentException.h> // for inlines
#include <geos/geom/Geometry.h> // for inlines
#include <geos/util/math.h> // for inlines
#include <geos/geom/CoordinateFilter.h> // for inheritance
#include <geos/geom/CoordinateSequenceFilter.h> // for inheritance

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
 * which is an approximation to the Hausdorff Distance
 * based on a discretization of the input {@link geom::Geometry}.
 *
 * The algorithm computes the Hausdorff distance restricted to discrete points
 * for one of the geometries.
 * The points can be either the vertices of the geometries (the default),
 * or the geometries with line segments densified by a given fraction.
 * Also determines two points of the Geometries which are separated by the
 * computed distance.
 *
 * This algorithm is an approximation to the standard Hausdorff distance.
 * Specifically,
 * <pre>
 *    for all geometries a, b:    DHD(a, b) <= HD(a, b)
 * </pre>
 * The approximation can be made as close as needed by densifying the
 * input geometries.
 * In the limit, this value will approach the true Hausdorff distance:
 * <pre>
 *    DHD(A, B, densifyFactor) -> HD(A, B) as densifyFactor -> 0.0
 * </pre>
 * The default approximation is exact or close enough for a large subset of
 * useful cases.
 * Examples of these are:
 *
 * - computing distance between Linestrings that are roughly parallel to
 *   each other, and roughly equal in length.  This occurs in matching
 *   linear networks.
 * - Testing similarity of geometries.
 *
 * An example where the default approximation is not close is:
 * <pre>
 *   A = LINESTRING (0 0, 100 0, 10 100, 10 100)
 *   B = LINESTRING (0 100, 0 10, 80 10)
 *
 *   DHD(A, B) = 22.360679774997898
 *   HD(A, B) ~= 47.8
 * </pre>
 */
class GEOS_DLL DiscreteHausdorffDistance {
public:

    static double distance(const geom::Geometry& g0,
                           const geom::Geometry& g1);

    static double distance(const geom::Geometry& g0,
                           const geom::Geometry& g1, double densifyFrac);

    DiscreteHausdorffDistance(const geom::Geometry& p_g0,
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

    double
    orientedDistance()
    {
        computeOrientedDistance(g0, g1, ptDist);
        return ptDist.getDistance();
    }

    const std::array<geom::Coordinate, 2>
    getCoordinates() const
    {
        return ptDist.getCoordinates();
    }

    class MaxPointDistanceFilter : public geom::CoordinateFilter {
    public:
        MaxPointDistanceFilter(const geom::Geometry& p_geom)
            :
            geom(p_geom)
        {}

        void
        filter_ro(const geom::Coordinate* pt) override
        {
            minPtDist.initialize();
            DistanceToPoint::computeDistance(geom, *pt,
                                             minPtDist);
            maxPtDist.setMaximum(minPtDist);
        }

        const PointPairDistance&
        getMaxPointDistance() const
        {
            return maxPtDist;
        }

    private:
        PointPairDistance maxPtDist;
        PointPairDistance minPtDist;
        DistanceToPoint euclideanDist;
        const geom::Geometry& geom;

        // Declare type as noncopyable
        MaxPointDistanceFilter(const MaxPointDistanceFilter& other);
        MaxPointDistanceFilter& operator=(const MaxPointDistanceFilter& rhs);
    };

    class MaxDensifiedByFractionDistanceFilter
        : public geom::CoordinateSequenceFilter {
    public:

        MaxDensifiedByFractionDistanceFilter(
            const geom::Geometry& p_geom, double fraction)
            :
            geom(p_geom),
            numSubSegs(std::size_t(util::round(1.0 / fraction)))
        {
        }

        void filter_ro(const geom::CoordinateSequence& seq,
                       std::size_t index) override;

        bool
        isGeometryChanged() const override
        {
            return false;
        }

        bool
        isDone() const override
        {
            return false;
        }

        const PointPairDistance&
        getMaxPointDistance() const
        {
            return maxPtDist;
        }

    private:
        PointPairDistance maxPtDist;
        PointPairDistance minPtDist;
        const geom::Geometry& geom;
        std::size_t numSubSegs; // = 0;

        // Declare type as noncopyable
        MaxDensifiedByFractionDistanceFilter(const MaxDensifiedByFractionDistanceFilter& other);
        MaxDensifiedByFractionDistanceFilter& operator=(const MaxDensifiedByFractionDistanceFilter& rhs);
    };

private:

    void
    compute(const geom::Geometry& p_g0,
            const geom::Geometry& p_g1)
    {
        computeOrientedDistance(p_g0, p_g1, ptDist);
        computeOrientedDistance(p_g1, p_g0, ptDist);
    }

    void computeOrientedDistance(const geom::Geometry& discreteGeom,
                                 const geom::Geometry& geom,
                                 PointPairDistance& ptDist);

    const geom::Geometry& g0;

    const geom::Geometry& g1;

    PointPairDistance ptDist;

    /// Value of 0.0 indicates that no densification should take place
    double densifyFrac; // = 0.0;

    // Declare type as noncopyable
    DiscreteHausdorffDistance(const DiscreteHausdorffDistance& other) = delete;
    DiscreteHausdorffDistance& operator=(const DiscreteHausdorffDistance& rhs) = delete;
};

} // geos::algorithm::distance
} // geos::algorithm
} // geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_ALGORITHM_DISTANCE_DISCRETEHAUSDORFFDISTANCE_H

