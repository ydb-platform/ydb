/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/overlayng/OverlayNGRobust.java 6ef89b09
 *
 **********************************************************************/

#pragma once

#include <geos/export.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/operation/union/UnionStrategy.h>
#include <geos/operation/overlayng/OverlayNG.h>


// Forward declarations
namespace geos {
namespace geom {
class Geometry;
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


/**
 * Performs an overlay operation, increasing robustness by using a series of
 * increasingly aggressive (and slower) noding strategies.
 *
 * The noding strategies used are:
 *
 *  - A simple, fast noder using FLOATING precision.
 *  - A {@link noding::snap::SnappingNoder} using an automatically-determined snap tolerance
 *  - First snapping each geometry to itself,
 *    and then overlaying them using a SnappingNoder.
 *  - The above two strategies are repeated with increasing snap tolerance, up to a limit.
 *
 * If the above heuristics still fail to compute a valid overlay,
 * the original {@link util::TopologyException} is thrown.
 *
 * This algorithm relies on each overlay operation execution
 * throwing a {@link util::TopologyException} if it is unable
 * to compute the overlay correctly.
 * Generally this occurs because the noding phase does
 * not produce a valid noding.
 * This requires the use of a {@link noding::ValidatingNoder}
 * in order to check the results of using a floating noder.
 *
 * @author Martin Davis
 */
class GEOS_DLL OverlayNGRobust {

private:


    // Constants
    static constexpr int NUM_SNAP_TRIES = 5;
    /**
    * A factor for a snapping tolerance distance which
    * should allow noding to be computed robustly.
    */
    static constexpr double SNAP_TOL_FACTOR = 1e12;

    static std::unique_ptr<Geometry> overlaySnapping(
        const Geometry* geom0, const Geometry* geom1, int opCode, double snapTol);

    static std::unique_ptr<Geometry> overlaySnapBoth(
        const Geometry* geom0, const Geometry* geom1, int opCode, double snapTol);

    static std::unique_ptr<Geometry> overlaySnapTol(
        const Geometry* geom0, const Geometry* geom1, int opCode, double snapTol);

    static double snapTolerance(const Geometry* geom);

    /**
    * Computes the largest magnitude of the ordinates of a geometry,
    * based on the geometry envelope.
    *
    * @param geom a geometry
    * @return the magnitude of the largest ordinate
    */
    static double ordinateMagnitude(const Geometry* geom);

    /**
    * Overlay using Snap-Rounding with an automatically-determined
    * scale factor.
    *
    * NOTE: currently this strategy is not used, since all known
    * test cases work using one of the Snapping strategies.
    */
    static std::unique_ptr<Geometry>
    overlaySR(const Geometry* geom0, const Geometry* geom1, int opCode);

	/**
   	 * Self-snaps a geometry by running a union operation with it as the only input.
   	 * This helps to remove narrow spike/gore artifacts to simplify the geometry,
   	 * which improves robustness.
   	 * Collapsed artifacts are removed from the result to allow using
   	 * it in further overlay operations.
   	 *
   	 * @param geom geometry to self-snap
   	 * @param snapTol snap tolerance
   	 * @return the snapped geometry (homogeneous)
   	 */
	static std::unique_ptr<Geometry>
	snapSelf(const Geometry* geom, double snapTol);


public:

    class SRUnionStrategy : public operation::geounion::UnionStrategy {

        std::unique_ptr<geom::Geometry> Union(const geom::Geometry* g0, const geom::Geometry* g1) override
        {
            return OverlayNGRobust::Overlay(g0, g1, OverlayNG::UNION);
        };

        bool isFloatingPrecision() const override
        {
            return true;
        };

    };

    static std::unique_ptr<Geometry> Intersection(
        const Geometry* g0, const Geometry* g1);

    static std::unique_ptr<Geometry> Union(
        const Geometry* g0, const Geometry* g1);

    static std::unique_ptr<Geometry> Difference(
        const Geometry* g0, const Geometry* g1);

    static std::unique_ptr<Geometry> SymDifference(
        const Geometry* g0, const Geometry* g1);

    static std::unique_ptr<Geometry> Union(
        const Geometry* a);

    static std::unique_ptr<Geometry> Overlay(
        const Geometry* geom0, const Geometry* geom1, int opCode);

    static std::unique_ptr<Geometry> overlaySnapTries(
        const Geometry* geom0, const Geometry* geom1, int opCode);

    /**
    * Computes a heuristic snap tolerance distance
    * for overlaying a pair of geometries using a {@link noding::snap::SnappingNoder}.
    */
    static double snapTolerance(const Geometry* geom0, const Geometry* geom1);




};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

