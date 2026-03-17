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

#include <geos/operation/overlayng/OverlayNGRobust.h>

#include <geos/operation/overlayng/OverlayNG.h>
#include <geos/operation/overlayng/OverlayUtil.h>
#include <geos/operation/overlayng/PrecisionUtil.h>
#include <geos/operation/union/UnionStrategy.h>
#include <geos/operation/union/UnaryUnionOp.h>
#include <geos/noding/snap/SnappingNoder.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/util/TopologyException.h>

#include <stdexcept>

#ifndef GEOS_DEBUG
# define GEOS_DEBUG 0
#endif

namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;


/*public static*/
std::unique_ptr<Geometry>
OverlayNGRobust::Intersection(const Geometry* g0, const Geometry* g1)
{
    return Overlay(g0, g1, OverlayNG::INTERSECTION);
}

/*public static*/
std::unique_ptr<Geometry>
OverlayNGRobust::Union(const Geometry* g0, const Geometry* g1)
{
    return Overlay(g0, g1, OverlayNG::UNION);
}

/*public static*/
std::unique_ptr<Geometry>
OverlayNGRobust::Difference(const Geometry* g0, const Geometry* g1)
{
    return Overlay(g0, g1, OverlayNG::DIFFERENCE);
}

/*public static*/
std::unique_ptr<Geometry>
OverlayNGRobust::SymDifference(const Geometry* g0, const Geometry* g1)
{
    return Overlay(g0, g1, OverlayNG::SYMDIFFERENCE);
}

/*public static*/
std::unique_ptr<Geometry>
OverlayNGRobust::Union(const Geometry* a)
{
    geounion::UnaryUnionOp op(*a);
    SRUnionStrategy unionSRFun;
    op.setUnionFunction(&unionSRFun);
    return op.Union();
}

/*public static*/
std::unique_ptr<Geometry>
OverlayNGRobust::Overlay(const Geometry* geom0, const Geometry* geom1, int opCode)
{
    std::unique_ptr<Geometry> result;
    std::runtime_error exOriginal("");

    /**
    * If input geometry has a non-floating precision model, just run
    * in snap-rounding mode with that precision.
    */
    if (!geom0->getPrecisionModel()->isFloating()) {
#if GEOS_DEBUG
        std::cout << "Using fixed precision overlay." << std::endl;
#endif
        return OverlayNG::overlay(geom0, geom1, opCode, geom0->getPrecisionModel());
    }

    /**
     * First try overlay with a FLOAT noder, which is fastest and causes least
     * change to geometry coordinates
     * By default the noder is validated, which is required in order
     * to detect certain invalid noding situations which otherwise
     * cause incorrect overlay output.
     */
    try {
        geom::PrecisionModel PM_FLOAT;
        // std::cout << "Using floating point overlay." << std::endl;
        result = OverlayNG::overlay(geom0, geom1, opCode, &PM_FLOAT);

        // Simple noding with no validation
        // There are cases where this succeeds with invalid noding (e.g. STMLF 1608).
        // So currently it is NOT safe to run overlay without noding validation
        //result = OverlayNG.overlay(geom0, geom1, opCode, createFloatingNoValidNoder());
        // std::cout << "Floating point overlay success." << std::endl;
        return result;
    }
    catch (const std::runtime_error &ex) {
        /**
        * Capture original exception,
        * so it can be rethrown if the remaining strategies all fail.
        */
        exOriginal = ex;
#if GEOS_DEBUG
        std::cout << "Floating point overlay FAILURE: " << ex.what() << std::endl;
#endif
    }

    /**
     * On failure retry using snapping noding with a "safe" tolerance.
     * if this throws an exception just let it go,
     * since it is something that is not a TopologyException
     */
    result = overlaySnapTries(geom0, geom1, opCode);
    if (result != nullptr)
        return result;

    /**
     * On failure retry using snap-rounding with a heuristic scale factor (grid size).
     */
    result = overlaySR(geom0, geom1, opCode);
    if (result != nullptr)
        return result;

    /**
     * Just can't get overlay to work, so throw original error.
     */
    throw exOriginal;
}


/*private static*/
std::unique_ptr<Geometry>
OverlayNGRobust::overlaySnapTries(const Geometry* geom0, const Geometry* geom1, int opCode)
{
    std::unique_ptr<Geometry> result;
    double snapTol = snapTolerance(geom0, geom1);

    for (std::size_t i = 0; i < NUM_SNAP_TRIES; i++) {

#if GEOS_DEBUG
        std::cout << "Trying overlaySnapping(tol " << snapTol << ")." << std::endl;
#endif

        result = overlaySnapping(geom0, geom1, opCode, snapTol);
        if (result != nullptr) return result;

      /**
       * Now try snapping each input individually,
       * and then doing the overlay.
       */
      result = overlaySnapBoth(geom0, geom1, opCode, snapTol);
      if (result != nullptr) return result;

      // increase the snap tolerance and try again
      snapTol = snapTol * 10;
    }
    // failed to compute overlay
    return nullptr;
}

/*private static*/
std::unique_ptr<Geometry>
OverlayNGRobust::overlaySnapping(const Geometry* geom0, const Geometry* geom1, int opCode, double snapTol)
{
    try {
        return overlaySnapTol(geom0, geom1, opCode, snapTol);
    }
    catch (const geos::util::TopologyException& ex)
    {
        ::geos::ignore_unused_variable_warning(ex);
        //---- ignore exception, return null result to indicate failure
#if GEOS_DEBUG
        std::cout << "overlaySnapping(tol " << snapTol << ") FAILURE: " << ex.what() << std::endl;
#endif
    }
    return nullptr;
}

/*private static*/
std::unique_ptr<Geometry>
OverlayNGRobust::overlaySnapBoth(const Geometry* geom0, const Geometry* geom1, int opCode, double snapTol)
{
    try {
        std::unique_ptr<Geometry> snap0 = snapSelf(geom0, snapTol);
        std::unique_ptr<Geometry> snap1 = snapSelf(geom1, snapTol);
        return overlaySnapTol(snap0.get(), snap1.get(), opCode, snapTol);
    }
    catch (const geos::util::TopologyException& ex)
    {
        ::geos::ignore_unused_variable_warning(ex);
        //---- ignore this exception, just return a nullptr result to indicate failure
#if GEOS_DEBUG
        std::cout << "overlaySnapBoth(tol " << snapTol << ") FAILURE: " << ex.what() << std::endl;
#endif
    }
    return nullptr;
}

/*private static*/
std::unique_ptr<Geometry>
OverlayNGRobust::snapSelf(const Geometry* geom, double snapTol)
{
	OverlayNG ov(geom, nullptr);
	noding::snap::SnappingNoder snapNoder(snapTol);
	ov.setNoder(&snapNoder);
	/**
	 * Ensure the result is not mixed-dimension,
	 * since it will be used in further overlay computation.
	 * It may however be lower dimension, if it collapses completely due to snapping.
	 */
	ov.setStrictMode(true);
	return ov.getResult();
}

/*private static*/
std::unique_ptr<Geometry>
OverlayNGRobust::overlaySnapTol(const Geometry* geom0, const Geometry* geom1, int opCode, double snapTol)
{
    noding::snap::SnappingNoder snapNoder(snapTol);
    return OverlayNG::overlay(geom0, geom1, opCode, &snapNoder);
}

/*public static*/
double
OverlayNGRobust::snapTolerance(const Geometry* geom0, const Geometry* geom1)
{
    double tol0 = snapTolerance(geom0);
    double tol1 = snapTolerance(geom1);
    double snapTol = std::max(tol0,  tol1);
    return snapTol;
}

/*private static*/
double
OverlayNGRobust::snapTolerance(const Geometry* geom)
{
    double magnitude = ordinateMagnitude(geom);
    return magnitude / SNAP_TOL_FACTOR;
}


/*private static*/
double
OverlayNGRobust::ordinateMagnitude(const Geometry* geom)
{
    if (geom == nullptr) return 0;
    const Envelope* env = geom->getEnvelopeInternal();
    double magMax = std::max(
        std::abs(env->getMaxX()),
        std::abs(env->getMaxY())
        );
    double magMin = std::max(
        std::abs(env->getMinX()),
        std::abs(env->getMinY())
        );
    return std::max(magMax, magMin);
}


/*private static*/
std::unique_ptr<Geometry>
OverlayNGRobust::overlaySR(const Geometry* geom0, const Geometry* geom1, int opCode)
{
    std::unique_ptr<Geometry> result;
    try {
        double scaleSafe = PrecisionUtil::safeScale(geom0, geom1);
        PrecisionModel pmSafe(scaleSafe);
        result = OverlayNG::overlay(geom0, geom1, opCode, &pmSafe);
        return result;
    }
    catch (const geos::util::TopologyException& ex)
    {
        ::geos::ignore_unused_variable_warning(ex);
        // ignore this exception, since the operation will be rerun
#if GEOS_DEBUG
        std::cout << "OverlayNGRobust::overlaySR FAILURE: " << ex.what() << std::endl;
#endif
    }
    return nullptr;
}






} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
