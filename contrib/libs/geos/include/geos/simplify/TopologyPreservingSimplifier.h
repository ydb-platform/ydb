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
 * Last port: simplify/TopologyPreservingSimplifier.java r536 (JTS-1.12+)
 *
 **********************************************************************
 *
 * NOTES:
 *
 **********************************************************************/

#ifndef GEOS_SIMPLIFY_TOPOLOGYPRESERVINGSIMPLIFIER_H
#define GEOS_SIMPLIFY_TOPOLOGYPRESERVINGSIMPLIFIER_H

#include <geos/export.h>
#include <geos/geom/Geometry.h>
#include <geos/simplify/TaggedLinesSimplifier.h>
#include <memory> // for unique_ptr
#include <map>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace simplify { // geos::simplify

/** \brief
 * Simplifies a geometry, ensuring that
 * the result is a valid geometry having the
 * same dimension and number of components as the input.
 *
 * The simplification uses a maximum distance difference algorithm
 * similar to the one used in the Douglas-Peucker algorithm.
 *
 * In particular, if the input is an areal geometry
 * ( Polygon or MultiPolygon )
 *
 *  -  The result has the same number of shells and holes (rings) as the input,
 *     in the same order
 *  -  The result rings touch at <b>no more</b> than the number of touching point in the input
 *     (although they may touch at fewer points)
 *
 */
class GEOS_DLL TopologyPreservingSimplifier {

public:

    static std::unique_ptr<geom::Geometry> simplify(
        const geom::Geometry* geom,
        double tolerance);

    TopologyPreservingSimplifier(const geom::Geometry* geom);

    /** \brief
     * Sets the distance tolerance for the simplification.
     *
     * All vertices in the simplified geometry will be within this
     * distance of the original geometry.
     * The tolerance value must be non-negative.  A tolerance value
     * of zero is effectively a no-op.
     *
     * @param tolerance the approximation tolerance to use
     */
    void setDistanceTolerance(double tolerance);

    std::unique_ptr<geom::Geometry> getResultGeometry();

private:

    const geom::Geometry* inputGeom;

    std::unique_ptr<TaggedLinesSimplifier> lineSimplifier;

};

} // namespace geos::simplify
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_SIMPLIFY_TOPOLOGYPRESERVINGSIMPLIFIER_H
