/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://trac.osgeo.org/geos
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: ORIGINAL WORK, generalization of CascadedPolygonUnion
 *
 **********************************************************************/

#ifndef GEOS_OP_UNION_OVERLAPUNION_H
#define GEOS_OP_UNION_OVERLAPUNION_H

#include <geos/export.h>

#include <vector>
#include <algorithm>
#include <unordered_set>

#include <geos/geom/Geometry.h>
#include <geos/operation/union/UnionStrategy.h>
#include <geos/operation/union/CascadedPolygonUnion.h>

// Forward declarations
namespace geos {
namespace geom {
class Envelope;
class LineSegment;
}
}

namespace geos {
namespace operation { // geos::operation
namespace geounion {  // geos::operation::geounion

/** \brief
 * Unions MultiPolygons efficiently by using full topological union only
 * for polygons which may overlap by virtue of intersecting the common
 * area of the inputs.
 *
 * Other polygons are simply combined with the union result, which is much
 * more performant.
 *
 * This situation is likely to occur during cascaded polygon union,
 * since the partitioning of polygons is done heuristically
 * and thus may group disjoint polygons which can lie far apart.
 * It may also occur in real world data which contains many disjoint polygons
 * (e.g. polygons representing parcels on different street blocks).
 *
 * # Algorithm
 *
 * The overlap region is determined as the common envelope of intersection.
 * The input polygons are partitioned into two sets:
 *
 * * Overlapping: Polygons which intersect the overlap region, and thus potentially overlap each other
 * * Disjoint: Polygons which are disjoint from (lie wholly outside) the overlap region
 *
 * The Overlapping set is fully unioned, and then combined with the Disjoint set.
 * Performing a simple combine works because
 * the disjoint polygons do not interact with each
 * other (since the inputs are valid MultiPolygons).
 * They also do not interact with the Overlapping polygons,
 * since they are outside their envelope.
 *
 * # Verification
 *
 * In the general case the Overlapping set of polygons will
 * extend beyond the overlap envelope.  This means that the union result
 * will extend beyond the overlap region.
 * There is a small chance that the topological
 * union of the overlap region will shift the result linework enough
 * that the result geometry intersects one of the Disjoint geometries.
 * This case is detected and if it occurs
 * is remedied by falling back to performing a full union of the original inputs.
 * Detection is done by a fairly efficient comparison of edge segments which
 * extend beyond the overlap region.  If any segments have changed
 * then there is a risk of introduced intersections, and full union is performed.
 *
 * This situation has not been observed in JTS using floating precision,
 * but it could happen due to snapping.  It has been observed
 * in other APIs (e.g. GEOS) due to more aggressive snapping.
 * And it will be more likely to happen if a snap-rounding overlay is used.
 *
 * DEPRECATED: This optimization has been removed, since it impairs performance.
 *
 * @author mbdavis
 *
 */
class GEOS_DLL OverlapUnion {

public:

    OverlapUnion(const geom::Geometry* p_g0, const geom::Geometry* p_g1, geounion::UnionStrategy* unionFun)
        : g0(p_g0)
        , g1(p_g1)
        , unionFunction(unionFun)
        , geomFactory(p_g0->getFactory())
        , isUnionSafe(false)
        {};

    OverlapUnion(const geom::Geometry* p_g0, const geom::Geometry* p_g1)
        : OverlapUnion(p_g0, p_g1, &defaultUnionFunction)
        {};


    std::unique_ptr<geom::Geometry> doUnion();

private:

    const geom::Geometry* g0;
    const geom::Geometry* g1;
    geounion::UnionStrategy* unionFunction;
    const geom::GeometryFactory* geomFactory;
    bool isUnionSafe;

    geounion::ClassicUnionStrategy defaultUnionFunction;

    geom::Envelope overlapEnvelope(const geom::Geometry* geom0, const geom::Geometry* geom1);
    std::unique_ptr<geom::Geometry> extractByEnvelope(const geom::Envelope& env, const geom::Geometry* geom, std::vector<std::unique_ptr<geom::Geometry>>& disjointGeoms);
    std::unique_ptr<geom::Geometry> combine(std::unique_ptr<geom::Geometry>& unionGeom, std::vector<std::unique_ptr<geom::Geometry>>& disjointPolys);
    std::unique_ptr<geom::Geometry> unionFull(const geom::Geometry* geom0, const geom::Geometry* geom1);
    std::unique_ptr<geom::Geometry> unionBuffer(const geom::Geometry* geom0, const geom::Geometry* geom1);
    bool isBorderSegmentsSame(const geom::Geometry* result, const geom::Envelope& env);
    bool isEqual(std::vector<geom::LineSegment>& segs0, std::vector<geom::LineSegment>& segs1);
    std::vector<geom::LineSegment> extractBorderSegments(const geom::Geometry* geom0, const geom::Geometry* geom1, const geom::Envelope& env);
    void extractBorderSegments(const geom::Geometry* geom, const geom::Envelope& penv, std::vector<geom::LineSegment>& psegs);

};

} // namespace geos::operation::union
} // namespace geos::operation
} // namespace geos

#endif
