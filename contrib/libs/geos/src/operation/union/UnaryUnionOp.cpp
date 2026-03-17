/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/union/UnaryUnionOp.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <memory> // for unique_ptr
#include <cassert> // for assert
#include <algorithm> // for copy

#include <geos/operation/union/UnaryUnionOp.h>
#include <geos/operation/union/CascadedUnion.h>
#include <geos/operation/union/CascadedPolygonUnion.h>
#include <geos/operation/union/PointGeometryUnion.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Point.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Location.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/util/GeometryCombiner.h>
#include <geos/algorithm/PointLocator.h>

namespace geos {
namespace operation { // geos::operation
namespace geounion {  // geos::operation::geounion

/*private*/
std::unique_ptr<geom::Geometry>
UnaryUnionOp::unionWithNull(std::unique_ptr<geom::Geometry> g0,
                            std::unique_ptr<geom::Geometry> g1)
{
    std::unique_ptr<geom::Geometry> ret;
    if((! g0.get()) && (! g1.get())) {
        return ret;
    }

    if(! g0.get()) {
        return g1;
    }
    if(! g1.get()) {
        return g0;
    }

    ret = g0->Union(g1.get());
    return ret;
}

/*public*/
std::unique_ptr<geom::Geometry>
UnaryUnionOp::Union()
{
    typedef std::unique_ptr<geom::Geometry> GeomPtr;

    GeomPtr ret;
    if(! geomFact) {
        return ret;
    }

    /*
     * For points and lines, only a single union operation is
     * required, since the OGC model allowings self-intersecting
     * MultiPoint and MultiLineStrings.
     * This is not the case for polygons, so Cascaded Union is required.
     */

    GeomPtr unionPoints;
    if(!points.empty()) {
        GeomPtr ptGeom = geomFact->buildGeometry(points.begin(),
                         points.end());
        unionPoints = unionNoOpt(*ptGeom);
    }

    GeomPtr unionLines;
    if(!lines.empty()) {
        /* JTS compatibility NOTE:
         * we use cascaded here for robustness [1]
         * but also add a final unionNoOpt step to deal with
         * self-intersecting lines [2]
         *
         * [1](http://trac.osgeo.org/geos/ticket/392
         * [2](http://trac.osgeo.org/geos/ticket/482
         *
         */
        try {
            auto combinedLines = geomFact->buildGeometry(lines.begin(), lines.end());
            unionLines = unionNoOpt(*combinedLines);
        } catch (geos::util::TopologyException &) {
            unionLines.reset(CascadedUnion::Union(lines.begin(),
                                                  lines.end()));
            if(unionLines.get()) {
                unionLines = unionNoOpt(*unionLines);
            }
        }
    }

    GeomPtr unionPolygons;
    if(!polygons.empty()) {
        unionPolygons.reset(CascadedPolygonUnion::Union(polygons.begin(),
                            polygons.end(), unionFunction));
    }

    /*
     * Performing two unions is somewhat inefficient,
     * but is mitigated by unioning lines and points first
     */

    GeomPtr unionLA = unionWithNull(std::move(unionLines), std::move(unionPolygons));


    if(! unionPoints.get()) {
        ret = std::move(unionLA);
    }
    else if(! unionLA.get()) {
        ret = std::move(unionPoints);
    }
    else {
        ret = PointGeometryUnion::Union(*unionPoints, *unionLA);
    }

    if(! ret.get()) {
        ret = geomFact->createGeometryCollection();
    }

    return ret;

}

} // namespace geos::operation::union
} // namespace geos::operation
} // namespace geos
