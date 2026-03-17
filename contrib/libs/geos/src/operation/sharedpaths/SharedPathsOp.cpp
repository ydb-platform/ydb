/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2010      Sandro Santilli <strk@kbt.io>
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
 * Developed by Sandro Santilli (strk@kbt.io)
 * for Faunalia (http://www.faunalia.it)
 * with funding from Regione Toscana - Settore SISTEMA INFORMATIVO
 * TERRITORIALE ED AMBIENTALE - for the project: "Sviluppo strumenti
 * software per il trattamento di dati geografici basati su QuantumGIS
 * e Postgis (CIG 0494241492)"
 *
 **********************************************************************/

#include <geos/operation/sharedpaths/SharedPathsOp.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/LineString.h>
#include <geos/geom/MultiLineString.h>
#include <geos/linearref/LinearLocation.h>
#include <geos/linearref/LocationIndexOfPoint.h>
#include <geos/operation/overlay/OverlayOp.h>
#include <geos/util/IllegalArgumentException.h>

using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace sharedpaths { // geos.operation.sharedpaths

/* public static */
void
SharedPathsOp::sharedPathsOp(const Geometry& g1, const Geometry& g2,
                             PathList& sameDirection,
                             PathList& oppositeDirection)
{
    SharedPathsOp sp(g1, g2);
    sp.getSharedPaths(sameDirection, oppositeDirection);
}

/* public */
SharedPathsOp::SharedPathsOp(
    const geom::Geometry& g1, const geom::Geometry& g2)
    :
    _g1(g1),
    _g2(g2),
    _gf(*g1.getFactory())
{
    checkLinealInput(_g1);
    checkLinealInput(_g2);
}

/* private */
void
SharedPathsOp::checkLinealInput(const geom::Geometry& g)
{
    if(! dynamic_cast<const LineString*>(&g) &&
            ! dynamic_cast<const MultiLineString*>(&g)) {
        throw util::IllegalArgumentException("Geometry is not lineal");
    }
}

/* public */
void
SharedPathsOp::getSharedPaths(PathList& forwDir, PathList& backDir)
{
    PathList paths;
    findLinearIntersections(paths);
    for(size_t i = 0, n = paths.size(); i < n; ++i) {
        LineString* path = paths[i];
        if(isSameDirection(*path)) {
            forwDir.push_back(path);
        }
        else {
            backDir.push_back(path);
        }
    }
}

/* static private */
void
SharedPathsOp::clearEdges(PathList& edges)
{
    for(PathList::const_iterator
            i = edges.begin(), e = edges.end();
            i != e; ++i) {
        delete *i;
    }
    edges.clear();
}

/* private */
void
SharedPathsOp::findLinearIntersections(PathList& to)
{
    using geos::operation::overlay::OverlayOp;

    // TODO: optionally use the tolerance,
    //       snapping _g2 over _g1 ?

    std::unique_ptr<Geometry> full(OverlayOp::overlayOp(
                                       &_g1, &_g2, OverlayOp::opINTERSECTION));

    // NOTE: intersection of equal lines yelds splitted lines,
    //       should we sew them back ?

    for(size_t i = 0, n = full->getNumGeometries(); i < n; ++i) {
        const Geometry* sub = full->getGeometryN(i);
        const LineString* path = dynamic_cast<const LineString*>(sub);
        if(path && ! path->isEmpty()) {
            // NOTE: we're making a copy here, wouldn't be needed
            //       for a simple predicate
            to.push_back(_gf.createLineString(*path).release());
        }
    }
}

/* private */
bool
SharedPathsOp::isForward(const geom::LineString& edge,
                         const geom::Geometry& geom)
{
    using namespace geos::linearref;

    /*
       ALGO:
        1. find first point of edge on geom (linearref)
        2. find second point of edge on geom (linearref)
        3. if first < second, we're forward

       PRECONDITIONS:
        1. edge has at least 2 points
        2. edge first two points are not equal
        3. geom is simple
     */

    const Coordinate& pt1 = edge.getCoordinateN(0);
    const Coordinate& pt2 = edge.getCoordinateN(1);

    /*
     * We move the coordinate somewhat closer, to avoid
     * vertices of the geometry being checked (geom).
     *
     * This is mostly only needed when one of the two points
     * of the edge is an endpoint of a _closed_ geom.
     * We have an unit test for this...
     */
    Coordinate pt1i = LinearLocation::pointAlongSegmentByFraction(pt1, pt2, 0.1);
    Coordinate pt2i = LinearLocation::pointAlongSegmentByFraction(pt1, pt2, 0.9);

    LinearLocation l1 = LocationIndexOfPoint::indexOf(&geom, pt1i);
    LinearLocation l2 = LocationIndexOfPoint::indexOf(&geom, pt2i);

    return l1.compareTo(l2) < 0;
}

} // namespace geos.operation.sharedpaths
} // namespace geos::operation
} // namespace geos

