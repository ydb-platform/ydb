/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2010 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/polygonize/Polygonizer.java rev. 1.6 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/geom/util/Densifier.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/CoordinateList.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineSegment.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/util/Interrupt.h>
#include <geos/util/IllegalArgumentException.h>

#include <vector>

using namespace geos::geom;
using namespace geos::geom::util;

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

/* geom::util::Densifier::DensifyTransformer */
Densifier::DensifyTransformer::DensifyTransformer(double distTol):
    distanceTolerance(distTol)
{}

CoordinateSequence::Ptr
Densifier::DensifyTransformer::transformCoordinates(const CoordinateSequence* coords, const Geometry* parent)
{
    Coordinate::Vect emptyPts;
    Coordinate::Vect inputPts;
    coords->toVector(inputPts);
    std::unique_ptr<Coordinate::Vect> newPts = Densifier::densifyPoints(inputPts, distanceTolerance,
            parent->getPrecisionModel());
    if(const LineString* ls = dynamic_cast<const LineString*>(parent)) {
        if(ls->getNumPoints() <= 1) {
            newPts->clear();
        }
    }
    CoordinateSequence::Ptr csp(factory->getCoordinateSequenceFactory()->create(newPts.release()));
    return csp;
}

Geometry::Ptr
Densifier::DensifyTransformer::transformPolygon(const Polygon* geom, const Geometry* parent)
{
    Geometry::Ptr roughGeom = GeometryTransformer::transformPolygon(geom, parent);
    // don't try and correct if the parent is going to do this
    if(parent && parent->getGeometryTypeId() == GEOS_MULTIPOLYGON) {
        return roughGeom;
    }
    Geometry::Ptr validGeom(createValidArea(roughGeom.get()));
    return validGeom;
}

Geometry::Ptr
Densifier::DensifyTransformer::transformMultiPolygon(const MultiPolygon* geom, const Geometry* parent)
{
    Geometry::Ptr roughGeom = GeometryTransformer::transformMultiPolygon(geom, parent);
    Geometry::Ptr validGeom(createValidArea(roughGeom.get()));
    return validGeom;
}

std::unique_ptr<Geometry>
Densifier::DensifyTransformer::createValidArea(const Geometry* roughAreaGeom)
{
    return roughAreaGeom->buffer(0.0);
}

/* util::Densifier */

Densifier::Densifier(const Geometry* geom):
    inputGeom(geom)
{}

std::unique_ptr<Coordinate::Vect>
Densifier::densifyPoints(const Coordinate::Vect pts, double distanceTolerance, const PrecisionModel* precModel)
{
    geom::LineSegment seg;
    geom::CoordinateList coordList;

    for(Coordinate::Vect::const_iterator it = pts.begin(), itEnd = pts.end() - 1; it < itEnd; ++it) {
        seg.p0 = *it;
        seg.p1 = *(it + 1);
        coordList.insert(coordList.end(), seg.p0, false);
        double len = seg.getLength();
        int densifiedSegCount = (int)(len / distanceTolerance) + 1;
        if(densifiedSegCount > 1) {
            double densifiedSegLen = len / densifiedSegCount;
            for(int j = 1; j < densifiedSegCount; j++) {
                double segFract = (j * densifiedSegLen) / len;
                Coordinate p;
                seg.pointAlong(segFract, p);
                precModel->makePrecise(p);
                coordList.insert(coordList.end(), p, false);
            }
        }
    }
    coordList.insert(coordList.end(), pts[pts.size() - 1], false);
    return coordList.toCoordinateArray();
}

/**
 * Densifies a geometry using a given distance tolerance,
 * and respecting the input geometry's {@link PrecisionModel}.
 *
 * @param geom the geometry to densify
 * @param distanceTolerance the distance tolerance to densify
 * @return the densified geometry
 */
Geometry::Ptr
Densifier::densify(const Geometry* geom, double distTol)
{
    util::Densifier densifier(geom);
    densifier.setDistanceTolerance(distTol);
    return densifier.getResultGeometry();
}

void
Densifier::setDistanceTolerance(double tol)
{
    if(tol <= 0.0) {
        throw geos::util::IllegalArgumentException("Tolerance must be positive");
    }
    distanceTolerance = tol;
}

Geometry::Ptr
Densifier::getResultGeometry() const
{
    DensifyTransformer dt(distanceTolerance);
    return dt.transform(inputGeom);
}


} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos
