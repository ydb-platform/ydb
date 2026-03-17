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
 * Last port: simplify/DouglasPeuckerSimplifier.java rev. 1.5 (JTS-1.7)
 *
 **********************************************************************/

#include <geos/simplify/DouglasPeuckerSimplifier.h>
#include <geos/simplify/DouglasPeuckerLineSimplifier.h>
#include <geos/geom/Geometry.h> // for Ptr typedefs
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/CoordinateSequence.h> // for Ptr typedefs
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/util/GeometryTransformer.h> // for DPTransformer inheritance
#include <geos/util/IllegalArgumentException.h>
#include <geos/util.h>

#include <memory> // for unique_ptr
#include <cassert>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

using namespace geos::geom;

namespace geos {
namespace simplify { // geos::simplify

class DPTransformer: public geom::util::GeometryTransformer {

public:

    DPTransformer(double tolerance);

protected:

    CoordinateSequence::Ptr transformCoordinates(
        const CoordinateSequence* coords,
        const Geometry* parent) override;

    Geometry::Ptr transformPolygon(
        const Polygon* geom,
        const Geometry* parent) override;

    Geometry::Ptr transformMultiPolygon(
        const MultiPolygon* geom,
        const Geometry* parent) override;

private:

    /*
     * Creates a valid area geometry from one that possibly has
     * bad topology (i.e. self-intersections).
     * Since buffer can handle invalid topology, but always returns
     * valid geometry, constructing a 0-width buffer "corrects" the
     * topology.
     * Note this only works for area geometries, since buffer always returns
     * areas.  This also may return empty geometries, if the input
     * has no actual area.
     *
     * @param roughAreaGeom an area geometry possibly containing
     *        self-intersections
     * @return a valid area geometry
     */
    Geometry::Ptr createValidArea(const Geometry* roughAreaGeom);

    double distanceTolerance;

};

DPTransformer::DPTransformer(double t)
    :
    distanceTolerance(t)
{
    setSkipTransformedInvalidInteriorRings(true);
}

Geometry::Ptr
DPTransformer::createValidArea(const Geometry* roughAreaGeom)
{
    return Geometry::Ptr(roughAreaGeom->buffer(0.0));
}

CoordinateSequence::Ptr
DPTransformer::transformCoordinates(
    const CoordinateSequence* coords,
    const Geometry* parent)
{
    ::geos::ignore_unused_variable_warning(parent);

    Coordinate::Vect inputPts;
    coords->toVector(inputPts);

    std::unique_ptr<Coordinate::Vect> newPts =
        DouglasPeuckerLineSimplifier::simplify(inputPts, distanceTolerance);

    return CoordinateSequence::Ptr(
               factory->getCoordinateSequenceFactory()->create(
                   newPts.release()
               ));
}

Geometry::Ptr
DPTransformer::transformPolygon(
    const Polygon* geom,
    const Geometry* parent)
{

#if GEOS_DEBUG
    std::cerr << "DPTransformer::transformPolygon(Polygon " << geom << ", Geometry " << parent << ");" << std::endl;
#endif

    Geometry::Ptr roughGeom(GeometryTransformer::transformPolygon(geom, parent));

    // don't try and correct if the parent is going to do this
    if(dynamic_cast<const MultiPolygon*>(parent)) {
        return roughGeom;
    }

    return createValidArea(roughGeom.get());
}

Geometry::Ptr
DPTransformer::transformMultiPolygon(
    const MultiPolygon* geom,
    const Geometry* parent)
{
#if GEOS_DEBUG
    std::cerr << "DPTransformer::transformMultiPolygon(MultiPolygon " << geom << ", Geometry " << parent << ");" <<
              std::endl;
#endif
    Geometry::Ptr roughGeom(GeometryTransformer::transformMultiPolygon(geom, parent));
    return createValidArea(roughGeom.get());
}

/************************************************************************/



//DouglasPeuckerSimplifier::

/*public static*/
Geometry::Ptr
DouglasPeuckerSimplifier::simplify(const Geometry* geom,
                                   double tolerance)
{
    DouglasPeuckerSimplifier tss(geom);
    tss.setDistanceTolerance(tolerance);
    return tss.getResultGeometry();
}

/*public*/
DouglasPeuckerSimplifier::DouglasPeuckerSimplifier(const Geometry* geom)
    :
    inputGeom(geom)
{
}

/*public*/
void
DouglasPeuckerSimplifier::setDistanceTolerance(double tol)
{
    if(tol < 0.0) {
        throw util::IllegalArgumentException("Tolerance must be non-negative");
    }
    distanceTolerance = tol;
}

Geometry::Ptr
DouglasPeuckerSimplifier::getResultGeometry()
{
    DPTransformer t(distanceTolerance);
    return t.transform(inputGeom);

}

} // namespace geos::simplify
} // namespace geos
