/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2010 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
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

#ifndef GEOS_DENSIFIER_H
#define GEOS_DENSIFIER_H

#include <geos/export.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/util/GeometryTransformer.h>
#include <geos/geom/util/Densifier.h>
#include <geos/util/Interrupt.h>

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
/* warning C4251: needs to have dll-interface to be used by */
/* clients of class */
#pragma warning(disable: 4251)
#endif


namespace geos {
namespace geom {
namespace util {

/**
 * Densifies a {@link Geometry} by inserting extra vertices along the line segments
 * contained in the geometry.
 * All segments in the created densified geometry will be no longer than
 * than the given distance tolerance.
 * Densified polygonal geometries are guaranteed to be topologically correct.
 * The coordinates created during densification respect the input geometry's
 * {@link PrecisionModel}.
 * <p>
 * <b>Note:</b> At some future point this class will
 * offer a variety of densification strategies.
 *
 * @author Martin Davis
 */
class GEOS_DLL Densifier {
public:
    Densifier(const Geometry* inputGeom);

    Geometry::Ptr densify(const Geometry* geom, double distanceTolerance);
    void setDistanceTolerance(double distanceTolerance);
    Geometry::Ptr getResultGeometry() const;

private:
    double distanceTolerance;
    const Geometry* inputGeom;
    static std::unique_ptr<Coordinate::Vect> densifyPoints(const Coordinate::Vect pts, double distanceTolerance,
            const PrecisionModel* precModel);

    class GEOS_DLL DensifyTransformer: public GeometryTransformer {
    public:
        DensifyTransformer(double distanceTolerance);
        double distanceTolerance;
        CoordinateSequence::Ptr transformCoordinates(const CoordinateSequence* coords, const Geometry* parent) override;
        Geometry::Ptr transformPolygon(const Polygon* geom, const Geometry* parent) override;
        Geometry::Ptr transformMultiPolygon(const MultiPolygon* geom, const Geometry* parent) override;
        Geometry::Ptr createValidArea(const Geometry* roughAreaGeom);
    };

}; // Densifier

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_DENSIFIER_H
