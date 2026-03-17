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
 * Last port: geom/util/SineStarFactory.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/constants.h>
#include <geos/geom/util/SineStarFactory.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/LinearRing.h>

#include <vector>
#include <cmath>
#include <memory>


namespace geos {
namespace geom { // geos::geom
namespace util { // geos::geom::util

/* public */
std::unique_ptr<Polygon>
SineStarFactory::createSineStar() const
{
    std::unique_ptr<Envelope> env(dim.getEnvelope());
    double radius = env->getWidth() / 2.0;

    double armRatio = armLengthRatio;
    if(armRatio < 0.0) {
        armRatio = 0.0;
    }
    if(armRatio > 1.0) {
        armRatio = 1.0;
    }

    double armMaxLen = armRatio * radius;
    double insideRadius = (1 - armRatio) * radius;

    double centreX = env->getMinX() + radius;
    double centreY = env->getMinY() + radius;

    std::vector<Coordinate> pts(nPts + 1);
    std::size_t iPt = 0;
    for(std::size_t i = 0; i < nPts; i++) {
        // the fraction of the way thru the current arm - in [0,1]
        double ptArcFrac = (i / (double) nPts) * numArms;
        double armAngFrac = ptArcFrac - floor(ptArcFrac);

        // the angle for the current arm - in [0,2Pi]
        // (each arm is a complete sine wave cycle)
        double armAng = 2 * MATH_PI * armAngFrac;
        // the current length of the arm
        double armLenFrac = (cos(armAng) + 1.0) / 2.0;

        // the current radius of the curve (core + arm)
        double curveRadius = insideRadius + armMaxLen * armLenFrac;

        // the current angle of the curve
        double ang = i * (2 * MATH_PI / nPts);
        double x = curveRadius * cos(ang) + centreX;
        double y = curveRadius * sin(ang) + centreY;
        pts[iPt++] = coord(x, y);
    }
    pts[iPt] = pts[0];

    auto cs = geomFact->getCoordinateSequenceFactory()->create(std::move(pts));
    auto ring = geomFact->createLinearRing(std::move(cs));
    auto poly = geomFact->createPolygon(std::move(ring));
    return poly;
}

} // namespace geos::geom::util
} // namespace geos::geom
} // namespace geos

