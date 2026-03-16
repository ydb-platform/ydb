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

#ifndef GEOS_UTIL_SINESTARFACTORY_H
#define GEOS_UTIL_SINESTARFACTORY_H

#include <geos/export.h>

#include <geos/util/GeometricShapeFactory.h> // for inheritance

#include <memory>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class Envelope;
class Polygon;
class GeometryFactory;
class PrecisionModel;
class LineString;
}
}

namespace geos {
namespace geom { // geos::geom
namespace util { // geos::geom::util

/**
 * Creates geometries which are shaped like multi-armed stars
 * with each arm shaped like a sine wave.
 * These kinds of geometries are useful as a more complex
 * geometry for testing algorithms.
 *
 * @author Martin Davis
 *
 */
class GEOS_DLL SineStarFactory : public geos::util::GeometricShapeFactory  {

protected:

    int numArms;
    double armLengthRatio;

public:

    /**
     * Creates a factory which will create sine stars using the given
     * {@link GeometryFactory}.
     *
     * @param fact the factory to use. You need to keep the
     *	factory alive for the whole SineStarFactory
     *	life time.
     */
    SineStarFactory(const geom::GeometryFactory* fact)
        :
        geos::util::GeometricShapeFactory(fact),
        numArms(8),
        armLengthRatio(0.5)
    {}

    /**
     * Sets the number of arms in the star
     *
     * @param nArms the number of arms to generate
     */
    void
    setNumArms(int nArms)
    {
        numArms = nArms;
    }

    /**
     * Sets the ration of the length of each arm to the distance from the tip
     * of the arm to the centre of the star.
     * Value should be between 0.0 and 1.0
     *
     * @param armLenRatio
     */
    void
    setArmLengthRatio(double armLenRatio)
    {
        armLengthRatio = armLenRatio;
    }

    /**
     * Generates the geometry for the sine star
     *
     * @return the geometry representing the sine star
     */
    std::unique_ptr<Polygon> createSineStar() const;


};

} // namespace geos::geom::util
} // namespace geos::geom
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_UTIL_SINESTARFACTORY_H
