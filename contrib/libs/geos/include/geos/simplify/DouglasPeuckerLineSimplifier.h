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
 * Last port: simplify/DouglasPeuckerLineSimplifier.java rev. 1.4
 *
 **********************************************************************/

#ifndef GEOS_SIMPLIFY_DOUBGLASPEUCKERLINESIMPLIFIER_H
#define GEOS_SIMPLIFY_DOUBGLASPEUCKERLINESIMPLIFIER_H

#include <geos/export.h>
#include <vector>
#include <memory> // for unique_ptr

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace simplify { // geos::simplify

/** \brief
 * Simplifies a linestring (sequence of points) using
 * the standard Douglas-Peucker algorithm.
 */
class GEOS_DLL DouglasPeuckerLineSimplifier {

public:

    typedef std::vector<short int> BoolVect;
    typedef std::unique_ptr<BoolVect> BoolVectAutoPtr;

    typedef std::vector<geom::Coordinate> CoordsVect;
    typedef std::unique_ptr<CoordsVect> CoordsVectAutoPtr;


    /** \brief
     * Returns a newly allocated Coordinate vector, wrapped
     * into an unique_ptr
     */
    static CoordsVectAutoPtr simplify(
        const CoordsVect& nPts,
        double distanceTolerance);

    DouglasPeuckerLineSimplifier(const CoordsVect& nPts);

    /** \brief
     * Sets the distance tolerance for the simplification.
     *
     * All vertices in the simplified linestring will be within this
     * distance of the original linestring.
     *
     * @param nDistanceTolerance the approximation tolerance to use
     */
    void setDistanceTolerance(double nDistanceTolerance);

    /** \brief
     * Returns a newly allocated Coordinate vector, wrapped
     * into an unique_ptr
     */
    CoordsVectAutoPtr simplify();

private:

    const CoordsVect& pts;
    BoolVectAutoPtr usePt;
    double distanceTolerance;

    void simplifySection(std::size_t i, std::size_t j);

    // Declare type as noncopyable
    DouglasPeuckerLineSimplifier(const DouglasPeuckerLineSimplifier& other) = delete;
    DouglasPeuckerLineSimplifier& operator=(const DouglasPeuckerLineSimplifier& rhs) = delete;
};

} // namespace geos::simplify
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_SIMPLIFY_DOUBGLASPEUCKERLINESIMPLIFIER_H
