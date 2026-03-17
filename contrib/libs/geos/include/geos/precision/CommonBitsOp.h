/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_PRECISION_COMMONBITSOP_H
#define GEOS_PRECISION_COMMONBITSOP_H

#include <geos/export.h>
#include <geos/precision/CommonBitsRemover.h> // for unique_ptr composition

#include <vector>
#include <memory>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace geom {
class Geometry;
}
namespace precision {
//class CommonBitsRemover;
}
}

namespace geos {
namespace precision { // geos.precision

/** \brief
 * Provides versions of Geometry spatial functions which use
 * common bit removal to reduce the likelihood of robustness problems.
 *
 * In the current implementation no rounding is performed on the
 * reshifted result geometry, which means that it is possible
 * that the returned Geometry is invalid.
 * Client classes should check the validity of the returned result themselves.
 */
class GEOS_DLL CommonBitsOp {

private:

    bool returnToOriginalPrecision;

    std::unique_ptr<CommonBitsRemover> cbr;

    /** \brief
     * Computes a copy of the input Geometry with the calculated
     * common bits removed from each coordinate.
     *
     * @param geom0 the Geometry to remove common bits from
     * @return a copy of the input Geometry with common bits removed
     *         (caller takes responsibility of its deletion)
     */
    std::unique_ptr<geom::Geometry> removeCommonBits(const geom::Geometry* geom0);

    /** \brief
     *
     */
    void removeCommonBits(
        const geom::Geometry* geom0,
        const geom::Geometry* geom1,
        std::unique_ptr<geom::Geometry>& rgeom0,
        std::unique_ptr<geom::Geometry>& rgeom1);


public:

    /** \brief
     * Creates a new instance of class, which reshifts result Geometry
     */
    CommonBitsOp();

    /** \brief
     * Creates a new instance of class, specifying whether
     * the result {@link geom::Geometry}s should be reshifted.
     *
     * @param nReturnToOriginalPrecision
     */
    CommonBitsOp(bool nReturnToOriginalPrecision);

    /** \brief
     * Computes the set-theoretic intersection of two Geometry,
     * using enhanced precision.
     * @param geom0 the first Geometry
     * @param geom1 the second Geometry
     * @return the Geometry representing the set-theoretic
     *  intersection of the input Geometries.
     */
    std::unique_ptr<geom::Geometry> intersection(
        const geom::Geometry* geom0,
        const geom::Geometry* geom1);

    /** \brief
     * Computes the set-theoretic union of two Geometry,
     * using enhanced precision.
     * @param geom0 the first Geometry
     * @param geom1 the second Geometry
     * @return the Geometry representing the set-theoretic union
     * of the input Geometries.
     */
    std::unique_ptr<geom::Geometry> Union(
        const geom::Geometry* geom0,
        const geom::Geometry* geom1);

    /** \brief
     * Computes the set-theoretic difference of two Geometry,
     * using enhanced precision.
     * @param geom0 the first Geometry
     * @param geom1 the second Geometry, to be subtracted from the first
     * @return the Geometry representing the set-theoretic difference
     * of the input Geometries.
     */
    std::unique_ptr<geom::Geometry> difference(
        const geom::Geometry* geom0,
        const geom::Geometry* geom1);

    /** \brief
     * Computes the set-theoretic symmetric difference of two geometries,
     * using enhanced precision.
     * @param geom0 the first Geometry
     * @param geom1 the second Geometry
     * @return the Geometry representing the set-theoretic symmetric
     * difference of the input Geometries.
     */
    std::unique_ptr<geom::Geometry> symDifference(
        const geom::Geometry* geom0,
        const geom::Geometry* geom1);

    /** \brief
     * Computes the buffer a geometry,
     * using enhanced precision.
     * @param geom0 the Geometry to buffer
     * @param distance the buffer distance
     * @return the Geometry representing the buffer of the input Geometry.
     */
    std::unique_ptr<geom::Geometry> buffer(
        const geom::Geometry* geom0,
        double distance);

    /** \brief
     * If required, returning the result to the orginal precision
     * if required.
     *
     * In this current implementation, no rounding is performed on the
     * reshifted result geometry, which means that it is possible
     * that the returned Geometry is invalid.
     *
     * @param result the result Geometry to modify
     * @return the result Geometry with the required precision
     */
    std::unique_ptr<geom::Geometry> computeResultPrecision(
        std::unique_ptr<geom::Geometry> result);
};

} // namespace geos.precision
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_PRECISION_COMMONBITSOP_H
