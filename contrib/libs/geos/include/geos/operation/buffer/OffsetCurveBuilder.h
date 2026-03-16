/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009-2011  Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006-2007 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/OffsetCurveBuilder.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_BUFFER_OFFSETCURVEBUILDER_H
#define GEOS_OP_BUFFER_OFFSETCURVEBUILDER_H

#include <geos/export.h>

#include <geos/operation/buffer/BufferParameters.h> // for composition
#include <geos/operation/buffer/OffsetSegmentGenerator.h>

#include <vector>
#include <memory> // for unique_ptr

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
class PrecisionModel;
}
}

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/**
 * \class OffsetCurveBuilder
 *
 * \brief
 * Computes the raw offset curve for a
 * single Geometry component (ring, line or point).
 *
 * A raw offset curve line is not noded -
 * it may contain self-intersections (and usually will).
 * The final buffer polygon is computed by forming a topological graph
 * of all the noded raw curves and tracing outside contours.
 * The points in the raw curve are rounded to a given geom::PrecisionModel.
 *
 */
class GEOS_DLL OffsetCurveBuilder {
public:

    /*
     * @param nBufParams buffer parameters, this object will
     *                   keep a reference to the passed parameters
     *                   so caller must make sure the object is
     *                   kept alive for the whole lifetime of
     *                   the buffer builder.
     */
    OffsetCurveBuilder(const geom::PrecisionModel* newPrecisionModel,
                       const BufferParameters& nBufParams)
        :
        distance(0.0),
        precisionModel(newPrecisionModel),
        bufParams(nBufParams)
    {}

    /** \brief
     * Gets the buffer parameters being used to generate the curve.
     *
     * @return the buffer parameters being used
     */
    const BufferParameters&
    getBufferParameters() const
    {
        return bufParams;
    }

    /**
     * Tests whether the offset curve for line or point geometries
     * at the given offset distance is empty (does not exist).
     * This is the case if:
     * <ul>
     * <li>the distance is zero,
     * <li>the distance is negative, except for the case of singled-sided buffers
     * </ul>
     *
     * @param distance the offset curve distance
     * @return true if the offset curve is empty
     */
    bool isLineOffsetEmpty(double distance);

    /** \brief
     * This method handles single points as well as lines.
     *
     * Lines are assumed to **not** be closed (the function will not
     * fail for closed lines, but will generate superfluous line caps).
     *
     * @param inputPts input points
     * @param distance offset distance
     * @param lineList the std::vector to which the newly created
     *                 CoordinateSequences will be pushed_back.
     *                 Caller is responsible to delete these new elements.
     */
    void getLineCurve(const geom::CoordinateSequence* inputPts,
                      double distance,
                      std::vector<geom::CoordinateSequence*>& lineList);

    /** \brief
     * This method handles single points as well as lines.
     *
     * Lines are assumed to **not** be closed (the function will not
     * fail for closed lines, but will generate superfluous line caps).
     *
     * @param inputPts input points
     * @param distance offset distance
     * @param lineList the std::vector to which newly created
     *                 CoordinateSequences will be pushed_back.
     *                 Caller will be responsible to delete them.
     * @param leftSide indicates that the left side buffer will be
     *                 obtained/skipped
     * @param rightSide indicates that the right side buffer will
     *                  be obtained/skipped
     *
     * @note This is a GEOS extension.
     */
    void getSingleSidedLineCurve(const geom::CoordinateSequence* inputPts,
                                 double distance, std::vector<geom::CoordinateSequence*>& lineList,
                                 bool leftSide, bool rightSide) ;

    /** \brief
     * This method handles the degenerate cases of single points and lines,
     * as well as rings.
     *
     * @param inputPts input points
     * @param side a [Position](@ref geom::Position)
     * @param distance offset distance
     * @param lineList the std::vector to which CoordinateSequences will
     *                 be pushed_back
     */
    void getRingCurve(const geom::CoordinateSequence* inputPts, int side,
                      double distance,
                      std::vector<geom::CoordinateSequence*>& lineList);

private:

    double distance;

    const geom::PrecisionModel* precisionModel;

    const BufferParameters& bufParams;

    /** \brief
     * Use a value which results in a potential distance error which is
     * significantly less than the error due to the quadrant segment discretization.
     *
     * For QS = 8 a value of 100 is reasonable.
     * This should produce a maximum of 1% distance error.
     */
    static const double SIMPLIFY_FACTOR; // 100.0;

    /** \brief
     * Computes the distance tolerance to use during input
     * line simplification.
     *
     * @param bufDistance the buffer distance
     * @return the simplification tolerance
     */
    double simplifyTolerance(double bufDistance);

    void computeLineBufferCurve(const geom::CoordinateSequence& inputPts,
                                OffsetSegmentGenerator& segGen);

    void computeSingleSidedBufferCurve(const geom::CoordinateSequence& inputPts,
                                       bool isRightSide,
                                       OffsetSegmentGenerator& segGen);

    void computeRingBufferCurve(const geom::CoordinateSequence& inputPts,
                                int side, OffsetSegmentGenerator& segGen);

    std::unique_ptr<OffsetSegmentGenerator> getSegGen(double dist);

    void computePointCurve(const geom::Coordinate& pt,
                           OffsetSegmentGenerator& segGen);


    // Declare type as noncopyable
    OffsetCurveBuilder(const OffsetCurveBuilder& other) = delete;
    OffsetCurveBuilder& operator=(const OffsetCurveBuilder& rhs) = delete;
};

} // namespace geos::operation::buffer
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_BUFFER_OFFSETCURVEBUILDER_H

