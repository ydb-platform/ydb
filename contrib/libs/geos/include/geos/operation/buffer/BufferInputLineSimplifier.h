/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009  Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/BufferInputLineSimplifier.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_BUFFER_BUFFERINPUTLINESIMPLIFIER_H
#define GEOS_OP_BUFFER_BUFFERINPUTLINESIMPLIFIER_H

#include <geos/geom/CoordinateSequence.h> // complete type required

#include <memory>
#include <vector> // for composition


// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
//class PrecisionModel;
}
}

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/** \brief
 * Simplifies a buffer input line to
 * remove concavities with shallow depth.
 *
 * The most important benefit of doing this
 * is to reduce the number of points and the complexity of
 * shape which will be buffered.
 * It also reduces the risk of gores created by
 * the quantized fillet arcs (although this issue
 * should be eliminated in any case by the
 * offset curve generation logic).
 *
 * A key aspect of the simplification is that it
 * affects inside (concave or inward) corners only.
 * Convex (outward) corners are preserved, since they
 * are required to ensure that the generated buffer curve
 * lies at the correct distance from the input geometry.
 *
 * Another important heuristic used is that the end segments
 * of the input are never simplified.  This ensures that
 * the client buffer code is able to generate end caps faithfully.
 *
 * No attempt is made to avoid self-intersections in the output.
 * This is acceptable for use for generating a buffer offset curve,
 * since the buffer algorithm is insensitive to invalid polygonal
 * geometry.  However,
 * this means that this algorithm
 * cannot be used as a general-purpose polygon simplification technique.
 *
 * @author Martin Davis
 *
 */
class BufferInputLineSimplifier {

public:

    /**
     * Simplify the input coordinate list.
     *
     * If the distance tolerance is positive,
     * concavities on the LEFT side of the line are simplified.
     * If the supplied distance tolerance is negative,
     * concavities on the RIGHT side of the line are simplified.
     *
     * @param inputLine the coordinate sequence to simplify
     * @param distanceTol simplification distance tolerance to use
     * @return a simplified version of the coordinate sequence
     */
    static std::unique_ptr<geom::CoordinateSequence> simplify(
        const geom::CoordinateSequence& inputLine, double distanceTol);

    BufferInputLineSimplifier(const geom::CoordinateSequence& input);

    /**
     * Simplify the input coordinate list.
     * If the distance tolerance is positive,
     * concavities on the LEFT side of the line are simplified.
     * If the supplied distance tolerance is negative,
     * concavities on the RIGHT side of the line are simplified.
     *
     * @param distanceTol simplification distance tolerance to use
     * @return the simplified coordinate list
     */
    std::unique_ptr<geom::CoordinateSequence> simplify(double distanceTol);

private:

    /**
     * Uses a sliding window containing 3 vertices to detect shallow angles
     * in which the middle vertex can be deleted, since it does not
     * affect the shape of the resulting buffer in a significant way.
     * @return
     */
    bool deleteShallowConcavities();

    /**
     * Finds the next non-deleted index,
     * or the end of the point array if none
     *
     * @param index
     * @return the next non-deleted index, if any
     * @return inputLine.size() if there are no more non-deleted indices
     */
    size_t findNextNonDeletedIndex(size_t index) const;

    std::unique_ptr<geom::CoordinateSequence> collapseLine() const;

    bool isDeletable(size_t i0, size_t i1, size_t i2, double distanceTol) const;

    bool isShallowConcavity(const geom::Coordinate& p0,
                            const geom::Coordinate& p1,
                            const geom::Coordinate& p2,
                            double distanceTol) const;

    /**
     * Checks for shallowness over a sample of points in the given section.
     *
     * This helps prevents the siplification from incrementally
     * "skipping" over points which are in fact non-shallow.
     *
     * @param p0 start coordinate of section
     * @param p2 end coordinate of section
     * @param i0 start index of section
     * @param i2 end index of section
     * @param distanceTol distance tolerance
     * @return
     */
    bool isShallowSampled(const geom::Coordinate& p0,
                          const geom::Coordinate& p2,
                          size_t i0, size_t i2, double distanceTol) const;

    bool isShallow(const geom::Coordinate& p0,
                   const geom::Coordinate& p1,
                   const geom::Coordinate& p2,
                   double distanceTol) const;

    bool isConcave(const geom::Coordinate& p0,
                   const geom::Coordinate& p1,
                   const geom::Coordinate& p2) const;

    static const int NUM_PTS_TO_CHECK = 10;

    static const int INIT = 0;
    static const int DELETE = 1;
    static const int KEEP = 1;

    const geom::CoordinateSequence& inputLine;
    double distanceTol;
    std::vector<int> isDeleted;

    int angleOrientation;

    // Declare type as noncopyable
    BufferInputLineSimplifier(const BufferInputLineSimplifier& other) = delete;
    BufferInputLineSimplifier& operator=(const BufferInputLineSimplifier& rhs) = delete;
};


} // namespace geos.operation.buffer
} // namespace geos.operation
} // namespace geos


#endif // ndef GEOS_OP_BUFFER_BUFFERINPUTLINESIMPLIFIER_H

