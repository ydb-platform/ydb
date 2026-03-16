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

#include <geos/operation/buffer/BufferInputLineSimplifier.h>
#include <geos/geom/CoordinateSequence.h> // for inlines
#include <geos/geom/CoordinateArraySequence.h> // for constructing the return
#include <geos/algorithm/Distance.h> // for use
#include <geos/algorithm/Orientation.h> // for use
#include <geos/util.h>

#include <memory>
#include <cmath>
#include <vector>

//#include <cassert>

using namespace geos::algorithm; // Orientation
using namespace geos::geom;
//using namespace geos::geomgraph; // DirectedEdge, Position

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

BufferInputLineSimplifier::BufferInputLineSimplifier(
    const geom::CoordinateSequence& input)
    :
    inputLine(input),
    angleOrientation(Orientation::COUNTERCLOCKWISE)
{}

/*public static*/
std::unique_ptr<geom::CoordinateSequence>
BufferInputLineSimplifier::simplify(const geom::CoordinateSequence& inputLine,
                                    double distanceTol)
{
    BufferInputLineSimplifier simp(inputLine);
    return simp.simplify(distanceTol);
}

/* public */
std::unique_ptr<geom::CoordinateSequence>
BufferInputLineSimplifier::simplify(double nDistanceTol)
{
    distanceTol = fabs(nDistanceTol);
    if(nDistanceTol < 0) {
        angleOrientation = Orientation::CLOCKWISE;
    }

    // rely on fact that boolean array is filled with false value
    static const int startValue = INIT;
    isDeleted.assign(inputLine.size(), startValue);

    bool isChanged = false;
    do {
        isChanged = deleteShallowConcavities();
    }
    while(isChanged);

    return collapseLine();
}

/* private */
bool
BufferInputLineSimplifier::deleteShallowConcavities()
{
    /*
     * Do not simplify end line segments of the line string.
     * This ensures that end caps are generated consistently.
     */
    size_t index = 1;

    auto midIndex = findNextNonDeletedIndex(index);
    auto lastIndex = findNextNonDeletedIndex(midIndex);

    bool isChanged = false;
    while(lastIndex < inputLine.size()) {
        // test triple for shallow concavity
        bool isMiddleVertexDeleted = false;
        if(isDeletable(index, midIndex, lastIndex,
                       distanceTol)) {
            isDeleted[midIndex] = DELETE;
            isMiddleVertexDeleted = true;
            isChanged = true;
        }
        // move simplification window forward
        if(isMiddleVertexDeleted) {
            index = lastIndex;
        }
        else {
            index = midIndex;
        }

        midIndex = findNextNonDeletedIndex(index);
        lastIndex = findNextNonDeletedIndex(midIndex);
    }
    return isChanged;
}

/* private */
size_t
BufferInputLineSimplifier::findNextNonDeletedIndex(size_t index) const
{
    std::size_t next = index + 1;
    const std::size_t len = inputLine.size();
    while(next < len && isDeleted[next] == DELETE) {
        next++;
    }
    return next;
}

/* private */
std::unique_ptr<geom::CoordinateSequence>
BufferInputLineSimplifier::collapseLine() const
{
    auto coordList = new CoordinateArraySequence();

    for(size_t i = 0, n = inputLine.size(); i < n; ++i) {
        if(isDeleted[i] != DELETE) {
            coordList->add(inputLine[i], false);
        }
    }

    return std::unique_ptr<CoordinateSequence>(coordList);
}

/* private */
bool
BufferInputLineSimplifier::isDeletable(size_t i0, size_t i1, size_t i2,
                                       double p_distanceTol) const
{
    const Coordinate& p0 = inputLine[i0];
    const Coordinate& p1 = inputLine[i1];
    const Coordinate& p2 = inputLine[i2];

    if(! isConcave(p0, p1, p2)) {
        return false;
    }
    if(! isShallow(p0, p1, p2, p_distanceTol)) {
        return false;
    }

    // MD - don't use this heuristic - it's too restricting
    // if (p0.distance(p2) > distanceTol) return false;

    return isShallowSampled(p0, p1, i0, i2, p_distanceTol);
}

/* private */
bool
BufferInputLineSimplifier::isShallowConcavity(const geom::Coordinate& p0,
        const geom::Coordinate& p1,
        const geom::Coordinate& p2,
        double p_distanceTol) const
{
    int orientation = Orientation::index(p0, p1, p2);
    bool isAngleToSimplify = (orientation == angleOrientation);
    if(! isAngleToSimplify) {
        return false;
    }

    double dist = Distance::pointToSegment(p1, p0, p2);
    return dist < p_distanceTol;
}

/* private */
bool
BufferInputLineSimplifier::isShallowSampled(const geom::Coordinate& p0,
        const geom::Coordinate& p2,
        size_t i0, size_t i2,
        double p_distanceTol) const
{
    // check every n'th point to see if it is within tolerance
    auto inc = (i2 - i0) / NUM_PTS_TO_CHECK;
    if(inc <= 0) {
        inc = 1;
    }

    for(size_t i = i0; i < i2; i += inc) {
        if(! isShallow(p0, p2, inputLine[i], p_distanceTol)) {
            return false;
        }
    }
    return true;
}

/* private */
bool
BufferInputLineSimplifier::isShallow(const geom::Coordinate& p0,
                                     const geom::Coordinate& p1,
                                     const geom::Coordinate& p2,
                                     double p_distanceTol) const
{
    double dist = Distance::pointToSegment(p1, p0, p2);
    return dist < p_distanceTol;
}

/* private */
bool
BufferInputLineSimplifier::isConcave(const geom::Coordinate& p0,
                                     const geom::Coordinate& p1,
                                     const geom::Coordinate& p2) const
{
    int orientation = Orientation::index(p0, p1, p2);
    bool p_isConcave = (orientation == angleOrientation);
    return p_isConcave;
}

} // namespace geos.operation.buffer
} // namespace geos.operation
} // namespace geos

