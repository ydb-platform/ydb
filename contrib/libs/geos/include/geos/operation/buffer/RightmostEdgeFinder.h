/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/RightmostEdgeFinder.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_BUFFER_RIGHTMOSTEDGEFINDER_H
#define GEOS_OP_BUFFER_RIGHTMOSTEDGEFINDER_H

#include <geos/export.h>

#include <geos/geom/Coordinate.h> // for composition

#include <vector>

// Forward declarations
namespace geos {
namespace geom {
}
namespace geomgraph {
class DirectedEdge;
}
}

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/**
 * \brief
 * A RightmostEdgeFinder find the geomgraph::DirectedEdge in a list which has
 * the highest coordinate, and which is oriented L to R at that point.
 * (I.e. the right side is on the RHS of the edge.)
 */
class GEOS_DLL RightmostEdgeFinder {

private:

    int minIndex;

    geom::Coordinate minCoord;

    geomgraph::DirectedEdge* minDe;

    geomgraph::DirectedEdge* orientedDe;

    void findRightmostEdgeAtNode();

    void findRightmostEdgeAtVertex();

    void checkForRightmostCoordinate(geomgraph::DirectedEdge* de);

    int getRightmostSide(geomgraph::DirectedEdge* de, int index);

    int getRightmostSideOfSegment(geomgraph::DirectedEdge* de, int i);

public:

    /** \brief
     * A RightmostEdgeFinder finds the geomgraph::DirectedEdge with the
     * rightmost coordinate.
     *
     * The geomgraph::DirectedEdge returned is guaranteed to have the R of
     * the world on its RHS.
     */
    RightmostEdgeFinder();

    geomgraph::DirectedEdge* getEdge();

    geom::Coordinate& getCoordinate();

    /// Note that only Forward DirectedEdges will be checked
    void findEdge(std::vector<geomgraph::DirectedEdge*>* dirEdgeList);
};

/*public*/
inline geomgraph::DirectedEdge*
RightmostEdgeFinder::getEdge()
{
    return orientedDe;
}

/*public*/
inline geom::Coordinate&
RightmostEdgeFinder::getCoordinate()
{
    return minCoord;
}




} // namespace geos::operation::buffer
} // namespace geos::operation
} // namespace geos

#endif // ndef GEOS_OP_BUFFER_RIGHTMOSTEDGEFINDER_H

