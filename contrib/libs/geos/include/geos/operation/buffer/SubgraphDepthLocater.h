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
 * Last port: operation/buffer/SubgraphDepthLocater.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_BUFFER_SUBGRAPHDEPTHLOCATER_H
#define GEOS_OP_BUFFER_SUBGRAPHDEPTHLOCATER_H

#include <geos/export.h>

#include <vector>

#include <geos/geom/LineSegment.h> // for composition

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
namespace geomgraph {
class DirectedEdge;
}
namespace operation {
namespace buffer {
class BufferSubgraph;
class DepthSegment;
}
}
}

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/**
 * \class SubgraphDepthLocater
 *
 * \brief
 * Locates a subgraph inside a set of subgraphs,
 * in order to determine the outside depth of the subgraph.
 *
 * The input subgraphs are assumed to have had depths
 * already calculated for their edges.
 *
 */
class GEOS_DLL SubgraphDepthLocater {

public:

    SubgraphDepthLocater(std::vector<BufferSubgraph*>* newSubgraphs)
        :
        subgraphs(newSubgraphs)
    {}

    ~SubgraphDepthLocater() {}

    int getDepth(const geom::Coordinate& p);

private:

    std::vector<BufferSubgraph*>* subgraphs;

    geom::LineSegment seg;

    /**
     * Finds all non-horizontal segments intersecting the stabbing line.
     * The stabbing line is the ray to the right of stabbingRayLeftPt.
     *
     * @param stabbingRayLeftPt the left-hand origin of the stabbing line
     * @param stabbedSegments a vector to which DepthSegments intersecting
     *        the stabbing line will be added.
     */
    void findStabbedSegments(const geom::Coordinate& stabbingRayLeftPt,
                             std::vector<DepthSegment*>& stabbedSegments);

    /**
     * Finds all non-horizontal segments intersecting the stabbing line
     * in the list of dirEdges.
     * The stabbing line is the ray to the right of stabbingRayLeftPt.
     *
     * @param stabbingRayLeftPt the left-hand origin of the stabbing line
     * @param stabbedSegments the current vector of DepthSegments
     *        intersecting the stabbing line will be added.
     */
    void findStabbedSegments(const geom::Coordinate& stabbingRayLeftPt,
                             std::vector<geomgraph::DirectedEdge*>* dirEdges,
                             std::vector<DepthSegment*>& stabbedSegments);

    /**
     * Finds all non-horizontal segments intersecting the stabbing line
     * in the input dirEdge.
     * The stabbing line is the ray to the right of stabbingRayLeftPt.
     *
     * @param stabbingRayLeftPt the left-hand origin of the stabbing line
     * @param stabbedSegments the current list of DepthSegments intersecting
     *        the stabbing line
     */
    void findStabbedSegments(const geom::Coordinate& stabbingRayLeftPt,
                             geomgraph::DirectedEdge* dirEdge,
                             std::vector<DepthSegment*>& stabbedSegments);

};


} // namespace geos::operation::buffer
} // namespace geos::operation
} // namespace geos

#endif // ndef GEOS_OP_BUFFER_SUBGRAPHDEPTHLOCATER_H

