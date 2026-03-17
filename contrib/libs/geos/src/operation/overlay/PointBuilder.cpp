/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/PointBuilder.java rev. 1.16 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/overlay/PointBuilder.h>
#include <geos/operation/overlay/OverlayOp.h>

#include <geos/geomgraph/Node.h>
#include <geos/geomgraph/EdgeEndStar.h>
#include <geos/geomgraph/Label.h>

#include <vector>
#include <cassert>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

using namespace std;
using namespace geos::geomgraph;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay


/*
 * @return a list of the Points in the result of the specified
 * overlay operation
 */
vector<Point*>*
PointBuilder::build(OverlayOp::OpCode opCode)
{
    extractNonCoveredResultNodes(opCode);
    return resultPointList;
}

/*
 * Determines nodes which are in the result, and creates Point for them.
 *
 * This method determines nodes which are candidates for the result via their
 * labelling and their graph topology.
 *
 * @param opCode the overlay operation
 */
void
PointBuilder::extractNonCoveredResultNodes(OverlayOp::OpCode opCode)
{
    auto& nodeMap = op->getGraph().getNodeMap()->nodeMap;
    for(auto& entry : nodeMap) {
        Node* n = entry.second;

        // filter out nodes which are known to be in the result
        if(n->isInResult()) {
            continue;
        }

        // if an incident edge is in the result, then
        // the node coordinate is included already
        if(n->isIncidentEdgeInResult()) {
            continue;
        }

        if(n->getEdges()->getDegree() == 0 ||
                opCode == OverlayOp::opINTERSECTION) {

            /*
             * For nodes on edges, only INTERSECTION can result
             * in edge nodes being included even
             * if none of their incident edges are included
             */
            const Label& label = n->getLabel();
            if(OverlayOp::isResultOfOp(label, opCode)) {
                filterCoveredNodeToPoint(n);
            }
        }
    }
}

void
PointBuilder::filterCoveredNodeToPoint(const Node* n)
{
    const Coordinate& coord = n->getCoordinate();
    if(!op->isCoveredByLA(coord)) {
        Point* pt = geometryFactory->createPoint(coord);
        resultPointList->push_back(pt);
    }
}

} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

