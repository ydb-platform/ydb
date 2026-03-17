/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/LineBuilder.java rev. 1.15 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/overlay/LineBuilder.h>
#include <geos/operation/overlay/OverlayOp.h>
#include <geos/algorithm/PointLocator.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geomgraph/Node.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/DirectedEdge.h>
#include <geos/geomgraph/DirectedEdgeStar.h>
#include <geos/util.h>

#include <map>
#include <vector>
#include <cassert>
#include <cmath>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif
#define COMPUTE_Z 1

using namespace std;
using namespace geos::algorithm;
using namespace geos::geomgraph;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay

LineBuilder::LineBuilder(OverlayOp* newOp,
                         const GeometryFactory* newGeometryFactory,
                         PointLocator* newPtLocator):
    op(newOp),
    geometryFactory(newGeometryFactory),
    ptLocator(newPtLocator),
    //lineEdgesList(new vector<Edge *>()),
    resultLineList(new vector<LineString *>())
{
}

/*
 * @return a list of the LineStrings in the result of the
 *         specified overlay operation
 */
vector<LineString*>*
LineBuilder::build(OverlayOp::OpCode opCode)
{
    findCoveredLineEdges();
    collectLines(opCode);
    //labelIsolatedLines(&lineEdgesList);
    buildLines(opCode);
    return resultLineList;
}

/*
 * Find and mark L edges which are "covered" by the result area (if any).
 * L edges at nodes which also have A edges can be checked by checking
 * their depth at that node.
 * L edges at nodes which do not have A edges can be checked by doing a
 * point-in-polygon test with the previously computed result areas.
 */
void
LineBuilder::findCoveredLineEdges()
{
    // first set covered for all L edges at nodes which have A edges too
    auto& nodeMap = op->getGraph().getNodeMap()->nodeMap;
    for(auto& entry : nodeMap) {
        Node* node = entry.second;
        //node.print(System.out);
        DirectedEdgeStar* des = detail::down_cast<DirectedEdgeStar*>(node->getEdges());
        des->findCoveredLineEdges();
        //((DirectedEdgeStar*)node->getEdges())->findCoveredLineEdges();
    }

    /*
     * For all L edges which weren't handled by the above,
     * use a point-in-poly test to determine whether they are covered
     */
    vector<EdgeEnd*>* ee = op->getGraph().getEdgeEnds();
    for(size_t i = 0, s = ee->size(); i < s; ++i) {
        DirectedEdge* de = detail::down_cast<DirectedEdge*>((*ee)[i]);
        Edge* e = de->getEdge();
        if(de->isLineEdge() && !e->isCoveredSet()) {
            bool isCovered = op->isCoveredByA(de->getCoordinate());
            e->setCovered(isCovered);
        }
    }
}

void
LineBuilder::collectLines(OverlayOp::OpCode opCode)
{
    vector<EdgeEnd*>* ee = op->getGraph().getEdgeEnds();
    for(size_t i = 0, s = ee->size(); i < s; ++i) {
        DirectedEdge* de = detail::down_cast<DirectedEdge*>((*ee)[i]);
        collectLineEdge(de, opCode, &lineEdgesList);
        collectBoundaryTouchEdge(de, opCode, &lineEdgesList);
    }
}

void
LineBuilder::collectLineEdge(DirectedEdge* de, OverlayOp::OpCode opCode,
                             vector<Edge*>* edges)
{

    // include L edges which are in the result
    if(de->isLineEdge()) {

        const Label& label = de->getLabel();

        Edge* e = de->getEdge();

        if(!de->isVisited()
                && OverlayOp::isResultOfOp(label, opCode)
                && !e->isCovered()) {
            //Debug.println("de: "+de.getLabel());
            //Debug.println("edge: "+e.getLabel());
            edges->push_back(e);
            de->setVisitedEdge(true);
        }

    }

}

/*private*/
void
LineBuilder::collectBoundaryTouchEdge(DirectedEdge* de,
                                      OverlayOp::OpCode opCode, vector<Edge*>* edges)
{
    if(de->isLineEdge()) {
        return;    // only interested in area edges
    }
    if(de->isVisited()) {
        return;    // already processed
    }

    // added to handle dimensional collapses
    if(de->isInteriorAreaEdge()) {
        return;
    }

    // if the edge linework is already included, don't include it again
    if(de->getEdge()->isInResult()) {
        return;
    }

    // sanity check for labelling of result edgerings
    assert(!(de->isInResult() || de->getSym()->isInResult())
           ||
           ! de->getEdge()->isInResult());


    // include the linework if it's in the result of the operation
    const Label& label = de->getLabel();
    if(OverlayOp::isResultOfOp(label, opCode)
            && opCode == OverlayOp::opINTERSECTION) {
        edges->push_back(de->getEdge());
        de->setVisitedEdge(true);
    }
}

void
LineBuilder::buildLines(OverlayOp::OpCode /* opCode */)
{
    for(size_t i = 0, s = lineEdgesList.size(); i < s; ++i) {
        Edge* e = lineEdgesList[i];
        auto cs = e->getCoordinates()->clone();
#if COMPUTE_Z
        propagateZ(cs.get());
#endif
        LineString* line = geometryFactory->createLineString(cs.release());
        resultLineList->push_back(line);
        e->setInResult(true);
    }
}

/*
 * If the given CoordinateSequence has mixed 3d/2d vertexes
 * set Z for all vertexes missing it.
 * The Z value is interpolated between 3d vertexes and copied
 * from a 3d vertex to the end.
 */
void
LineBuilder::propagateZ(CoordinateSequence* cs)
{
#if GEOS_DEBUG
    cerr << "LineBuilder::propagateZ() called" << endl;
#endif

    vector<size_t> v3d; // vertex 3d
    size_t cssize = cs->getSize();
    for(size_t i = 0; i < cssize; ++i) {
        if(!std::isnan(cs->getAt(i).z)) {
            v3d.push_back(i);
        }
    }

#if GEOS_DEBUG
    cerr << "  found " << v3d.size() << " 3d vertexes" << endl;
#endif

    if(v3d.empty()) {
#if GEOS_DEBUG
        cerr << "  nothing to do" << endl;
#endif
        return;
    }

    Coordinate buf;

    // fill initial part
    if(v3d[0] != 0) {
        double z = cs->getAt(v3d[0]).z;
        for(size_t j = 0; j < v3d[0]; ++j) {
            buf = cs->getAt(j);
            buf.z = z;
            cs->setAt(buf, j);
        }
    }

    // interpolate inbetweens
    size_t prev = v3d[0];
    for(size_t i = 1; i < v3d.size(); ++i) {
        auto curr = v3d[i];
        auto dist = curr - prev;
        if(dist > 1) {
            const Coordinate& cto = cs->getAt(curr);
            const Coordinate& cfrom = cs->getAt(prev);
            double gap = cto.z - cfrom.z;
            double zstep = gap / static_cast<double>(dist);
            double z = cfrom.z;
            for(size_t j = prev + 1; j < curr; ++j) {
                buf = cs->getAt(j);
                z += zstep;
                buf.z = z;
                cs->setAt(buf, j);
            }
        }
        prev = curr;
    }

    // fill final part
    if(prev < cssize - 1) {
        double z = cs->getAt(prev).z;
        for(size_t j = prev + 1; j < cssize; j++) {
            buf = cs->getAt(j);
            buf.z = z;
            cs->setAt(buf, j);
        }
    }

}



void
LineBuilder::labelIsolatedLines(vector<Edge*>* edgesList)
{
    for(size_t i = 0, s = edgesList->size(); i < s; ++i) {
        Edge* e = (*edgesList)[i];
        const Label& label = e->getLabel();
        //n.print(System.out);
        if(e->isIsolated()) {
            if(label.isNull(0)) {
                labelIsolatedLine(e, 0);
            }
            else {
                labelIsolatedLine(e, 1);
            }
        }
    }
}

/*
 * Label an isolated node with its relationship to the target geometry.
 */
void
LineBuilder::labelIsolatedLine(Edge* e, int targetIndex)
{
    Location loc = ptLocator->locate(e->getCoordinate(),
                                op->getArgGeometry(targetIndex));
    e->getLabel().setLocation(targetIndex, loc);
}

} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos
