/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/linemerge/LineMergeGraph.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/operation/linemerge/LineMergeGraph.h>
#include <geos/operation/linemerge/LineMergeEdge.h>
#include <geos/operation/linemerge/LineMergeDirectedEdge.h>
#include <geos/operation/valid/RepeatedPointRemover.h>
#include <geos/planargraph/DirectedEdge.h>
#include <geos/planargraph/Node.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/LineString.h>
#include <memory>

#include <vector>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

using namespace std;
//using namespace geos::planargraph;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace linemerge { // geos.operation.linemerge

void
LineMergeGraph::addEdge(const LineString* lineString)
{
    if(lineString->isEmpty()) {
        return;
    }

#if GEOS_DEBUG
    cerr << "Adding LineString " << lineString->toString() << endl;
#endif

    auto coordinates = valid::RepeatedPointRemover::removeRepeatedPoints(lineString->getCoordinatesRO());

    std::size_t nCoords = coordinates->size(); // virtual call..

    // don't add lines with all coordinates equal
    if(nCoords <= 1) {
        return;
    }

    const Coordinate& startCoordinate = coordinates->getAt(0);
    const Coordinate& endCoordinate = coordinates->getAt(nCoords - 1);

    planargraph::Node* startNode = getNode(startCoordinate);
    planargraph::Node* endNode = getNode(endCoordinate);
#if GEOS_DEBUG
    cerr << " startNode: " << *startNode << endl;
    cerr << " endNode: " << *endNode << endl;
#endif

    planargraph::DirectedEdge* directedEdge0 = new LineMergeDirectedEdge(startNode,
            endNode, coordinates->getAt(1),
            true);
    newDirEdges.push_back(directedEdge0);

    planargraph::DirectedEdge* directedEdge1 = new LineMergeDirectedEdge(endNode,
            startNode, coordinates->getAt(nCoords - 2),
            false);
    newDirEdges.push_back(directedEdge1);

    planargraph::Edge* edge = new LineMergeEdge(lineString);
    newEdges.push_back(edge);
    edge->setDirectedEdges(directedEdge0, directedEdge1);

#if GEOS_DEBUG
    cerr << " planargraph::Edge: " << *edge << endl;
#endif

    add(edge);

#if GEOS_DEBUG
    cerr << " After addition to the graph:" << endl;
    cerr << "  startNode: " << *startNode << endl;
    cerr << "  endNode: " << *endNode << endl;
#endif

}

planargraph::Node*
LineMergeGraph::getNode(const Coordinate& coordinate)
{
    planargraph::Node* node = findNode(coordinate);
    if(node == nullptr) {
        node = new planargraph::Node(coordinate);
        newNodes.push_back(node);
        add(node);
    }
    return node;
}

LineMergeGraph::~LineMergeGraph()
{
    unsigned int i;
    for(i = 0; i < newNodes.size(); i++) {
        delete newNodes[i];
    }
    for(i = 0; i < newEdges.size(); i++) {
        delete newEdges[i];
    }
    for(i = 0; i < newDirEdges.size(); i++) {
        delete newDirEdges[i];
    }
}

} // namespace geos.operation.linemerge
} // namespace geos.operation
} // namespace geos
