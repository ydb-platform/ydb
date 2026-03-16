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
 **********************************************************************
 *
 * Last port: operation/polygonize/PolygonizeGraph.java rev. 6/138 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/polygonize/PolygonizeGraph.h>
#include <geos/operation/polygonize/PolygonizeDirectedEdge.h>
#include <geos/operation/polygonize/PolygonizeEdge.h>
#include <geos/operation/polygonize/EdgeRing.h>
#include <geos/operation/valid/RepeatedPointRemover.h>
#include <geos/planargraph/Node.h>
#include <geos/planargraph/DirectedEdgeStar.h>
#include <geos/planargraph/DirectedEdge.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/LineString.h>
#include <geos/util.h>

#include <cassert>
#include <vector>
#include <set>

//using namespace std;
using namespace geos::planargraph;
using namespace geos::geom;

// Define the following to add assertions on downcasts
//#define GEOS_CAST_PARANOIA 1

namespace geos {
namespace operation { // geos.operation
namespace polygonize { // geos.operation.polygonize

int
PolygonizeGraph::getDegreeNonDeleted(Node* node)
{
    auto edges = node->getOutEdges()->getEdges();
    int degree = 0;
    for(const auto& de : edges) {
        if(!de->isMarked()) {
            ++degree;
        }
    }
    return degree;
}

int
PolygonizeGraph::getDegree(Node* node, long label)
{
    auto edges = node->getOutEdges()->getEdges();
    int degree = 0;
    for(const auto& de : edges) {
        auto pde = detail::down_cast<PolygonizeDirectedEdge*>(de);
        if(pde->getLabel() == label) {
            ++degree;
        }
    }
    return degree;
}

/**
 * Deletes all edges at a node
 */
void
PolygonizeGraph::deleteAllEdges(Node* node)
{
    auto edges = node->getOutEdges()->getEdges();
    for(const auto& de : edges) {
        de->setMarked(true);
        auto sym = de->getSym();
        if(sym != nullptr) {
            sym->setMarked(true);
        }
    }
}

/*
 * Create a new polygonization graph.
 */
PolygonizeGraph::PolygonizeGraph(const GeometryFactory* newFactory):
    factory(newFactory)
{
}

/*
 * Destroy a PolygonizeGraph
 */
PolygonizeGraph::~PolygonizeGraph()
{
    unsigned int i;
    for(i = 0; i < newEdges.size(); i++) {
        delete newEdges[i];
    }
    for(i = 0; i < newDirEdges.size(); i++) {
        delete newDirEdges[i];
    }
    for(i = 0; i < newNodes.size(); i++) {
        delete newNodes[i];
    }
    for(i = 0; i < newEdgeRings.size(); i++) {
        delete newEdgeRings[i];
    }
    for(i = 0; i < newCoords.size(); i++) {
        delete newCoords[i];
    }
}

/*
 * Add a LineString forming an edge of the polygon graph.
 * @param line the line to add
 */
void
PolygonizeGraph::addEdge(const LineString* line)
{
    if(line->isEmpty()) {
        return;
    }

    auto linePts = valid::RepeatedPointRemover::removeRepeatedPoints(line->getCoordinatesRO());

    /*
     * This would catch invalid linestrings
     * (containing duplicated points only)
     */
    if(linePts->getSize() < 2) {
        return;
    }

    const Coordinate& startPt = linePts->getAt(0);
    const Coordinate& endPt = linePts->getAt(linePts->getSize() - 1);
    Node* nStart = getNode(startPt);
    Node* nEnd = getNode(endPt);
    DirectedEdge* de0 = new PolygonizeDirectedEdge(nStart, nEnd, linePts->getAt(1), true);
    newDirEdges.push_back(de0);
    DirectedEdge* de1 = new PolygonizeDirectedEdge(nEnd, nStart,
            linePts->getAt(linePts->getSize() - 2), false);
    newDirEdges.push_back(de1);
    Edge* edge = new PolygonizeEdge(line);
    newEdges.push_back(edge);
    edge->setDirectedEdges(de0, de1);
    add(edge);

    newCoords.push_back(linePts.release());
}

Node*
PolygonizeGraph::getNode(const Coordinate& pt)
{
    Node* node = findNode(pt);
    if(node == nullptr) {
        node = new Node(pt);
        newNodes.push_back(node);
        // ensure node is only added once to graph
        add(node);
    }
    return node;
}

void
PolygonizeGraph::computeNextCWEdges()
{
    typedef std::vector<Node*> Nodes;
    Nodes pns;
    getNodes(pns);
    // set the next pointers for the edges around each node
    for(Node* node : pns) {
        computeNextCWEdges(node);
    }
}

/* private */
void
PolygonizeGraph::convertMaximalToMinimalEdgeRings(
    std::vector<PolygonizeDirectedEdge*>& ringEdges)
{
    std::vector<Node*> intNodes;
    for(PolygonizeDirectedEdge* de : ringEdges) {
        long p_label = de->getLabel();
        findIntersectionNodes(de, p_label, intNodes);

        // set the next pointers for the edges around each node
        for(Node* node : intNodes) {
            computeNextCCWEdges(node, p_label);
        }

        intNodes.clear();
    }
}

/* private static */
void
PolygonizeGraph::findIntersectionNodes(PolygonizeDirectedEdge* startDE,
                                       long label, std::vector<Node*>& intNodes)
{
    PolygonizeDirectedEdge* de = startDE;
    do {
        Node* node = de->getFromNode();
        if(getDegree(node, label) > 1) {
            intNodes.push_back(node);
        }
        de = de->getNext();
        assert(de != nullptr); // found NULL DE in ring
        assert(de == startDE || !de->isInRing()); // found DE already in ring
    }
    while(de != startDE);
}

/* public */
void
PolygonizeGraph::getEdgeRings(std::vector<EdgeRing*>& edgeRingList)
{
    // maybe could optimize this, since most of these pointers should
    // be set correctly already
    // by deleteCutEdges()
    computeNextCWEdges();

    // clear labels of all edges in graph
    label(dirEdges, -1);
    std::vector<PolygonizeDirectedEdge*> maximalRings;
    findLabeledEdgeRings(dirEdges, maximalRings);
    convertMaximalToMinimalEdgeRings(maximalRings);
    maximalRings.clear(); // not needed anymore

    // find all edgerings
    for(DirectedEdge* de : dirEdges) {
        auto pde = detail::down_cast<PolygonizeDirectedEdge*>(de);
        if(pde->isMarked()) {
            continue;
        }
        if(pde->isInRing()) {
            continue;
        }
        EdgeRing* er = findEdgeRing(pde);
        edgeRingList.push_back(er);
    }
}

/* static private */
void
PolygonizeGraph::findLabeledEdgeRings(std::vector<DirectedEdge*>& dirEdges,
                                      std::vector<PolygonizeDirectedEdge*>& edgeRingStarts)
{
    // label the edge rings formed
    long currLabel = 1;
    for(DirectedEdge* de : dirEdges) {
        auto pde = detail::down_cast<PolygonizeDirectedEdge*>(de);

        if(pde->isMarked()) {
            continue;
        }
        if(pde->getLabel() >= 0) {
            continue;
        }
        edgeRingStarts.push_back(pde);

        auto edges = EdgeRing::findDirEdgesInRing(pde);
        label(edges, currLabel);
        edges.clear();

        ++currLabel;
    }
}

/* public */
void
PolygonizeGraph::deleteCutEdges(std::vector<const LineString*>& cutLines)
{
    computeNextCWEdges();

    typedef std::vector<PolygonizeDirectedEdge*> DirEdges;

    // label the current set of edgerings
    DirEdges junk;
    findLabeledEdgeRings(dirEdges, junk);
    junk.clear(); // not needed anymore

    /*
     * Cut Edges are edges where both dirEdges have the same label.
     * Delete them, and record them
     */
    for(DirectedEdge* de : dirEdges) {
        auto pde = detail::down_cast<PolygonizeDirectedEdge*>(de);

        if(de->isMarked()) {
            continue;
        }

        auto sym = detail::down_cast<PolygonizeDirectedEdge*>(de->getSym());

        if(pde->getLabel() == sym->getLabel()) {
            de->setMarked(true);
            sym->setMarked(true);

            // save the line as a cut edge
            auto e = detail::down_cast<PolygonizeEdge*>(de->getEdge());

            cutLines.push_back(e->getLine());
        }
    }
}

void
PolygonizeGraph::label(std::vector<PolygonizeDirectedEdge*>& dirEdges, long label)
{
    for (auto & pde: dirEdges) {
        pde->setLabel(label);
    }
}

void
PolygonizeGraph::label(std::vector<DirectedEdge*>& dirEdges, long label)
{
    for(auto& de : dirEdges) {
        auto pde = detail::down_cast<PolygonizeDirectedEdge*>(de);
        pde->setLabel(label);
    }
}

void
PolygonizeGraph::computeNextCWEdges(Node* node)
{
    DirectedEdgeStar* deStar = node->getOutEdges();
    PolygonizeDirectedEdge* startDE = nullptr;
    PolygonizeDirectedEdge* prevDE = nullptr;

    // the edges are stored in CCW order around the star
    std::vector<DirectedEdge*>& pde = deStar->getEdges();
    for(DirectedEdge* de : pde) {
        auto outDE = detail::down_cast<PolygonizeDirectedEdge*>(de);
        if(outDE->isMarked()) {
            continue;
        }
        if(startDE == nullptr) {
            startDE = outDE;
        }
        if(prevDE != nullptr) {
            auto sym = detail::down_cast<PolygonizeDirectedEdge*>(prevDE->getSym());
            sym->setNext(outDE);
        }
        prevDE = outDE;
    }
    if(prevDE != nullptr) {
        auto sym = detail::down_cast<PolygonizeDirectedEdge*>(prevDE->getSym());
        sym->setNext(startDE);
    }
}

/**
 * Computes the next edge pointers going CCW around the given node, for the
 * given edgering label.
 * This algorithm has the effect of converting maximal edgerings into
 * minimal edgerings
 */
void
PolygonizeGraph::computeNextCCWEdges(Node* node, long label)
{
    DirectedEdgeStar* deStar = node->getOutEdges();
    PolygonizeDirectedEdge* firstOutDE = nullptr;
    PolygonizeDirectedEdge* prevInDE = nullptr;

    // the edges are stored in CCW order around the star
    std::vector<DirectedEdge*>& edges = deStar->getEdges();

    for(auto i = edges.size(); i > 0; --i) {
        PolygonizeDirectedEdge* de = detail::down_cast<PolygonizeDirectedEdge*>(edges[i - 1]);
        PolygonizeDirectedEdge* sym = detail::down_cast<PolygonizeDirectedEdge*>(de->getSym());
        PolygonizeDirectedEdge* outDE = nullptr;
        if(de->getLabel() == label) {
            outDE = de;
        }
        PolygonizeDirectedEdge* inDE = nullptr;
        if(sym->getLabel() == label) {
            inDE = sym;
        }
        if(outDE == nullptr && inDE == nullptr) {
            continue;    // this edge is not in edgering
        }
        if(inDE != nullptr) {
            prevInDE = inDE;
        }
        if(outDE != nullptr) {
            if(prevInDE != nullptr) {
                prevInDE->setNext(outDE);
                prevInDE = nullptr;
            }
            if(firstOutDE == nullptr) {
                firstOutDE = outDE;
            }
        }
    }
    if(prevInDE != nullptr) {
        assert(firstOutDE != nullptr);
        prevInDE->setNext(firstOutDE);
    }
}

EdgeRing*
PolygonizeGraph::findEdgeRing(PolygonizeDirectedEdge* startDE)
{
    PolygonizeDirectedEdge* de = startDE;
    EdgeRing* er = new EdgeRing(factory);
    // Now, when will we delete those EdgeRings ?
    newEdgeRings.push_back(er);
    do {
        er->add(de);
        de->setRing(er);
        de = de->getNext();
        assert(de != nullptr); // found NULL DE in ring
        assert(de == startDE || ! de->isInRing()); // found DE already in ring
    }
    while(de != startDE);
    return er;
}

/* public */
void
PolygonizeGraph::deleteDangles(std::vector<const LineString*>& dangleLines)
{
    std::vector<Node*> nodeStack;
    findNodesOfDegree(1, nodeStack);

    std::set<const LineString*> uniqueDangles;

    while(!nodeStack.empty()) {
        Node* node = nodeStack.back();
        nodeStack.pop_back();
        deleteAllEdges(node);
        auto nodeOutEdges = node->getOutEdges()->getEdges();
        for(DirectedEdge* de : nodeOutEdges) {
            // delete this edge and its sym
            de->setMarked(true);
            auto sym = dynamic_cast<PolygonizeDirectedEdge*>(de->getSym());
            if(sym != nullptr) {
                sym->setMarked(true);
            }
            // save the line as a dangle
            auto e = detail::down_cast<PolygonizeEdge*>(de->getEdge());
            const LineString* ls = e->getLine();
            if(uniqueDangles.insert(ls).second) {
                dangleLines.push_back(ls);
            }
            Node* toNode = de->getToNode();
            // add the toNode to the list to be processed,
            // if it is now a dangle
            if(getDegreeNonDeleted(toNode) == 1) {
                nodeStack.push_back(toNode);
            }
        }
    }

}

} // namespace geos.operation.polygonize
} // namespace geos.operation
} // namespace geos

