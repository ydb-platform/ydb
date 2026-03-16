/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/linemerge/LineSequencer.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/operation/linemerge/LineSequencer.h>
#include <geos/operation/linemerge/LineMergeEdge.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/LineString.h>
#include <geos/planargraph/Node.h>
#include <geos/planargraph/DirectedEdge.h>
#include <geos/planargraph/Subgraph.h>
#include <geos/planargraph/algorithm/ConnectedSubgraphFinder.h>
#include <geos/util/Assert.h>
#include <geos/util.h>

#include <cassert>
#include <limits>
#include <vector>

using namespace std;
//using namespace geos::planargraph;
using namespace geos::geom;
//using namespace geos::planargraph::algorithm;

#ifdef _MSC_VER
#pragma warning(disable : 4127)
#endif

namespace geos {
namespace operation { // geos.operation
namespace linemerge { // geos.operation.linemerge

/* static */
bool
LineSequencer::isSequenced(const Geometry* geom)
{
    const MultiLineString* mls;

    if(nullptr == (mls = dynamic_cast<const MultiLineString*>(geom))) {
        return true;
    }

    // the nodes in all subgraphs which have been completely scanned
    Coordinate::ConstSet prevSubgraphNodes;
    Coordinate::ConstVect currNodes;

    const Coordinate* lastNode = nullptr;

    for(std::size_t i = 0, n = mls->getNumGeometries(); i < n; ++i) {
        const LineString* lineptr = mls->getGeometryN(i);
        assert(lineptr);
        const LineString& line = *lineptr;


        const Coordinate* startNode = &(line.getCoordinateN(0));
        const Coordinate* endNode = &(line.getCoordinateN(line.getNumPoints() - 1));

        /*
         * If this linestring is connected to a previous subgraph,
         * geom is not sequenced
         */
        if(prevSubgraphNodes.find(startNode) != prevSubgraphNodes.end()) {
            return false;
        }
        if(prevSubgraphNodes.find(endNode) != prevSubgraphNodes.end()) {
            return false;
        }

        if(lastNode != nullptr) {
            if(! startNode->equals2D(*lastNode)) {
                // start new connected sequence
                prevSubgraphNodes.insert(currNodes.begin(),
                                         currNodes.end());
                currNodes.clear();
            }
        }
        currNodes.push_back(startNode);
        currNodes.push_back(endNode);
        lastNode = endNode;
    }
    return true;
}

/* private */
bool
LineSequencer::hasSequence(planargraph::Subgraph& p_graph)
{
    int oddDegreeCount = 0;
    for(planargraph::NodeMap::container::const_iterator
            it = p_graph.nodeBegin(), endIt = p_graph.nodeEnd();
            it != endIt;
            ++it) {
        planargraph::Node* node = it->second;
        if(node->getDegree() % 2 == 1) {
            oddDegreeCount++;
        }
    }
    return oddDegreeCount <= 2;
}

void
LineSequencer::delAll(LineSequencer::Sequences& s)
{
    for(Sequences::iterator i = s.begin(), e = s.end(); i != e; ++i) {
        delete *i;
    }
}

/*private*/
LineSequencer::Sequences*
LineSequencer::findSequences()
{
    Sequences* sequences = new Sequences();
    planargraph::algorithm::ConnectedSubgraphFinder csFinder(graph);
    vector<planargraph::Subgraph*> subgraphs;
    csFinder.getConnectedSubgraphs(subgraphs);
    for(vector<planargraph::Subgraph*>::const_iterator
            it = subgraphs.begin(), endIt = subgraphs.end();
            it != endIt;
            ++it) {
        planargraph::Subgraph* subgraph = *it;
        if(hasSequence(*subgraph)) {
            planargraph::DirectedEdge::NonConstList* seq = findSequence(*subgraph);
            sequences->push_back(seq);
        }
        else {
            // if any subgraph cannot be sequenced, abort
            delete subgraph;
            delAll(*sequences);
            delete sequences;
            return nullptr;
        }
        delete subgraph;
    }
    return sequences;
}

/*private*/
void
LineSequencer::addLine(const LineString* lineString)
{
    if(factory == nullptr) {
        factory = lineString->getFactory();
    }
    graph.addEdge(lineString);
    ++lineCount;
}

/* private */
void
LineSequencer::computeSequence()
{
    if(isRun) {
        return;
    }
    isRun = true;

    Sequences* sequences = findSequences();
    if(sequences == nullptr) {
        return;
    }

    sequencedGeometry = unique_ptr<Geometry>(buildSequencedGeometry(*sequences));
    isSequenceableVar = true;

    delAll(*sequences);
    delete sequences;

    // Lines were missing from result
    assert(lineCount == sequencedGeometry->getNumGeometries());

    // Result is not linear
    assert(dynamic_cast<LineString*>(sequencedGeometry.get())
           || dynamic_cast<MultiLineString*>(sequencedGeometry.get()));
}

/*private*/
Geometry*
LineSequencer::buildSequencedGeometry(const Sequences& sequences)
{
    unique_ptr<Geometry::NonConstVect> lines(new Geometry::NonConstVect);

    for(Sequences::const_iterator
            i1 = sequences.begin(), i1End = sequences.end();
            i1 != i1End;
            ++i1) {
        planargraph::DirectedEdge::NonConstList& seq = *(*i1);
        for(planargraph::DirectedEdge::NonConstList::iterator i2 = seq.begin(),
                i2End = seq.end(); i2 != i2End; ++i2) {
            const planargraph::DirectedEdge* de = *i2;
            LineMergeEdge* e = detail::down_cast<LineMergeEdge* >(de->getEdge());
            const LineString* line = e->getLine();

            // lineToAdd will be a *copy* of input things
            LineString* lineToAdd;

            if(! de->getEdgeDirection() && ! line->isClosed()) {
                lineToAdd = reverse(line);
            }
            else {
                Geometry* lineClone = line->clone().release();
                lineToAdd = detail::down_cast<LineString*>(lineClone);
            }

            lines->push_back(lineToAdd);
        }
    }

    if(lines->empty()) {
        return nullptr;
    }
    else {
        Geometry::NonConstVect* l = lines.get();
        lines.release();
        return factory->buildGeometry(l);
    }
}

/*static private*/
LineString*
LineSequencer::reverse(const LineString* line)
{
    auto cs = line->getCoordinates();
    CoordinateSequence::reverse(cs.get());
    return line->getFactory()->createLineString(cs.release());
}

/*private static*/
const planargraph::Node*
LineSequencer::findLowestDegreeNode(const planargraph::Subgraph& graph)
{
    size_t minDegree = numeric_limits<size_t>::max();
    const planargraph::Node* minDegreeNode = nullptr;
    for(planargraph::NodeMap::container::const_iterator
            it = graph.nodeBegin(), itEnd = graph.nodeEnd();
            it != itEnd;
            ++it) {
        const planargraph::Node* node = (*it).second;
        if(minDegreeNode == nullptr || node->getDegree() < minDegree) {
            minDegree = node->getDegree();
            minDegreeNode = node;
        }
    }
    return minDegreeNode;
}

/*private static*/
const planargraph::DirectedEdge*
LineSequencer::findUnvisitedBestOrientedDE(const planargraph::Node* node)
{
    using planargraph::DirectedEdge;
    using planargraph::DirectedEdgeStar;

    const DirectedEdge* wellOrientedDE = nullptr;
    const DirectedEdge* unvisitedDE = nullptr;
    const DirectedEdgeStar* des = node->getOutEdges();
    for(DirectedEdge::NonConstVect::const_iterator i = des->begin(),
            e = des->end();
            i != e;
            ++i) {
        planargraph::DirectedEdge* de = *i;
        if(! de->getEdge()->isVisited()) {
            unvisitedDE = de;
            if(de->getEdgeDirection()) {
                wellOrientedDE = de;
            }
        }
    }
    if(wellOrientedDE != nullptr) {
        return wellOrientedDE;
    }
    return unvisitedDE;
}


/*private*/
void
LineSequencer::addReverseSubpath(const planargraph::DirectedEdge* de,
                                 planargraph::DirectedEdge::NonConstList& deList,
                                 planargraph::DirectedEdge::NonConstList::iterator lit,
                                 bool expectedClosed)
{
    using planargraph::Node;
    using planargraph::DirectedEdge;

    // trace an unvisited path *backwards* from this de
    Node* endNode = de->getToNode();

    Node* fromNode = nullptr;
    while(true) {
        deList.insert(lit, de->getSym());
        de->getEdge()->setVisited(true);
        fromNode = de->getFromNode();
        const DirectedEdge* unvisitedOutDE = findUnvisitedBestOrientedDE(fromNode);

        // this must terminate, since we are continually marking edges as visited
        if(unvisitedOutDE == nullptr) {
            break;
        }
        de = unvisitedOutDE->getSym();
    }
    if(expectedClosed) {
        // the path should end at the toNode of this de,
        // otherwise we have an error
        util::Assert::isTrue(fromNode == endNode, "path not contiguos");
        //assert(fromNode == endNode);
    }

}

/*private*/
planargraph::DirectedEdge::NonConstList*
LineSequencer::findSequence(planargraph::Subgraph& p_graph)
{
    using planargraph::DirectedEdge;
    using planargraph::Node;
    using planargraph::GraphComponent;

    GraphComponent::setVisited(p_graph.edgeBegin(),
                               p_graph.edgeEnd(), false);

    const Node* startNode = findLowestDegreeNode(p_graph);

    const DirectedEdge* startDE = *(startNode->getOutEdges()->begin());
    const DirectedEdge* startDESym = startDE->getSym();

    DirectedEdge::NonConstList* seq = new DirectedEdge::NonConstList();

    DirectedEdge::NonConstList::iterator lit = seq->begin();
    addReverseSubpath(startDESym, *seq, lit, false);

    lit = seq->end();
    while(lit != seq->begin()) {
        const DirectedEdge* prev = *(--lit);
        const DirectedEdge* unvisitedOutDE = findUnvisitedBestOrientedDE(prev->getFromNode());
        if(unvisitedOutDE != nullptr) {
            addReverseSubpath(unvisitedOutDE->getSym(), *seq, lit, true);
        }
    }

    // At this point, we have a valid sequence of graph DirectedEdges,
    // but it is not necessarily appropriately oriented relative to
    // the underlying geometry.
    DirectedEdge::NonConstList* orientedSeq = orient(seq);

    if(orientedSeq != seq) {
        delete seq;
    }

    return orientedSeq;
}

/* private */
planargraph::DirectedEdge::NonConstList*
LineSequencer::orient(planargraph::DirectedEdge::NonConstList* seq)
{
    using namespace geos::planargraph;

    const DirectedEdge* startEdge = seq->front();
    const DirectedEdge* endEdge = seq->back();
    Node* startNode = startEdge->getFromNode();
    Node* endNode = endEdge->getToNode();

    bool flipSeq = false;
    bool hasDegree1Node = \
                          startNode->getDegree() == 1 || endNode->getDegree() == 1;

    if(hasDegree1Node) {
        bool hasObviousStartNode = false;

        // test end edge before start edge, to make result stable
        // (ie. if both are good starts, pick the actual start
        if(endEdge->getToNode()->getDegree() == 1 &&
                endEdge->getEdgeDirection() == false) {
            hasObviousStartNode = true;
            flipSeq = true;
        }
        if(startEdge->getFromNode()->getDegree() == 1 &&
                startEdge->getEdgeDirection() == true) {
            hasObviousStartNode = true;
            flipSeq = false;
        }

        // since there is no obvious start node,
        // use any node of degree 1
        if(! hasObviousStartNode) {
            // check if the start node should actually
            // be the end node
            if(startEdge->getFromNode()->getDegree() == 1) {
                flipSeq = true;
            }
            // if the end node is of degree 1, it is
            // properly the end node
        }

    }


    // if there is no degree 1 node, just use the sequence as is
    // (Could insert heuristic of taking direction of majority of
    // lines as overall direction)

    if(flipSeq) {
        return reverse(*seq);
    }
    return seq;
}

/* private */
planargraph::DirectedEdge::NonConstList*
LineSequencer::reverse(planargraph::DirectedEdge::NonConstList& seq)
{
    using namespace geos::planargraph;

    DirectedEdge::NonConstList* newSeq = new DirectedEdge::NonConstList();
    DirectedEdge::NonConstList::iterator it = seq.begin(), itEnd = seq.end();
    for(; it != itEnd; ++it) {
        const DirectedEdge* de = *it;
        newSeq->push_front(de->getSym());
    }
    return newSeq;
}



} // namespace geos.operation.linemerge
} // namespace geos.operation
} // namespace geos
