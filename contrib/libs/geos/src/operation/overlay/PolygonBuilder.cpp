/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/overlay/PolygonBuilder.java rev. 1.20 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/overlay/PolygonBuilder.h>
#include <geos/operation/overlay/OverlayOp.h>
#include <geos/operation/overlay/MaximalEdgeRing.h>
#include <geos/operation/overlay/MinimalEdgeRing.h>
#include <geos/operation/polygonize/EdgeRing.h>
#include <geos/geomgraph/Node.h>
#include <geos/geomgraph/NodeMap.h>
#include <geos/geomgraph/DirectedEdgeStar.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/util/TopologyException.h>
#include <geos/util/GEOSException.h>
#include <geos/util.h>


#include <vector>
#include <cassert>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

using namespace std;
using namespace geos::geomgraph;
using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay

PolygonBuilder::PolygonBuilder(const GeometryFactory* newGeometryFactory)
    :
    geometryFactory(newGeometryFactory)
{
}

PolygonBuilder::~PolygonBuilder()
{
    for(size_t i = 0, n = shellList.size(); i < n; ++i) {
        delete shellList[i];
    }
}

/*public*/
void
PolygonBuilder::add(PlanarGraph* graph)
//throw(TopologyException *)
{
    const vector<EdgeEnd*>* eeptr = graph->getEdgeEnds();
    assert(eeptr);
    const vector<EdgeEnd*>& ee = *eeptr;

    size_t eeSize = ee.size();

#if GEOS_DEBUG
    cerr << __FUNCTION__ << ": PlanarGraph has " << eeSize << " EdgeEnds" << endl;
#endif

    vector<DirectedEdge*> dirEdges(eeSize);
    for(size_t i = 0; i < eeSize; ++i) {
        DirectedEdge* de = detail::down_cast<DirectedEdge*>(ee[i]);
        dirEdges[i] = de;
    }

    NodeMap::container& nodeMap = graph->getNodeMap()->nodeMap;
    vector<Node*> nodes;
    nodes.reserve(nodeMap.size());
    for(NodeMap::iterator it = nodeMap.begin(), itEnd = nodeMap.end();
            it != itEnd; ++it) {
        Node* node = it->second;
        nodes.push_back(node);
    }

    add(&dirEdges, &nodes); // might throw a TopologyException *
}

/*public*/
void
PolygonBuilder::add(const vector<DirectedEdge*>* dirEdges,
                    const vector<Node*>* nodes)
//throw(TopologyException *)
{
    PlanarGraph::linkResultDirectedEdges(nodes->begin(), nodes->end());

    vector<MaximalEdgeRing*> maxEdgeRings;
    buildMaximalEdgeRings(dirEdges, maxEdgeRings);

    vector<EdgeRing*> freeHoleList;
    vector<MaximalEdgeRing*> edgeRings;
    buildMinimalEdgeRings(maxEdgeRings, shellList, freeHoleList, edgeRings);

    sortShellsAndHoles(edgeRings, shellList, freeHoleList);

    placeFreeHoles(shellList, freeHoleList);
    //Assert: every hole on freeHoleList has a shell assigned to it
}

/*public*/
vector<Geometry*>*
PolygonBuilder::getPolygons()
{
    vector<Geometry*>* resultPolyList = computePolygons(shellList);
    return resultPolyList;
}


/*private*/
void
PolygonBuilder::buildMaximalEdgeRings(const vector<DirectedEdge*>* dirEdges,
                                      vector<MaximalEdgeRing*>& maxEdgeRings)
// throw(const TopologyException &)
{
#if GEOS_DEBUG
    cerr << "PolygonBuilder::buildMaximalEdgeRings got " << dirEdges->size() << " dirEdges" << endl;
#endif

    vector<MaximalEdgeRing*>::size_type oldSize = maxEdgeRings.size();

    for(size_t i = 0, n = dirEdges->size(); i < n; i++) {
        DirectedEdge* de = (*dirEdges)[i];
#if GEOS_DEBUG
        cerr << "  dirEdge " << i << endl
             << de->printEdge() << endl
             << " inResult:" << de->isInResult() << endl
             << " isArea:" << de->getLabel().isArea() << endl;
#endif
        if(de->isInResult() && de->getLabel().isArea()) {
            // if this edge has not yet been processed
            if(de->getEdgeRing() == nullptr) {
                MaximalEdgeRing* er;
                try {
                    // MaximalEdgeRing constructor may throw
                    er = new MaximalEdgeRing(de, geometryFactory);
                }
                catch(util::GEOSException&) {
                    // cleanup if that happens (see stmlf-cases-20061020.xml)
                    for(size_t p_i = oldSize, p_n = maxEdgeRings.size(); p_i < p_n; p_i++) {
                        delete maxEdgeRings[p_i];
                    }
                    //cerr << "Exception! " << e.what() << endl;
                    throw;
                }
                maxEdgeRings.push_back(er);
                er->setInResult();
                //System.out.println("max node degree=" + er.getMaxDegree());
            }
        }
    }
#if GEOS_DEBUG
    cerr << "  pushed " << maxEdgeRings.size() - oldSize << " maxEdgeRings" << endl;
#endif
}

/*private*/
void
PolygonBuilder::buildMinimalEdgeRings(
    vector<MaximalEdgeRing*>& maxEdgeRings,
    vector<EdgeRing*>& newShellList, vector<EdgeRing*>& freeHoleList,
    vector<MaximalEdgeRing*>& edgeRings)
{
    for(size_t i = 0, n = maxEdgeRings.size(); i < n; ++i) {
        MaximalEdgeRing* er = maxEdgeRings[i];
#if GEOS_DEBUG
        cerr << "buildMinimalEdgeRings: maxEdgeRing " << i << " has " << er->getMaxNodeDegree() << " maxNodeDegree" << endl;
#endif
        if(er->getMaxNodeDegree() > 2) {
            er->linkDirectedEdgesForMinimalEdgeRings();
            vector<MinimalEdgeRing*> minEdgeRings;
            er->buildMinimalRings(minEdgeRings);
            // at this point we can go ahead and attempt to place
            // holes, if this EdgeRing is a polygon
            EdgeRing* shell = findShell(&minEdgeRings);
            if(shell != nullptr) {
                placePolygonHoles(shell, &minEdgeRings);
                newShellList.push_back(shell);
            }
            else {
                freeHoleList.insert(freeHoleList.end(),
                                    minEdgeRings.begin(),
                                    minEdgeRings.end());
            }
            delete er;
        }
        else {
            edgeRings.push_back(er);
        }
    }
}

/*private*/
EdgeRing*
PolygonBuilder::findShell(vector<MinimalEdgeRing*>* minEdgeRings)
{
    int shellCount = 0;
    EdgeRing* shell = nullptr;

#if GEOS_DEBUG
    cerr << "PolygonBuilder::findShell got " << minEdgeRings->size() << " minEdgeRings" << endl;
#endif

    for(size_t i = 0, n = minEdgeRings->size(); i < n; ++i) {
        EdgeRing* er = (*minEdgeRings)[i];
        if(! er->isHole()) {
            shell = er;
            ++shellCount;
        }
    }

    if(shellCount > 1) {
        throw util::TopologyException("found two shells in MinimalEdgeRing list");
    }

    return shell;
}

/*private*/
void
PolygonBuilder::placePolygonHoles(EdgeRing* shell,
                                  vector<MinimalEdgeRing*>* minEdgeRings)
{
    for(size_t i = 0, n = minEdgeRings->size(); i < n; ++i) {
        MinimalEdgeRing* er = (*minEdgeRings)[i];
        if(er->isHole()) {
            er->setShell(shell);
        }
    }
}

/*private*/
void
PolygonBuilder::sortShellsAndHoles(vector<MaximalEdgeRing*>& edgeRings,
                                   vector<EdgeRing*>& newShellList, vector<EdgeRing*>& freeHoleList)
{
    for(size_t i = 0, n = edgeRings.size(); i < n; i++) {
        EdgeRing* er = edgeRings[i];
        //er->setInResult();
        if(er->isHole()) {
            freeHoleList.push_back(er);
        }
        else {
            newShellList.push_back(er);
        }
    }
}

/*private*/
void
PolygonBuilder::placeFreeHoles(vector<EdgeRing*>& newShellList,
                               std::vector<EdgeRing*>& freeHoleList)
{
    for(std::vector<EdgeRing*>::iterator
            it = freeHoleList.begin(), itEnd = freeHoleList.end();
            it != itEnd;
            ++it) {
        EdgeRing* hole = *it;
        // only place this hole if it doesn't yet have a shell
        if(hole->getShell() == nullptr) {
            EdgeRing* shell = findEdgeRingContaining(hole, newShellList);
            if(shell == nullptr) {
#if GEOS_DEBUG
                Geometry* geom;
                std::cerr << "CREATE TABLE shells (g geometry);" << std::endl;
                std::cerr << "CREATE TABLE hole (g geometry);" << std::endl;
                for(std::vector<EdgeRing*>::iterator rIt = newShellList.begin(),
                        rEnd = newShellList.end(); rIt != rEnd; rIt++) {
                    geom = (*rIt)->toPolygon(geometryFactory);
                    std::cerr << "INSERT INTO shells VALUES ('"
                              << *geom
                              << "');" << std::endl;
                    delete geom;
                }
                geom = hole->toPolygon(geometryFactory);
                std::cerr << "INSERT INTO hole VALUES ('"
                          << *geom
                          << "');" << std::endl;
                delete geom;
#endif
                //assert(shell!=NULL); // unable to assign hole to a shell
                throw util::TopologyException("unable to assign hole to a shell");
            }

            hole->setShell(shell);
        }
    }
}

/*private*/
EdgeRing*
PolygonBuilder::findEdgeRingContaining(EdgeRing* testEr,
                                       vector<EdgeRing*>& newShellList)
{
    LinearRing* testRing = testEr->getLinearRing();
    const Envelope* testEnv = testRing->getEnvelopeInternal();
    EdgeRing* minShell = nullptr;
    const Envelope* minShellEnv = nullptr;

    for(auto const& tryShell : newShellList) {
        LinearRing* tryShellRing = tryShell->getLinearRing();
        const Envelope* tryShellEnv = tryShellRing->getEnvelopeInternal();
        // the hole envelope cannot equal the shell envelope
        // (also guards against testing rings against themselves)
        if(tryShellEnv->equals(testEnv)) {
            continue;
        }
        // hole must be contained in shell
        if(!tryShellEnv->contains(testEnv)) {
            continue;
        }

        const CoordinateSequence* tsrcs = tryShellRing->getCoordinatesRO();
        const Coordinate& testPt = operation::polygonize::EdgeRing::ptNotInList(testRing->getCoordinatesRO(), tsrcs);

        bool isContained = false;
        if(PointLocation::isInRing(testPt, tsrcs)) {
            isContained = true;
        }

        // check if this new containing ring is smaller than
        // the current minimum ring
        if(isContained) {
            if(minShell == nullptr
                    || minShellEnv->contains(tryShellEnv)) {
                minShell = tryShell;
                minShellEnv = minShell->getLinearRing()->getEnvelopeInternal();
            }
        }
    }
    return minShell;
}

/*private*/
vector<Geometry*>*
PolygonBuilder::computePolygons(vector<EdgeRing*>& newShellList)
{
#if GEOS_DEBUG
    cerr << "PolygonBuilder::computePolygons: got " << newShellList.size() << " shells" << endl;
#endif
    vector<Geometry*>* resultPolyList = new vector<Geometry*>();

    // add Polygons for all shells
    for(size_t i = 0, n = newShellList.size(); i < n; i++) {
        EdgeRing* er = newShellList[i];
        Polygon* poly = er->toPolygon(geometryFactory).release();
        resultPolyList->push_back(poly);
    }
    return resultPolyList;
}


} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

