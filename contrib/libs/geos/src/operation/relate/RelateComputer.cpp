/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2005 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/relate/RelateComputer.java rev. 1.24 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/relate/RelateComputer.h>
#include <geos/operation/relate/RelateNodeFactory.h>
#include <geos/operation/relate/RelateNode.h>
#include <geos/operation/relate/EdgeEndBuilder.h>
#include <geos/algorithm/BoundaryNodeRule.h>
#include <geos/algorithm/LineIntersector.h>
#include <geos/algorithm/PointLocator.h>
#include <geos/geom/IntersectionMatrix.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Envelope.h>
#include <geos/geomgraph/index/SegmentIntersector.h>
#include <geos/geomgraph/GeometryGraph.h>
#include <geos/geomgraph/Label.h>
#include <geos/geomgraph/Edge.h>
#include <geos/geomgraph/EdgeEndStar.h>
#include <geos/geomgraph/Node.h>
#include <geos/geomgraph/EdgeIntersectionList.h>
#include <geos/geomgraph/EdgeIntersection.h>
#include <geos/operation/BoundaryOp.h>

#include <geos/util/Interrupt.h>
#include <geos/util.h>

#include <vector>
#include <cassert>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#if GEOS_DEBUG
# include <iostream>
#endif

using namespace geos::geom;
using namespace geos::geomgraph;
using namespace geos::geomgraph::index;
using namespace geos::algorithm;

namespace geos {
namespace operation { // geos.operation
namespace relate { // geos.operation.relate

RelateComputer::RelateComputer(std::vector<GeometryGraph*>* newArg):
    arg(newArg),
    nodes(RelateNodeFactory::instance()),
    im(new IntersectionMatrix())
{
}

std::unique_ptr<IntersectionMatrix>
RelateComputer::computeIM()
{
    // since Geometries are finite and embedded in a 2-D space, the EE element must always be 2
    im->set(Location::EXTERIOR, Location::EXTERIOR, 2);
    // if the Geometries don't overlap there is nothing to do
    const Envelope* e1 = (*arg)[0]->getGeometry()->getEnvelopeInternal();
    const Envelope* e2 = (*arg)[1]->getGeometry()->getEnvelopeInternal();
    if(!e1->intersects(e2)) {
        computeDisjointIM(im.get(), (*arg)[0]->getBoundaryNodeRule());
        return std::move(im);
    }

#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "computing self nodes 1"
              << std::endl;
#endif

    std::unique_ptr<SegmentIntersector> si1(
        (*arg)[0]->computeSelfNodes(&li, false)
    );

    GEOS_CHECK_FOR_INTERRUPTS();

#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "computing self nodes 2"
              << std::endl;
#endif

    std::unique_ptr<SegmentIntersector> si2(
        (*arg)[1]->computeSelfNodes(&li, false)
    );

    GEOS_CHECK_FOR_INTERRUPTS();

#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "computing edge intersections"
              << std::endl;
#endif

    // compute intersections between edges of the two input geometries
    std::unique_ptr< SegmentIntersector> intersector(
        (*arg)[0]->computeEdgeIntersections((*arg)[1], &li, false)
    );

    GEOS_CHECK_FOR_INTERRUPTS();

#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "copying intersection nodes"
              << std::endl;
#endif

    computeIntersectionNodes(0);
    computeIntersectionNodes(1);

    GEOS_CHECK_FOR_INTERRUPTS();

#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "copying nodes and labels"
              << std::endl;
#endif

    GEOS_CHECK_FOR_INTERRUPTS();

    /*
     * Copy the labelling for the nodes in the parent Geometries.
     * These override any labels determined by intersections
     * between the geometries.
     */
    copyNodesAndLabels(0);
    copyNodesAndLabels(1);

    GEOS_CHECK_FOR_INTERRUPTS();

    /*
     * complete the labelling for any nodes which only have a
     * label for a single geometry
     */
    //Debug.addWatch(nodes.find(new Coordinate(110, 200)));
    //Debug.printWatch();
#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "labeling isolated nodes"
              << std::endl;
#endif
    labelIsolatedNodes();
    //Debug.printWatch();

#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "computing proper intersection matrix"
              << std::endl;
#endif

    /*
     * If a proper intersection was found, we can set a lower bound
     * on the IM.
     */
    computeProperIntersectionIM(intersector.get(), im.get());

#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "computing improper intersections"
              << std::endl;
#endif

    /*
     * Now process improper intersections
     * (eg where one or other of the geometrys has a vertex at the
     * intersection point)
     * We need to compute the edge graph at all nodes to determine
     * the IM.
     */
    // build EdgeEnds for all intersections
    EdgeEndBuilder eeBuilder;
    std::vector<EdgeEnd*> ee0 = eeBuilder.computeEdgeEnds((*arg)[0]->getEdges());
    insertEdgeEnds(&ee0);
    std::vector<EdgeEnd*> ee1 = eeBuilder.computeEdgeEnds((*arg)[1]->getEdges());

#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "inserting edge ends"
              << std::endl;
#endif

    insertEdgeEnds(&ee1);

#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "labeling node edges"
              << std::endl;
#endif

    labelNodeEdges();

    /*
     * Compute the labeling for isolated components.
     * Isolated components are components that do not touch any
     * other components in the graph.
     * They can be identified by the fact that they will
     * contain labels containing ONLY a single element, the one for
     * their parent geometry.
     * We only need to check components contained in the input graphs,
     * since isolated components will not have been replaced by new
     * components formed by intersections.
     */
#if GEOS_DEBUG
    std::cerr << "RelateComputer::computeIM: "
              << "computing labeling for isolated components"
              << std::endl;
#endif
    //debugPrintln("Graph A isolated edges - ");
    labelIsolatedEdges(0, 1);
    //debugPrintln("Graph B isolated edges - ");
    labelIsolatedEdges(1, 0);
    // update the IM from all components
    updateIM(*im);
    return std::move(im);
}

void
RelateComputer::insertEdgeEnds(std::vector<EdgeEnd*>* ee)
{
    for(EdgeEnd* e: *ee) {
        nodes.add(e);
    }
}

/* private */
void
RelateComputer::computeProperIntersectionIM(SegmentIntersector* intersector, IntersectionMatrix* imX)
{
    // If a proper intersection is found, we can set a lower bound on the IM.
    int dimA = (*arg)[0]->getGeometry()->getDimension();
    int dimB = (*arg)[1]->getGeometry()->getDimension();
    bool hasProper = intersector->hasProperIntersection();
    bool hasProperInterior = intersector->hasProperInteriorIntersection();
    // For Geometry's of dim 0 there can never be proper intersections.
    /*
     * If edge segments of Areas properly intersect, the areas must properly overlap.
     */
    if(dimA == 2 && dimB == 2) {
        if(hasProper) {
            imX->setAtLeast("212101212");
        }
    }
    /*
     * If an Line segment properly intersects an edge segment of an Area,
     * it follows that the Interior of the Line intersects the Boundary of the Area.
     * If the intersection is a proper *interior* intersection, then
     * there is an Interior-Interior intersection too.
     * Note that it does not follow that the Interior of the Line intersects the Exterior
     * of the Area, since there may be another Area component which contains the rest of the Line.
     */
    else if(dimA == 2 && dimB == 1) {
        if(hasProper) {
            imX->setAtLeast("FFF0FFFF2");
        }
        if(hasProperInterior) {
            imX->setAtLeast("1FFFFF1FF");
        }
    }
    else if(dimA == 1 && dimB == 2) {
        if(hasProper) {
            imX->setAtLeast("F0FFFFFF2");
        }
        if(hasProperInterior) {
            imX->setAtLeast("1F1FFFFFF");
        }
    }
    /* If edges of LineStrings properly intersect *in an interior point*, all
    we can deduce is that
    the interiors intersect.  (We can NOT deduce that the exteriors intersect,
    since some other segments in the geometries might cover the points in the
    neighbourhood of the intersection.)
    It is important that the point be known to be an interior point of
    both Geometries, since it is possible in a self-intersecting geometry to
    have a proper intersection on one segment that is also a boundary point of another segment.
    */
    else if(dimA == 1 && dimB == 1) {
        if(hasProperInterior) {
            imX->setAtLeast("0FFFFFFFF");
        }
    }
}

/**
 * Copy all nodes from an arg geometry into this graph.
 * The node label in the arg geometry overrides any previously computed
 * label for that argIndex.
 * (E.g. a node may be an intersection node with
 * a computed label of BOUNDARY,
 * but in the original arg Geometry it is actually
 * in the interior due to the Boundary Determination Rule)
 */
void
RelateComputer::copyNodesAndLabels(int argIndex)
{
    const NodeMap* nm = (*arg)[argIndex]->getNodeMap();
    for(const auto& it: *nm) {
        const Node* graphNode = it.second;
        Node* newNode = nodes.addNode(graphNode->getCoordinate());
        newNode->setLabel(argIndex,
                          graphNode->getLabel().getLocation(argIndex));
        //node.print(System.out);
    }
}


/**
 * Insert nodes for all intersections on the edges of a Geometry.
 * Label the created nodes the same as the edge label if they do not
 * already have a label.
 * This allows nodes created by either self-intersections or
 * mutual intersections to be labelled.
 * Endpoint nodes will already be labelled from when they were inserted.
 */
void
RelateComputer::computeIntersectionNodes(int argIndex)
{
    std::vector<Edge*>* edges = (*arg)[argIndex]->getEdges();
    for(Edge* e: *edges) {
        Location eLoc = e->getLabel().getLocation(argIndex);
        EdgeIntersectionList& eiL = e->getEdgeIntersectionList();
        for(const EdgeIntersection & ei : eiL) {
            RelateNode* n = detail::down_cast<RelateNode*>(nodes.addNode(ei.coord));
            if(eLoc == Location::BOUNDARY) {
                n->setLabelBoundary(argIndex);
            }
            else {
                if(n->getLabel().isNull(argIndex)) {
                    n->setLabel(argIndex, Location::INTERIOR);
                }
            }
        }
    }
}

/*
 * For all intersections on the edges of a Geometry,
 * label the corresponding node IF it doesn't already have a label.
 * This allows nodes created by either self-intersections or
 * mutual intersections to be labelled.
 * Endpoint nodes will already be labelled from when they were inserted.
 */
void
RelateComputer::labelIntersectionNodes(int argIndex)
{
    std::vector<Edge*>* edges = (*arg)[argIndex]->getEdges();
    for(Edge* e: *edges) {
        Location eLoc = e->getLabel().getLocation(argIndex);
        EdgeIntersectionList& eiL = e->getEdgeIntersectionList();

        for(const EdgeIntersection& ei : eiL) {
            RelateNode* n = (RelateNode*) nodes.find(ei.coord);
            if(n->getLabel().isNull(argIndex)) {
                if(eLoc == Location::BOUNDARY) {
                    n->setLabelBoundary(argIndex);
                }
                else {
                    n->setLabel(argIndex, Location::INTERIOR);
                }
            }
        }
    }
}

/* private */
void
RelateComputer::computeDisjointIM(IntersectionMatrix* imX, const algorithm::BoundaryNodeRule& boundaryNodeRule)
{
    const Geometry* ga = (*arg)[0]->getGeometry();
    if(!ga->isEmpty()) {
        imX->set(Location::INTERIOR, Location::EXTERIOR, ga->getDimension());
        imX->set(Location::BOUNDARY, Location::EXTERIOR, getBoundaryDim(*ga, boundaryNodeRule));
    }
    const Geometry* gb = (*arg)[1]->getGeometry();
    if(!gb->isEmpty()) {
        imX->set(Location::EXTERIOR, Location::INTERIOR, gb->getDimension());
        imX->set(Location::EXTERIOR, Location::BOUNDARY, getBoundaryDim(*gb, boundaryNodeRule));
    }
}

int
RelateComputer::getBoundaryDim(const Geometry& geom, const algorithm::BoundaryNodeRule& boundaryNodeRule)
{
    // If the geometry has a non-empty boundary
    // the intersection is the nominal dimension.
    if (BoundaryOp::hasBoundary(geom, boundaryNodeRule)) {
        /**
      * special case for lines, since Geometry.getBoundaryDimension is not aware
      * of Boundary Node Rule.
      */
        if (geom.getDimension() == 1)
            return Dimension::P;
        return geom.getBoundaryDimension();
    }

    // Otherwise intersection is F
    return Dimension::False;
}

void
RelateComputer::labelNodeEdges()
{
    auto& nMap = nodes.nodeMap;
    for(auto& entry : nMap) {
        RelateNode* node = detail::down_cast<RelateNode*>(entry.second);
#if GEOS_DEBUG
        std::cerr << "RelateComputer::labelNodeEdges: "
                  << "node edges: " << *(node->getEdges())
                  << std::endl;
#endif
        node->getEdges()->computeLabelling(arg);
        //Debug.print(node.getEdges());
        //node.print(System.out);
    }
}

/*private*/
void
RelateComputer::updateIM(IntersectionMatrix& imX)
{
    std::vector<Edge*>::iterator ei = isolatedEdges.begin();
    for(; ei < isolatedEdges.end(); ++ei) {
        Edge* e = *ei;
        e->GraphComponent::updateIM(imX);
    }
    auto& nMap = nodes.nodeMap;
    for(auto& entry : nMap) {
        RelateNode* node = (RelateNode*) entry.second;
        node->updateIM(imX);
        node->updateIMFromEdges(imX);
    }
}

/*private*/
void
RelateComputer::labelIsolatedEdges(int thisIndex, int targetIndex)
{
    std::vector<Edge*>* edges = (*arg)[thisIndex]->getEdges();
    for(Edge* e: *edges) {
        if(e->isIsolated()) {
            labelIsolatedEdge(e, targetIndex, (*arg)[targetIndex]->getGeometry());
            isolatedEdges.push_back(e);
        }
    }
}

/* private */
void
RelateComputer::labelIsolatedEdge(Edge* e, int targetIndex, const Geometry* target)
{
    // this won't work for GeometryCollections with both dim 2 and 1 geoms
    if(target->getDimension() > 0) {
        // since edge is not in boundary, may not need the full generality of PointLocator?
        // Possibly should use ptInArea locator instead?  We probably know here
        // that the edge does not touch the bdy of the target Geometry
        Location loc = ptLocator.locate(e->getCoordinate(), target);
        e->getLabel().setAllLocations(targetIndex, loc);
    }
    else {
        e->getLabel().setAllLocations(targetIndex, Location::EXTERIOR);
    }
    //System.out.println(e.getLabel());
}

/*private*/
void
RelateComputer::labelIsolatedNodes()
{
    for(const auto& it: nodes) {
        Node* n = it.second;
        const Label& label = n->getLabel();
        // isolated nodes should always have at least one geometry in their label
        assert(label.getGeometryCount() > 0); // node with empty label found
        if(n->isIsolated()) {
            if(label.isNull(0)) {
                labelIsolatedNode(n, 0);
            }
            else {
                labelIsolatedNode(n, 1);
            }
        }
    }
}

/* private */
void
RelateComputer::labelIsolatedNode(Node* n, int targetIndex)
{
    Location loc = ptLocator.locate(n->getCoordinate(),
                               (*arg)[targetIndex]->getGeometry());
    n->getLabel().setAllLocations(targetIndex, loc);
    //debugPrintln(n.getLabel());
}

} // namespace geos.operation.relate
} // namespace geos.operation
} // namespace geos

