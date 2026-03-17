/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/operation/overlayng/PolygonBuilder.h>

#include <geos/operation/overlayng/OverlayLabel.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/util/Assert.h>
#include <geos/util/TopologyException.h>



namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


/*public*/
std::vector<std::unique_ptr<Polygon>>
PolygonBuilder::getPolygons()
{
    return computePolygons(shellList);
}

/*public*/
std::vector<OverlayEdgeRing*>
PolygonBuilder::getShellRings()
{
    return shellList;
}

/*private*/
std::vector<std::unique_ptr<Polygon>>
PolygonBuilder::computePolygons(std::vector<OverlayEdgeRing*> shells)
{
    std::vector<std::unique_ptr<Polygon>> resultPolyList;
    // add Polygons for all shells
    for (OverlayEdgeRing* er : shells) {
        std::unique_ptr<Polygon> poly = er->toPolygon(geometryFactory);
        resultPolyList.push_back(std::move(poly));
    }
    return resultPolyList;
}

/*private*/
void
PolygonBuilder::buildRings(std::vector<OverlayEdge*>& resultAreaEdges)
{
    linkResultAreaEdgesMax(resultAreaEdges);
    std::vector<std::unique_ptr<MaximalEdgeRing>> maxRings = buildMaximalRings(resultAreaEdges);
    buildMinimalRings(maxRings);
    placeFreeHoles(shellList, freeHoleList);
}

/*private*/
void
PolygonBuilder::linkResultAreaEdgesMax(std::vector<OverlayEdge*>& resultEdges)
{
    for (OverlayEdge* edge : resultEdges) {
        // TODO: find some way to skip nodes which are already linked
        MaximalEdgeRing::linkResultAreaMaxRingAtNode(edge);
    }
}

/*private*/
std::vector<std::unique_ptr<MaximalEdgeRing>>
PolygonBuilder::buildMaximalRings(std::vector<OverlayEdge*>& edges)
{
    std::vector<std::unique_ptr<MaximalEdgeRing>> edgeRings;
    for (OverlayEdge* e : edges) {
        if (e->isInResultArea() && e->getLabel()->isBoundaryEither()) {
            // if this edge has not yet been processed
            if (e->getEdgeRingMax() == nullptr) {
                // Add a MaximalEdgeRing to the vector
                edgeRings.emplace_back(new MaximalEdgeRing(e));
            }
        }
    }
    return edgeRings;
}

/*private*/
std::vector<OverlayEdgeRing*>
PolygonBuilder::storeMinimalRings(std::vector<std::unique_ptr<OverlayEdgeRing>>& minRings)
{
    std::vector<OverlayEdgeRing*> minRingPtrs;
    for (auto& mr: minRings) {
        minRingPtrs.push_back(mr.get());
        vecOER.push_back(std::move(mr));
    }
    return minRingPtrs;
}

/*private*/
void
PolygonBuilder::buildMinimalRings(std::vector<std::unique_ptr<MaximalEdgeRing>>& maxRings)
{
    for (auto& erMax : maxRings) {
        auto minRings = erMax->buildMinimalRings(geometryFactory);
        std::vector<OverlayEdgeRing*> minRingPtrs = storeMinimalRings(minRings);
        assignShellsAndHoles(minRingPtrs);
    }
}

/*private*/
void
PolygonBuilder::assignShellsAndHoles(std::vector<OverlayEdgeRing*>& minRings)
{
    /**
    * Two situations may occur:
    * - the rings are a shell and some holes
    * - rings are a set of holes
    * This code identifies the situation
    * and places the rings appropriately
    */
    OverlayEdgeRing* shell = findSingleShell(minRings);
    if (shell != nullptr) {
        assignHoles(shell, minRings);
        shellList.push_back(shell);
    }
    else {
        // all rings are holes; their shells will be found later
        freeHoleList.insert(freeHoleList.end(), minRings.begin(), minRings.end());
    }
}

/*private*/
OverlayEdgeRing*
PolygonBuilder::findSingleShell(std::vector<OverlayEdgeRing*>& edgeRings) const
{
    std::size_t shellCount = 0;
    OverlayEdgeRing* shell = nullptr;
    for (auto er : edgeRings) {
        if (! er->isHole()) {
            shell = er;
            shellCount++;
        }
    }
    util::Assert::isTrue(shellCount <= 1, "found two shells in EdgeRing list");
    return shell;
}

/*private*/
void
PolygonBuilder::assignHoles(OverlayEdgeRing* shell, std::vector<OverlayEdgeRing*>& edgeRings)
{
    for (auto er : edgeRings) {
        if (er->isHole()) {
            er->setShell(shell);
        }
    }
}

/*private*/
void
PolygonBuilder::placeFreeHoles(std::vector<OverlayEdgeRing*> shells, std::vector<OverlayEdgeRing*> freeHoles)
{
    // TODO: use a spatial index to improve performance
    for (OverlayEdgeRing* hole : freeHoles) {
        // only place this hole if it doesn't yet have a shell
        if (hole->getShell() == nullptr) {
            OverlayEdgeRing* shell = hole->findEdgeRingContaining(shells);
            // only when building a polygon-valid result
            if (isEnforcePolygonal && shell == nullptr) {
                throw util::TopologyException("unable to assign free hole to a shell", hole->getCoordinate());
            }
            hole->setShell(shell);
        }
    }
}




} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
