/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last Port: operation/overlayng/OverlayNG.java 4c88fea52
 *
 **********************************************************************/

#include <geos/operation/overlayng/OverlayNG.h>

#include <geos/operation/overlayng/Edge.h>
#include <geos/operation/overlayng/EdgeNodingBuilder.h>
#include <geos/operation/overlayng/ElevationModel.h>
#include <geos/operation/overlayng/InputGeometry.h>
#include <geos/operation/overlayng/IntersectionPointBuilder.h>
#include <geos/operation/overlayng/LineBuilder.h>
#include <geos/operation/overlayng/OverlayEdge.h>
#include <geos/operation/overlayng/OverlayLabeller.h>
#include <geos/operation/overlayng/OverlayMixedPoints.h>
#include <geos/operation/overlayng/OverlayPoints.h>
#include <geos/operation/overlayng/OverlayUtil.h>
#include <geos/operation/overlayng/PolygonBuilder.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Location.h>
#include <geos/geom/Geometry.h>
#include <geos/util/Interrupt.h>

#include <algorithm>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#if GEOS_DEBUG
# include <iostream>
# include <geos/io/WKTWriter.h>
#endif



namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;


/*public static*/
bool
OverlayNG::isResultOfOpPoint(const OverlayLabel* label, int opCode)
{
    Location loc0 = label->getLocation(0);
    Location loc1 = label->getLocation(1);
    return isResultOfOp(opCode, loc0, loc1);
}

/*public static*/
bool
OverlayNG::isResultOfOp(int overlayOpCode, Location loc0, Location loc1)
{
    if (loc0 == Location::BOUNDARY) loc0 = Location::INTERIOR;
    if (loc1 == Location::BOUNDARY) loc1 = Location::INTERIOR;
    switch (overlayOpCode) {
        case INTERSECTION:
            return loc0 == Location::INTERIOR
                && loc1 == Location::INTERIOR;
        case UNION:
            return loc0 == Location::INTERIOR
                || loc1 == Location::INTERIOR;
        case DIFFERENCE:
            return loc0 == Location::INTERIOR
                && loc1 != Location::INTERIOR;
        case SYMDIFFERENCE:
            return   (loc0 == Location::INTERIOR && loc1 != Location::INTERIOR)
                  || (loc0 != Location::INTERIOR && loc1 == Location::INTERIOR);
    }
    return false;
}


/*public static*/
std::unique_ptr<Geometry>
OverlayNG::overlay(const Geometry* geom0, const Geometry* geom1,
        int opCode, const PrecisionModel* pm)
{
    OverlayNG ov(geom0, geom1, pm, opCode);
    return ov.getResult();
}

/*public static*/
std::unique_ptr<Geometry>
OverlayNG::overlay(const Geometry* geom0, const Geometry* geom1,
        int opCode, const PrecisionModel* pm, noding::Noder* noder)
{
    OverlayNG ov(geom0, geom1, pm, opCode);
    ov.setNoder(noder);
    return ov.getResult();
}

/*public static*/
std::unique_ptr<Geometry>
OverlayNG::overlay(const Geometry* geom0, const Geometry* geom1,
        int opCode, noding::Noder* noder)
{
    OverlayNG ov(geom0, geom1, static_cast<PrecisionModel*>(nullptr), opCode);
    ov.setNoder(noder);
    return ov.getResult();
}

/*public static*/
std::unique_ptr<Geometry>
OverlayNG::overlay(const Geometry* geom0, const Geometry* geom1, int opCode)
{
    OverlayNG ov(geom0, geom1, opCode);
    return ov.getResult();
}

/*public static*/
std::unique_ptr<Geometry>
OverlayNG::geomunion(const Geometry* geom, const PrecisionModel* pm)
{
    OverlayNG ov(geom, pm);
    return ov.getResult();
}

/*public static*/
std::unique_ptr<Geometry>
OverlayNG::geomunion(const Geometry* geom, const PrecisionModel* pm, noding::Noder* noder)
{
    OverlayNG ov(geom, pm);
    ov.setNoder(noder);
    return ov.getResult();
}

/*public*/
std::unique_ptr<Geometry>
OverlayNG::getResult()
{
    const Geometry *ig0 = inputGeom.getGeometry(0);
    const Geometry *ig1 = inputGeom.getGeometry(1);

    if ( OverlayUtil::isEmptyResult(opCode, ig0, ig1, pm) )
    {
        return createEmptyResult();
    }

    /**
     * The elevation model is only computed if the input geometries have Z values.
     */
    std::unique_ptr<ElevationModel> elevModel;
    if ( ig1 ) {
        elevModel = ElevationModel::create(*ig0, *ig1);
    } else {
        elevModel = ElevationModel::create(*ig0);
    }
    std::unique_ptr<Geometry> result;

    if (inputGeom.isAllPoints()) {
        // handle Point-Point inputs
        result = OverlayPoints::overlay(opCode, ig0, ig1, pm);
    }
    else if (! inputGeom.isSingle() &&  inputGeom.hasPoints()) {
        // handle Point-nonPoint inputs
        result = OverlayMixedPoints::overlay(opCode, ig0, ig1, pm);
    }
    else {
        // handle case where both inputs are formed of edges (Lines and Polygons)
        result = computeEdgeOverlay();
    }

#if GEOS_DEBUG
    io::WKTWriter w;
    w.setOutputDimension(3);
    w.setTrim(true);

    std::cout << "Before populatingZ: " << w.write(result.get()) << std::endl;
#endif

    /**
     * This is a no-op if the elevation model was not computed due to
     * Z not present
     */
    elevModel->populateZ(*result);

#if GEOS_DEBUG
    std::cout << " After populatingZ: " << w.write(result.get()) << std::endl;
#endif

    return result;
}


/*private*/
std::unique_ptr<Geometry>
OverlayNG::computeEdgeOverlay()
{
    /**
     * Node the edges, using whatever noder is being used
     * Formerly in nodeEdges())
     */
    EdgeNodingBuilder nodingBuilder(pm, noder);
    // clipEnv not always used, but needs to remain in scope
    // as long as nodingBuilder when it is.
    Envelope clipEnv;

    GEOS_CHECK_FOR_INTERRUPTS();

    if (isOptimized) {
        bool gotClipEnv = OverlayUtil::clippingEnvelope(opCode, &inputGeom, pm, clipEnv);
        if (gotClipEnv) {
            nodingBuilder.setClipEnvelope(&clipEnv);
        }
    }

    std::vector<Edge*> edges = nodingBuilder.build(
        inputGeom.getGeometry(0),
        inputGeom.getGeometry(1));

    GEOS_CHECK_FOR_INTERRUPTS();

    /**
     * Record if an input geometry has collapsed.
     * This is used to avoid trying to locate disconnected edges
     * against a geometry which has collapsed completely.
     */
    inputGeom.setCollapsed(0, ! nodingBuilder.hasEdgesFor(0));
    inputGeom.setCollapsed(1, ! nodingBuilder.hasEdgesFor(1));

    /**
    * Inlined buildGraph() method here for memory purposes, so the
    * Edge* list allocated in the EdgeNodingBuilder survives
    * long enough to be copied into the OverlayGraph
    */
    // Sort the edges first, for comparison with JTS results
    // std::sort(edges.begin(), edges.end(), EdgeComparator);
    OverlayGraph graph;
    for (Edge* e : edges) {
        // Write out edge graph as hex for examination
        // std::cout << *e << std::endl;
        graph.addEdge(e);
    }

    if (isOutputNodedEdges) {
        return OverlayUtil::toLines(&graph, isOutputEdges, geomFact);
    }

    GEOS_CHECK_FOR_INTERRUPTS();
    labelGraph(&graph);

    // std::cout << std::endl << graph << std::endl;

    if (isOutputEdges || isOutputResultEdges) {
        return OverlayUtil::toLines(&graph, isOutputEdges, geomFact);
    }

    GEOS_CHECK_FOR_INTERRUPTS();
    return extractResult(opCode, &graph);
}

/*private*/
void
OverlayNG::labelGraph(OverlayGraph* graph)
{
    OverlayLabeller labeller(graph, &inputGeom);
    labeller.computeLabelling();
    labeller.markResultAreaEdges(opCode);
    labeller.unmarkDuplicateEdgesFromResultArea();
}


/*private*/
std::unique_ptr<Geometry>
OverlayNG::extractResult(int p_opCode, OverlayGraph* graph)
{

#if GEOS_DEBUG
    std::cerr << "OverlayNG::extractResult: graph: " << *graph << std::endl;
#endif

    bool isAllowMixedIntResult = ! isStrictMode;

    //--- Build polygons
    std::vector<OverlayEdge*> resultAreaEdges = graph->getResultAreaEdges();
    PolygonBuilder polyBuilder(resultAreaEdges, geomFact);
    std::vector<std::unique_ptr<Polygon>> resultPolyList = polyBuilder.getPolygons();
    bool hasResultAreaComponents = resultPolyList.size() > 0;

    std::vector<std::unique_ptr<LineString>> resultLineList;
    std::vector<std::unique_ptr<Point>> resultPointList;

    GEOS_CHECK_FOR_INTERRUPTS();
    if (!isAreaResultOnly) {
        //--- Build lines
        bool allowResultLines = !hasResultAreaComponents ||
                                isAllowMixedIntResult ||
                                opCode == SYMDIFFERENCE ||
                                opCode == UNION;

        if (allowResultLines) {
            LineBuilder lineBuilder(&inputGeom, graph, hasResultAreaComponents, p_opCode, geomFact);
            lineBuilder.setStrictMode(isStrictMode);
            resultLineList = lineBuilder.getLines();
        }
        /**
         * Operations with point inputs are handled elsewhere.
         * Only an intersection op can produce point results
         * from non-point inputs.
         */
        bool hasResultComponents = hasResultAreaComponents || resultLineList.size() > 0;
        bool allowResultPoints = ! hasResultComponents || isAllowMixedIntResult;
        if (opCode == INTERSECTION && allowResultPoints) {
            IntersectionPointBuilder pointBuilder(graph, geomFact);
            pointBuilder.setStrictMode(isStrictMode);
            resultPointList = pointBuilder.getPoints();
        }
    }

    if (resultPolyList.size() == 0 &&
        resultLineList.size() == 0 &&
        resultPointList.size() == 0)
    {
        return createEmptyResult();
    }

    std::unique_ptr<Geometry> resultGeom = OverlayUtil::createResultGeometry(resultPolyList, resultLineList, resultPointList, geomFact);
    return resultGeom;
}

/*private*/
std::unique_ptr<Geometry>
OverlayNG::createEmptyResult()
{
    return OverlayUtil::createEmptyResult(
                OverlayUtil::resultDimension(opCode,
                    inputGeom.getDimension(0),
                    inputGeom.getDimension(1)),
                geomFact);
}




} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
