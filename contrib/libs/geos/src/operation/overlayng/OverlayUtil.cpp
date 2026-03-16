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

#include <geos/operation/overlayng/OverlayUtil.h>

#include <geos/operation/overlayng/InputGeometry.h>
#include <geos/operation/overlayng/OverlayGraph.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/operation/overlayng/OverlayNG.h>
#include <geos/operation/overlayng/RobustClipEnvelopeComputer.h>
#include <geos/util/Assert.h>



namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/*public static*/
bool
OverlayUtil::isFloating(const PrecisionModel* pm)
{
    if (pm == nullptr) return true;
    return pm->isFloating();
}

/*private static*/
double
OverlayUtil::safeExpandDistance(const Envelope* env, const PrecisionModel* pm)
{
    double envExpandDist;
    if (isFloating(pm)) {
        // if PM is FLOAT then there is no scale factor, so add 10%
        double minSize = std::min(env->getHeight(), env->getWidth());
        // heuristic to ensure zero-width envelopes don't cause total clipping
        if (minSize <= 0.0) {
            minSize = std::max(env->getHeight(), env->getWidth());
        }
        envExpandDist = SAFE_ENV_BUFFER_FACTOR * minSize;
    }
    else {
        // if PM is fixed, add a small multiple of the grid size
        double gridSize = 1.0 / pm->getScale();
        envExpandDist = SAFE_ENV_GRID_FACTOR * gridSize;
    }
    return envExpandDist;
}

/*private static*/
bool
OverlayUtil::safeEnv(const Envelope* env, const PrecisionModel* pm, Envelope& rsltEnvelope)
{
    double envExpandDist = safeExpandDistance(env, pm);
    rsltEnvelope = *env;
    rsltEnvelope.expandBy(envExpandDist);
    return true;
}

/*private static*/
bool
OverlayUtil::resultEnvelope(int opCode, const InputGeometry* inputGeom, const PrecisionModel* pm, Envelope& rsltEnvelope)
{
    switch (opCode) {
        case OverlayNG::INTERSECTION: {
            // use safe envelopes for intersection to ensure they contain rounded coordinates
            Envelope envA, envB;
            safeEnv(inputGeom->getEnvelope(0), pm, envA);
            safeEnv(inputGeom->getEnvelope(1), pm, envB);
            envA.intersection(envB, rsltEnvelope);
            return true;
        }
        case OverlayNG::DIFFERENCE: {
            safeEnv(inputGeom->getEnvelope(0), pm, rsltEnvelope);
            return true;
        }
    }
    // return false for UNION and SYMDIFFERENCE to indicate no clipping
    return false;
}

/*public static*/
bool
OverlayUtil::clippingEnvelope(int opCode, const InputGeometry* inputGeom, const PrecisionModel* pm, Envelope& rsltEnvelope)
{
    bool resultEnv = resultEnvelope(opCode, inputGeom, pm, rsltEnvelope);
    if (!resultEnv)
      return false;

    Envelope clipEnv = RobustClipEnvelopeComputer::getEnvelope(
        inputGeom->getGeometry(0),
        inputGeom->getGeometry(1),
        &rsltEnvelope);

    return safeEnv(&clipEnv, pm, rsltEnvelope);
}


/*public static*/
bool
OverlayUtil::isEmptyResult(int opCode, const Geometry* a, const Geometry* b, const PrecisionModel* pm)
{
    switch (opCode) {
    case OverlayNG::INTERSECTION: {
        if (isEnvDisjoint(a, b, pm))
            return true;
        break;
        }
    case OverlayNG::DIFFERENCE: {
        if (isEmpty(a))
            return true;
        break;
        }
    case OverlayNG::UNION:
    case OverlayNG::SYMDIFFERENCE: {
        if (isEmpty(a) && isEmpty(b))
            return true;
        break;
        }
    }
    return false;
}

/*private*/
bool
OverlayUtil::isEmpty(const Geometry* geom)
{
    return geom == nullptr || geom->isEmpty();
}

/*public static*/
bool
OverlayUtil::isEnvDisjoint(const Geometry* a, const Geometry* b, const PrecisionModel* pm)
{
    if (isEmpty(a) || isEmpty(b)) {
        return true;
    }
    if (isFloating(pm)) {
        return a->getEnvelopeInternal()->disjoint(b->getEnvelopeInternal());
    }
    return isDisjoint(a->getEnvelopeInternal(), b->getEnvelopeInternal(), pm);
}

/*private static*/
bool
OverlayUtil::isDisjoint(const Envelope* envA, const Envelope* envB, const PrecisionModel* pm)
{
    if (pm->makePrecise(envB->getMinX()) > pm->makePrecise(envA->getMaxX()))
        return true;
    if (pm->makePrecise(envB->getMaxX()) < pm->makePrecise(envA->getMinX()))
        return true;
    if (pm->makePrecise(envB->getMinY()) > pm->makePrecise(envA->getMaxY()))
        return true;
    if (pm->makePrecise(envB->getMaxY()) < pm->makePrecise(envA->getMinY()))
        return true;
    return false;
}

/*public static*/
std::unique_ptr<Geometry>
OverlayUtil::createEmptyResult(int dim, const GeometryFactory* geomFact)
{
    std::unique_ptr<Geometry> result(nullptr);
    switch (dim) {
    case 0:
        result = geomFact->createPoint();
        break;
    case 1:
        result = geomFact->createLineString();
        break;
    case 2:
        result = geomFact->createPolygon();
        break;
    case -1:
        result = geomFact->createGeometryCollection();
        break;
    default:
        util::Assert::shouldNeverReachHere("Unable to determine overlay result geometry dimension");
    }
    return result;
}

/*public static*/
int
OverlayUtil::resultDimension(int opCode, int dim0, int dim1)
{
    int resultDimension = -1;
    switch (opCode) {
    case OverlayNG::INTERSECTION:
        resultDimension = std::min(dim0, dim1);
        break;
    case OverlayNG::UNION:
        resultDimension = std::max(dim0, dim1);
        break;
    case OverlayNG::DIFFERENCE:
        resultDimension = dim0;
        break;
    case OverlayNG::SYMDIFFERENCE:
        /**
        * This result is chosen because
        * <pre>
        * SymDiff = Union( Diff(A, B), Diff(B, A) )
        * </pre>
        * and Union has the dimension of the highest-dimension argument.
        */
        resultDimension = std::max(dim0, dim1);
        break;
    }
    return resultDimension;
}


/*public static*/
std::unique_ptr<Geometry>
OverlayUtil::createResultGeometry(
    std::vector<std::unique_ptr<Polygon>>& resultPolyList,
    std::vector<std::unique_ptr<LineString>>& resultLineList,
    std::vector<std::unique_ptr<Point>>& resultPointList,
    const GeometryFactory* geometryFactory)
{
    std::vector<std::unique_ptr<Geometry>> geomList;

    // TODO: for mixed dimension, return collection of Multigeom for each dimension (breaking change)

    // element geometries of the result are always in the order A,L,P
    if (resultPolyList.size() > 0)
        moveGeometry(resultPolyList, geomList);
    if (resultLineList.size() > 0)
        moveGeometry(resultLineList, geomList);
    if (resultPointList.size() > 0)
        moveGeometry(resultPointList, geomList);

    // build the most specific geometry possible
    // TODO: perhaps do this internally to give more control?
    return geometryFactory->buildGeometry(std::move(geomList));
}

/*public static*/
std::unique_ptr<Geometry>
OverlayUtil::toLines(OverlayGraph* graph, bool isOutputEdges, const GeometryFactory* geomFact)
{
    std::vector<std::unique_ptr<LineString>> lines;
    std::vector<OverlayEdge*>& edges = graph->getEdges();
    for (OverlayEdge* edge : edges) {
      bool includeEdge = isOutputEdges || edge->isInResultArea();
      if (! includeEdge) continue;

      std::unique_ptr<CoordinateSequence> pts = edge->getCoordinatesOriented();
      std::unique_ptr<LineString> line = geomFact->createLineString(std::move(pts));
      // line->setUserData(labelForResult(edge));
      lines.push_back(std::move(line));
    }
    return geomFact->buildGeometry(std::move(lines));
}

/*public static*/
bool
OverlayUtil::round(const Point* pt, const PrecisionModel* pm, Coordinate& rsltCoord)
{
    if (pt->isEmpty()) return false;
    const Coordinate* p = pt->getCoordinate();
    rsltCoord = *p;
    if (! isFloating(pm)) {
        pm->makePrecise(rsltCoord);
    }
    return true;
}



} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
