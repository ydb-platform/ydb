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

#include <geos/operation/overlayng/IntersectionPointBuilder.h>

#include <geos/operation/overlayng/OverlayEdge.h>
#include <geos/operation/overlayng/OverlayGraph.h>
#include <geos/operation/overlayng/OverlayLabel.h>
#include <geos/geom/GeometryFactory.h>



namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


/*public*/
std::vector<std::unique_ptr<Point>>
IntersectionPointBuilder::getPoints()
{
    addResultPoints();
    std::vector<std::unique_ptr<Point>> rsltPts;
    for (auto& pt : points) {
        rsltPts.emplace_back(pt.release());
    }
    return rsltPts;
}

/*private*/
void
IntersectionPointBuilder::addResultPoints()
{
    for (OverlayEdge* nodeEdge : graph->getNodeEdges()) {
        if (isResultPoint(nodeEdge)) {
            points.emplace_back(geometryFactory->createPoint(nodeEdge->getCoordinate()));
        }
    }
}

/*private*/
bool
IntersectionPointBuilder::isResultPoint(OverlayEdge* nodeEdge) const
{
    bool isEdgeOfA = false;
    bool isEdgeOfB = false;

    OverlayEdge* edge = nodeEdge;
    do {
        if (edge->isInResult()) {
            return false;
        }
        const OverlayLabel* label = edge->getLabel();
        isEdgeOfA |= isEdgeOf(label, 0);
        isEdgeOfB |= isEdgeOf(label, 1);
        edge = static_cast<OverlayEdge*>(edge->oNext());
    }
    while (edge != nodeEdge);
    bool isNodeInBoth = isEdgeOfA && isEdgeOfB;
    return isNodeInBoth;
}

/*private*/
bool
IntersectionPointBuilder::isEdgeOf(const OverlayLabel* label, int i) const
{
    if (!isAllowCollapseLines && label->isBoundaryCollapse()) {
        return false;
    }
    return label->isBoundary(i) || label->isLine(i);
}




} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
