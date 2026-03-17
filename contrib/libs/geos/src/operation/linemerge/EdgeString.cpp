/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/linemerge/EdgeString.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/operation/linemerge/EdgeString.h>
#include <geos/operation/linemerge/LineMergeEdge.h>
#include <geos/operation/linemerge/LineMergeDirectedEdge.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/LineString.h>
#include <geos/util.h>

#include <vector>
#include <cassert>

using namespace std;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace linemerge { // geos.operation.linemerge

/**
 * Constructs an EdgeString with the given factory used to convert
 * this EdgeString to a LineString
 */
EdgeString::EdgeString(const GeometryFactory* newFactory):
    factory(newFactory),
    directedEdges(),
    coordinates(nullptr)
{
}

/**
 * Adds a directed edge which is known to form part of this line.
 */
void
EdgeString::add(LineMergeDirectedEdge* directedEdge)
{
    directedEdges.push_back(directedEdge);
}

CoordinateSequence*
EdgeString::getCoordinates()
{
    if(coordinates == nullptr) {
        int forwardDirectedEdges = 0;
        int reverseDirectedEdges = 0;
        coordinates = new CoordinateArraySequence();
        for(std::size_t i = 0, e = directedEdges.size(); i < e; ++i) {
            LineMergeDirectedEdge* directedEdge = directedEdges[i];
            if(directedEdge->getEdgeDirection()) {
                forwardDirectedEdges++;
            }
            else {
                reverseDirectedEdges++;
            }

            LineMergeEdge* lme = detail::down_cast<LineMergeEdge*>(directedEdge->getEdge());

            coordinates->add(lme->getLine()->getCoordinatesRO(),
                             false,
                             directedEdge->getEdgeDirection());
        }
        if(reverseDirectedEdges > forwardDirectedEdges) {
            CoordinateSequence::reverse(coordinates);
        }
    }
    return coordinates;
}

/*
 * Converts this EdgeString into a new LineString.
 */
LineString*
EdgeString::toLineString()
{
    return factory->createLineString(getCoordinates());
}

} // namespace geos.operation.linemerge
} // namespace geos.operation
} // namespace geos
