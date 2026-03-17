/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Excensus LLC.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: triangulate/quadedge/LastFoundQuadEdgeLocator.java r524
 *
 **********************************************************************/

#include <geos/triangulate/quadedge/LastFoundQuadEdgeLocator.h>
#include <geos/triangulate/quadedge/QuadEdgeSubdivision.h>

namespace geos {
namespace triangulate { //geos.triangulate
namespace quadedge { //geos.triangulate.quadedge

LastFoundQuadEdgeLocator::LastFoundQuadEdgeLocator(QuadEdgeSubdivision* p_subdiv) :
    subdiv(p_subdiv), lastEdge(nullptr)
{
}

void
LastFoundQuadEdgeLocator::init()
{
    lastEdge = findEdge();
}

QuadEdge*
LastFoundQuadEdgeLocator::findEdge()
{
    // assume there is an edge
    return &(subdiv->getEdges()[0].base());
}

QuadEdge*
LastFoundQuadEdgeLocator::locate(const Vertex& v)
{
    if(!lastEdge || !lastEdge->isLive()) {
        init();
    }

    QuadEdge* e = subdiv->locateFromEdge(v, *lastEdge);
    lastEdge = e;
    return e;
}

} //namespace geos.triangulate.quadedge
} //namespace geos.triangulate
} //namespace goes

