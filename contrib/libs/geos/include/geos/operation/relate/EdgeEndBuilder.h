/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/relate/EdgeEndBuilder.java rev. 1.12 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_RELATE_EDGEENDBUILDER_H
#define GEOS_OP_RELATE_EDGEENDBUILDER_H

#include <geos/export.h>

#include <vector>

// Forward declarations
namespace geos {
namespace geom {
class IntersectionMatrix;
class Coordinate;
}
namespace geomgraph {
class Edge;
class EdgeIntersection;
class EdgeEnd;
}
}


namespace geos {
namespace operation { // geos::operation
namespace relate { // geos::operation::relate

/** \brief
 * Computes the geomgraph::EdgeEnd objects which arise
 * from a noded geomgraph::Edge.
 */
class GEOS_DLL EdgeEndBuilder {
public:
    EdgeEndBuilder() {}

    std::vector<geomgraph::EdgeEnd*> computeEdgeEnds(std::vector<geomgraph::Edge*>* edges);
    void computeEdgeEnds(geomgraph::Edge* edge, std::vector<geomgraph::EdgeEnd*>* l);

protected:

    void createEdgeEndForPrev(geomgraph::Edge* edge,
                              std::vector<geomgraph::EdgeEnd*>* l,
                              const geomgraph::EdgeIntersection* eiCurr,
                              const geomgraph::EdgeIntersection* eiPrev);

    void createEdgeEndForNext(geomgraph::Edge* edge,
                              std::vector<geomgraph::EdgeEnd*>* l,
                              const geomgraph::EdgeIntersection* eiCurr,
                              const geomgraph::EdgeIntersection* eiNext);
};

} // namespace geos:operation:relate
} // namespace geos:operation
} // namespace geos

#endif // GEOS_OP_RELATE_EDGEENDBUILDER_H
