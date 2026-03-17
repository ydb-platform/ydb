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
 * Last port: triangulate/quadedge/QuadEdgeLocator.java r524
 *
 **********************************************************************/

#ifndef GEOS_TRIANGULATE_QUADEDGE_QUADEDGELOCATOR_H
#define GEOS_TRIANGULATE_QUADEDGE_QUADEDGELOCATOR_H

namespace geos {
namespace triangulate { //geos.triangulate
namespace quadedge { //geos.triangulate.quadedge

class Vertex;
class QuadEdge;

/** \brief
 * An interface for classes which locate an edge in a {@link QuadEdgeSubdivision}
 * which either contains a given {@link Vertex} V or is an edge of a triangle
 * which contains V.
 *
 * Implementors may utilized different strategies for
 * optimizing locating containing edges/triangles.
 *
 * @author JTS: Martin Davis
 * @author Ben Campbell
 */
class QuadEdgeLocator {
public:
    virtual ~QuadEdgeLocator() = default;
    virtual QuadEdge* locate(const Vertex& v) = 0; //not implemented
};

} //namespace geos.triangulate.quadedge
} //namespace geos.triangulate
} //namespace goes

#endif //GEOS_TRIANGULATE_QUADEDGE_QUADEDGELOCATOR_H
