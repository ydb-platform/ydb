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
 * Last port: triangulate/quadedge/TriangleVisitor.java r524
 *
 **********************************************************************/

#ifndef GEOS_TRIANGULATE_QUADEDGE_TRIANGLEVISITOR_H
#define GEOS_TRIANGULATE_QUADEDGE_TRIANGLEVISITOR_H

#include <geos/triangulate/quadedge/QuadEdge.h>

namespace geos {
namespace triangulate { //geos.triangulate
namespace quadedge { //geos.triangulate.quadedge

/** \brief
 * An interface for algorithms which process the triangles in a {@link QuadEdgeSubdivision}.
 *
 * @author JTS: Martin Davis
 * @author Benjamin Campbell
 */
class GEOS_DLL TriangleVisitor {
public:
    /**
     * Visits the {@link QuadEdge}s of a triangle.
     *
     * @param triEdges an array of the 3 quad edges in a triangle (in CCW order)
     */
    virtual void visit(QuadEdge* triEdges[3]) = 0;
    virtual ~TriangleVisitor() = default;
private:
} ;

} //namespace geos.triangulate.quadedge
} //namespace geos.triangulate
} //namespace goes

#endif // GEOS_TRIANGULATE_QUADEDGE_TRIANGLEVISITOR_H
