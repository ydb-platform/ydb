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

#ifndef GEOS_TRIANGULATE_QUADEDGE_LASTFOUNDQUADEDGELOCATOR_H
#define GEOS_TRIANGULATE_QUADEDGE_LASTFOUNDQUADEDGELOCATOR_H

#include <geos/triangulate/quadedge/QuadEdge.h>
#include <geos/triangulate/quadedge/QuadEdgeLocator.h>

namespace geos {
namespace triangulate { //geos.triangulate
namespace quadedge { //geos.triangulate.quadedge

//fwd declarations
class QuadEdgeSubdivision;

/** \brief
 * Locates {@link QuadEdge}s in a {@link QuadEdgeSubdivision}, optimizing the
 * search by starting in the locality of the last edge found.
 *
 * @author JTS: Martin Davis
 * @author Benjamin Campbell
 */
class LastFoundQuadEdgeLocator : public QuadEdgeLocator {
private:
    QuadEdgeSubdivision* subdiv;
    QuadEdge*			lastEdge;

public:
    LastFoundQuadEdgeLocator(QuadEdgeSubdivision* subdiv);

private:
    virtual void init();

    virtual QuadEdge* findEdge();

public:
    /**
     * Locates an edge e, such that either v is on e, or e is an edge of a triangle containing v.
     * The search starts from the last located edge amd proceeds on the general direction of v.
     * @return The caller _does not_ take ownership of the returned object.
     */
    QuadEdge* locate(const Vertex& v) override;
};

} //namespace geos.triangulate.quadedge
} //namespace geos.triangulate
} //namespace goes

#endif //  GEOS_TRIANGULATE_QUADEDGE_LASTFOUNDQUADEDGELOCATOR_H

