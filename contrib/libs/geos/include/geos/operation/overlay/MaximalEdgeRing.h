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
 * Last port: operation/overlay/MaximalEdgeRing.java rev. 1.15 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_OVERLAY_MAXIMALEDGERING_H
#define GEOS_OP_OVERLAY_MAXIMALEDGERING_H

#include <geos/export.h>

#include <vector>

#include <geos/geomgraph/EdgeRing.h> // for inheritance

// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
}
namespace geomgraph {
class DirectedEdge;
//class EdgeRing;
}
namespace operation {
namespace overlay {
class MinimalEdgeRing;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace overlay { // geos::operation::overlay

/** \brief
 * A ring of [DirectedEdges](@ref geomgraph::DirectedEdge) which may contain nodes of degree > 2.
 *
 * A MaximalEdgeRing may represent two different spatial entities:
 *
 * - a single polygon possibly containing inversions (if the ring is oriented CW)
 * - a single hole possibly containing exversions (if the ring is oriented CCW)
 *
 * If the MaximalEdgeRing represents a polygon,
 * the interior of the polygon is strongly connected.
 *
 * These are the form of rings used to define polygons under some spatial data models.
 * However, under the OGC SFS model, [MinimalEdgeRings](@ref MinimalEdgeRing) are required.
 * A MaximalEdgeRing can be converted to a list of MinimalEdgeRings using the
 * {@link #buildMinimalRings() } method.
 *
 * @see com.vividsolutions.jts.operation.overlay.MinimalEdgeRing
 */
class GEOS_DLL MaximalEdgeRing: public geomgraph::EdgeRing {

public:

    MaximalEdgeRing(geomgraph::DirectedEdge* start,
                    const geom::GeometryFactory* geometryFactory);
    // throw(const TopologyException &)

    ~MaximalEdgeRing() override = default;

    geomgraph::DirectedEdge* getNext(geomgraph::DirectedEdge* de) override;

    void setEdgeRing(geomgraph::DirectedEdge* de, geomgraph::EdgeRing* er) override;

    /// \brief
    /// This function returns a newly allocated vector of
    /// pointers to newly allocated MinimalEdgeRing objects.
    ///
    /// @deprecated pass the vector yourself instead
    ///
    std::vector<MinimalEdgeRing*>* buildMinimalRings();

    /// \brief
    /// This function pushes pointers to newly allocated  MinimalEdgeRing
    /// objects to the provided vector.
    ///
    void buildMinimalRings(std::vector<MinimalEdgeRing*>& minEdgeRings);
    void buildMinimalRings(std::vector<EdgeRing*>& minEdgeRings);

    /// \brief
    /// For all nodes in this EdgeRing,
    /// link the DirectedEdges at the node to form minimalEdgeRings
    ///
    void linkDirectedEdgesForMinimalEdgeRings();
};


} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#endif // ndef GEOS_OP_OVERLAY_MAXIMALEDGERING_H
