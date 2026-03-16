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

#pragma once

#include <geos/export.h>

#include <geos/geom/Polygon.h>
#include <geos/operation/overlayng/OverlayEdge.h>
#include <geos/operation/overlayng/OverlayEdgeRing.h>
#include <geos/operation/overlayng/MaximalEdgeRing.h>

#include <vector>


// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class Geometry;
class Polygon;
}
namespace operation {
namespace overlayng {
class Edge;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

class GEOS_DLL PolygonBuilder {

private:

    // Members
    const geom::GeometryFactory* geometryFactory;
    std::vector<OverlayEdgeRing*> shellList;
    std::vector<OverlayEdgeRing*> freeHoleList;
    bool isEnforcePolygonal;

    // Storage
    std::vector<std::unique_ptr<OverlayEdgeRing>> vecOER;

    std::vector<std::unique_ptr<geom::Polygon>> computePolygons(std::vector<OverlayEdgeRing*> shellList);

    void buildRings(std::vector<OverlayEdge*>& resultAreaEdges);

    void linkResultAreaEdgesMax(std::vector<OverlayEdge*>& resultEdges);

    /**
    * For all OverlayEdge*s in result, form them into MaximalEdgeRings
    */
    std::vector<std::unique_ptr<MaximalEdgeRing>>
        buildMaximalRings(std::vector<OverlayEdge*>& edges);

    /**
    * The lifespan of the OverlayEdgeRings is tieds to the lifespan
    * of the PolygonBuilder, so we hold them on the PolygonBuilder
    * and use bare pointers for managing the relationships
    */
    std::vector<OverlayEdgeRing*> storeMinimalRings(std::vector<std::unique_ptr<OverlayEdgeRing>>& minRings);

    void buildMinimalRings(std::vector<std::unique_ptr<MaximalEdgeRing>>& maxRings);

    void assignShellsAndHoles(std::vector<OverlayEdgeRing*>& minRings);

    /**
    * Finds the single shell, if any, out of
    * a list of minimal rings derived from a maximal ring.
    * The other possibility is that they are a set of (connected) holes,
    * in which case no shell will be found.
    *
    * @return the shell ring, if there is one
    * or null, if all rings are holes
    */
    OverlayEdgeRing* findSingleShell(std::vector<OverlayEdgeRing*>& edgeRings) const;

    /**
    * For the set of minimal rings comprising a maximal ring,
    * assigns the holes to the shell known to contain them.
    * Assigning the holes directly to the shell serves two purposes:
    *
    *  - it is faster than using a point-in-polygon check later on.
    *  - it ensures correctness, since if the PIP test was used the point
    * chosen might lie on the shell, which might return an incorrect result from the
    * PIP test
    */
    void assignHoles(OverlayEdgeRing* shell, std::vector<OverlayEdgeRing*>& edgeRings);


    /**
    * Place holes have not yet been assigned to a shell.
    * These "free" holes should
    * all be <b>properly</b> contained in their parent shells, so it is safe to use the
    * <code>findEdgeRingContaining</code> method.
    * (This is the case because any holes which are NOT
    * properly contained (i.e. are connected to their
    * parent shell) would have formed part of a MaximalEdgeRing
    * and been handled in a previous step).
    *
    * @throws TopologyException if a hole cannot be assigned to a shell
    */
    void placeFreeHoles(std::vector<OverlayEdgeRing*> shellList, std::vector<OverlayEdgeRing*> freeHoleList);



public:

    PolygonBuilder(std::vector<OverlayEdge*>& resultAreaEdges, const geom::GeometryFactory* geomFact)
        : geometryFactory(geomFact)
        , isEnforcePolygonal(true)
    {
        buildRings(resultAreaEdges);
    }

    PolygonBuilder(std::vector<OverlayEdge*>& resultAreaEdges, const geom::GeometryFactory* geomFact, bool p_isEnforcePolygonal)
        : geometryFactory(geomFact)
        , isEnforcePolygonal(p_isEnforcePolygonal)
    {
        buildRings(resultAreaEdges);
    }

    PolygonBuilder(const PolygonBuilder&) = delete;
    PolygonBuilder& operator=(const PolygonBuilder&) = delete;

    // Methods
    std::vector<std::unique_ptr<geom::Polygon>> getPolygons();
    std::vector<OverlayEdgeRing*> getShellRings();


};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

