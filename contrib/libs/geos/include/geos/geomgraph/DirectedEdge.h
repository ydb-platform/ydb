/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geomgraph/DirectedEdge.java r428 (JTS-1.12)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_DIRECTEDEDGE_H
#define GEOS_GEOMGRAPH_DIRECTEDEDGE_H

#include <geos/export.h>
#include <string>

#include <geos/geom/Location.h>
#include <geos/geomgraph/EdgeEnd.h> // for inheritance

#include <geos/inline.h>

// Forward declarations
namespace geos {
namespace geomgraph {
class Edge;
class EdgeRing;
}
}

namespace geos {
namespace geomgraph { // geos.geomgraph

/// A directed EdgeEnd
class GEOS_DLL DirectedEdge: public EdgeEnd {

public:

    /** \brief
     * Computes the factor for the change in depth when moving from
     * one location to another.
     * E.g. if crossing from the INTERIOR to the EXTERIOR the depth
     * decreases, so the factor is -1
     */
    static int depthFactor(geom::Location currLocation, geom::Location nextLocation);

    //DirectedEdge();
    //virtual ~DirectedEdge();

    DirectedEdge(Edge* newEdge, bool newIsForward);

    // this is no different from Base class, no need to override
    //Edge* getEdge();

    void setInResult(bool newIsInResult);

    bool isInResult();

    bool isVisited();

    void setVisited(bool newIsVisited);

    void setEdgeRing(EdgeRing* newEdgeRing);

    EdgeRing* getEdgeRing();

    void setMinEdgeRing(EdgeRing* newMinEdgeRing);

    EdgeRing* getMinEdgeRing();

    int getDepth(int position);

    void setDepth(int position, int newDepth);

    int getDepthDelta() const;

    /// Marks both DirectedEdges attached to a given Edge.
    ///
    /// This is used for edges corresponding to lines, which will only
    /// appear oriented in a single direction in the result.
    ///
    void setVisitedEdge(bool newIsVisited);


    /** \brief
     * Each Edge gives rise to a pair of symmetric DirectedEdges,
     * in opposite directions.
     *
     * @return the DirectedEdge for the same Edge but in the
     *         opposite direction
     */
    DirectedEdge* getSym();

    bool isForward();

    void setSym(DirectedEdge* de);

    DirectedEdge* getNext();

    void setNext(DirectedEdge* newNext);

    DirectedEdge* getNextMin();

    void setNextMin(DirectedEdge* newNextMin);

    /** \brief
     * Tells wheter this edge is a Line
     *
     * This edge is a line edge if
     * - at least one of the labels is a line label
     * - any labels which are not line labels have all Locations = EXTERIOR
     *
     */
    bool isLineEdge();

    /** \brief
     * Tells wheter this edge is an Area
     *
     * This is an interior Area edge if
     * - its label is an Area label for both Geometries
     * - and for each Geometry both sides are in the interior.
     *
     * @return true if this is an interior Area edge
     */
    bool isInteriorAreaEdge();

    /** \brief
     * Set both edge depths.
     *
     * One depth for a given side is provided.
     * The other is computed depending on the Location transition and the
     * depthDelta of the edge.
     */
    void setEdgeDepths(int position, int newDepth);

    std::string print() const override;

    std::string printEdge();

protected:

    bool isForwardVar;

private:

    bool isInResultVar;

    bool isVisitedVar;

    /// the symmetric edge
    DirectedEdge* sym;

    /// the next edge in the edge ring for the polygon containing this edge
    DirectedEdge* next;

    /// the next edge in the MinimalEdgeRing that contains this edge
    DirectedEdge* nextMin;

    /// the EdgeRing that this edge is part of
    EdgeRing* edgeRing;

    /// the MinimalEdgeRing that this edge is part of
    EdgeRing* minEdgeRing;

    /** \brief
     * The depth of each side (position) of this edge.
     * The 0 element of the array is never used.
     */
    int depth[3];

    /// Compute the label in the appropriate orientation for this DirEdge
    void computeDirectedLabel();
};

} // namespace geos.geomgraph
} // namespace geos

#ifdef GEOS_INLINE
# include "geos/geomgraph/DirectedEdge.inl"
#endif

#endif // ifndef GEOS_GEOMGRAPH_DIRECTEDEDGE_H
