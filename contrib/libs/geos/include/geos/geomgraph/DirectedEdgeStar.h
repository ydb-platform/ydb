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
 * Last port: geomgraph/DirectedEdgeStar.java r428 (JTS-1.12+)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_DIRECTEDEDGEENDSTAR_H
#define GEOS_GEOMGRAPH_DIRECTEDEDGEENDSTAR_H

#include <geos/export.h>
#include <set>
#include <string>
#include <vector>

#include <geos/geomgraph/EdgeEndStar.h>  // for inheritance
#include <geos/geomgraph/Label.h>  // for private member
#include <geos/geom/Coordinate.h>  // for p0,p1

#include <geos/inline.h>

// Forward declarations
namespace geos {
namespace geomgraph {
class DirectedEdge;
class EdgeRing;
}
}

namespace geos {
namespace geomgraph { // geos.geomgraph

/**
 * \brief
 * A DirectedEdgeStar is an ordered list of **outgoing** DirectedEdges around a node.
 *
 * It supports labelling the edges as well as linking the edges to form both
 * MaximalEdgeRings and MinimalEdgeRings.
 *
 */
class GEOS_DLL DirectedEdgeStar: public EdgeEndStar {

public:

    DirectedEdgeStar()
        :
        EdgeEndStar(),
        label(),
        resultAreaEdgesComputed(false)
    {}

    ~DirectedEdgeStar() override = default;

    /// Insert a directed edge in the list
    void insert(EdgeEnd* ee) override;

    Label&
    getLabel()
    {
        return label;
    }

    int getOutgoingDegree();

    int getOutgoingDegree(EdgeRing* er);

    DirectedEdge* getRightmostEdge();

    /** \brief
     * Compute the labelling for all dirEdges in this star, as well as the overall labelling
     */
    void computeLabelling(std::vector<GeometryGraph*>* geom) override; // throw(TopologyException *);

    /** \brief
     * For each dirEdge in the star, merge the label from the sym dirEdge into the label
     */
    void mergeSymLabels();

    /// \brief Update incomplete dirEdge labels from the labelling for the node
    void updateLabelling(const Label& nodeLabel);


    /** \brief
     * Traverse the star of DirectedEdges, linking the included edges together.
     *
     * To link two dirEdges, the `next` pointer for an incoming dirEdge
     * is set to the next outgoing edge.
     *
     * DirEdges are only linked if:
     *
     * - they belong to an area (i.e. they have sides)
     * - they are marked as being in the result
     *
     * Edges are linked in CCW order (the order they are stored). This means
     * that rings have their face on the Right (in other words, the topological
     * location of the face is given by the RHS label of the DirectedEdge)
     *
     * PRECONDITION: No pair of dirEdges are both marked as being in the result
     */
    void linkResultDirectedEdges(); // throw(TopologyException *);

    void linkMinimalDirectedEdges(EdgeRing* er);

    void linkAllDirectedEdges();

    /** \brief
     * Traverse the star of edges, maintaing the current location in the result
     * area at this node (if any).
     *
     * If any L edges are found in the interior of the result, mark them as covered.
     */
    void findCoveredLineEdges();

    /** \brief
     * Compute the DirectedEdge depths for a subsequence of the edge array.
     */
    void computeDepths(DirectedEdge* de);

    std::string print() const override;

private:

    /**
     * A list of all outgoing edges in the result, in CCW order
     */
    std::vector<DirectedEdge*> resultAreaEdgeList;

    Label label;

    bool resultAreaEdgesComputed;

    /// \brief
    /// Returned vector is owned by DirectedEdgeStar object, but
    /// lazily created
    const std::vector<DirectedEdge*>& getResultAreaEdges();


    /// States for linResultDirectedEdges
    enum {
        SCANNING_FOR_INCOMING = 1,
        LINKING_TO_OUTGOING
    };

    int computeDepths(EdgeEndStar::iterator startIt,
                      EdgeEndStar::iterator endIt, int startDepth);
};


} // namespace geos.geomgraph
} // namespace geos

//#ifdef GEOS_INLINE
//# include "geos/geomgraph/DirectedEdgeEndStar.inl"
//#endif

#endif // ifndef GEOS_GEOMGRAPH_DIRECTEDEDGEENDSTAR_H

