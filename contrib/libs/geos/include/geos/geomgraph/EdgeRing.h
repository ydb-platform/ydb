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
 * Last port: geomgraph/EdgeRing.java r428 (JTS-1.12+)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_EDGERING_H
#define GEOS_GEOMGRAPH_EDGERING_H

#include <geos/export.h>
#include <geos/geomgraph/Label.h> // for composition
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/LinearRing.h>

#include <geos/inline.h>

#include <cassert> // for testInvariant
#include <iosfwd> // for operator<<
#include <memory>
#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class Polygon;
class Coordinate;
}
namespace geomgraph {
class DirectedEdge;
//class Label;
class Edge;
}
}

namespace geos {
namespace geomgraph { // geos.geomgraph

/** EdgeRing */
class GEOS_DLL EdgeRing {

public:
    friend std::ostream& operator<< (std::ostream& os, const EdgeRing& er);

    EdgeRing(DirectedEdge* newStart,
             const geom::GeometryFactory* newGeometryFactory);

    virtual ~EdgeRing() = default;

    bool isIsolated();

    bool isHole();

    /**
     * Return a pointer to the LinearRing owned by
     * this object. Make a copy if you need it beyond
     * this objects's lifetime.
     */
    geom::LinearRing* getLinearRing();

    Label& getLabel();

    bool isShell();

    EdgeRing* getShell();

    void setShell(EdgeRing* newShell);

    void addHole(EdgeRing* edgeRing);

    /**
     * Return a Polygon copying coordinates from this
     * EdgeRing and its holes.
     */
    std::unique_ptr<geom::Polygon> toPolygon(const geom::GeometryFactory* geometryFactory);

    /**
     * Compute a LinearRing from the point list previously collected.
     * Test if the ring is a hole (i.e. if it is CCW) and set the hole
     * flag accordingly.
     */
    void computeRing();

    virtual DirectedEdge* getNext(DirectedEdge* de) = 0;

    virtual void setEdgeRing(DirectedEdge* de, EdgeRing* er) = 0;

    /**
     * Returns the list of DirectedEdges that make up this EdgeRing
     */
    std::vector<DirectedEdge*>& getEdges();

    int getMaxNodeDegree();

    void setInResult();

    /**
     * This method will use the computed ring.
     * It will also check any holes, if they have been assigned.
     */
    bool containsPoint(const geom::Coordinate& p);

    void
    testInvariant() const
    {
        // pts are never NULL
        // assert(pts);

#ifndef NDEBUG
        // If this is not an hole, check that
        // each hole is not null and
        // has 'this' as it's shell
        if(! shell) {
            for(const auto& hole : holes) {
                assert(hole);
                assert(hole->getShell() == this);
            }
        }
#endif // ndef NDEBUG
    }

protected:

    DirectedEdge* startDe; // the directed edge which starts the list of edges for this EdgeRing

    const geom::GeometryFactory* geometryFactory;

    /// @throws util::TopologyException
    void computePoints(DirectedEdge* newStart);

    void mergeLabel(const Label& deLabel);

    /** \brief
     * Merge the RHS label from a DirectedEdge into the label for
     * this EdgeRing.
     *
     * The DirectedEdge label may be null.
     * This is acceptable - it results from a node which is NOT
     * an intersection node between the Geometries
     * (e.g. the end node of a LinearRing).
     * In this case the DirectedEdge label does not contribute any
     * information to the overall labelling, and is
     * simply skipped.
     */
    void mergeLabel(const Label& deLabel, int geomIndex);

    void addPoints(Edge* edge, bool isForward, bool isFirstEdge);

    /// a list of EdgeRings which are holes in this EdgeRing
    std::vector<std::unique_ptr<EdgeRing>> holes;

private:

    int maxNodeDegree;

    /// the DirectedEdges making up this EdgeRing
    std::vector<DirectedEdge*> edges;

    std::vector<geom::Coordinate> pts;

    // label stores the locations of each geometry on the
    // face surrounded by this ring
    Label label;

    std::unique_ptr<geom::LinearRing> ring;  // the ring created for this EdgeRing

    bool isHoleVar;

    /// if non-null, the ring is a hole and this EdgeRing is its containing shell
    EdgeRing* shell;

    void computeMaxNodeDegree();

};

std::ostream& operator<< (std::ostream& os, const EdgeRing& er);

} // namespace geos.geomgraph
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ifndef GEOS_GEOMGRAPH_EDGERING_H

