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
#include <string>
#include <cassert>
#include <geos/geom/Coordinate.h>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace edgegraph { // geos.edgegraph

/**
 * Represents a directed component of an edge in an {@link EdgeGraph}.
 * HalfEdges link vertices whose locations are defined by {@link geom::Coordinate}s.
 * HalfEdges start at an origin vertex,
 * and terminate at a destination vertex.
 * HalfEdges always occur in symmetric pairs, with the {@link sym()} method
 * giving access to the oppositely-oriented component.
 * HalfEdges and the methods on them form an edge algebra,
 * which can be used to traverse and query the topology
 * of the graph formed by the edges.
 *
 * To support graphs where the edges are sequences of coordinates
 * each edge may also have a direction point supplied.
 * This is used to determine the ordering
 * of the edges around the origin.
 * HalfEdges with the same origin are ordered
 * so that the ring of edges formed by them is oriented CCW.
 *
 * By design HalfEdges carry minimal information
 * about the actual usage of the graph they represent.
 * They can be subclassed to carry more information if required.
 *
 * HalfEdges form a complete and consistent data structure by themselves,
 * but an {@link EdgeGraph} is useful to allow retrieving edges
 * by vertex and edge location, as well as ensuring
 * edges are created and linked appropriately.
 *
 * @author Martin Davis
 *
 */
class GEOS_DLL HalfEdge {

private:

    /* members */
    geom::Coordinate m_orig;
    HalfEdge* m_sym;
    HalfEdge* m_next;


    /**
    * Sets the symmetric (opposite) edge to this edge.
    *
    * @param e the sym edge to set
    */
    void setSym(HalfEdge* e) { m_sym = e; };

    /**
    * Finds the insertion edge for a edge
    * being added to this origin,
    * ensuring that the star of edges
    * around the origin remains fully CCW.
    *
    * @param eAdd the edge being added
    * @return the edge to insert after
    */
    HalfEdge* insertionEdge(HalfEdge* eAdd);

    /**
    * Insert an edge with the same origin after this one.
    * Assumes that the inserted edge is in the correct
    * position around the ring.
    *
    * @param e the edge to insert (with same origin)
    */
    void insertAfter(HalfEdge* e);

    /**
    * Finds the lowest edge around the origin,
    * using the standard edge ordering.
    *
    * @return the lowest edge around the origin
    */
    const HalfEdge* findLowest() const;

protected:

    /**
    * Gets the direction point of this edge.
    * In the base case this is the dest coordinate
    * of the edge.
    * Subclasses may override to
    * allow a HalfEdge to represent an edge with more than two coordinates.
    *
    * @return the direction point for the edge
    */
    virtual const geom::Coordinate& directionPt() const { return dest(); };


public:

    /**
    * Creates a half-edge originating from a given coordinate.
    *
    * @param p_orig the origin coordinate
    */
    HalfEdge(const geom::Coordinate& p_orig) :
        m_orig(p_orig)
    {};

    virtual ~HalfEdge() {};

    /**
    * Creates a HalfEdge pair representing an edge
    * between two vertices located at coordinates p0 and p1.
    *
    * @param p0 a vertex coordinate
    * @param p1 a vertex coordinate
    * @return the HalfEdge with origin at p0
    */
    static HalfEdge* create(const geom::Coordinate& p0, const geom::Coordinate& p1);

    /**
    * Links this edge with its sym (opposite) edge.
    * This must be done for each pair of edges created.
    *
    * @param p_sym the sym edge to link.
    */
    void link(HalfEdge* p_sym);

    /**
    * Gets the origin coordinate of this edge.
    *
    * @return the origin coordinate
    */
    const geom::Coordinate& orig() const { return m_orig; };

    /**
    * Gets the destination coordinate of this edge.
    *
    * @return the destination coordinate
    */
    const geom::Coordinate& dest() const { return m_sym->m_orig; }

    /**
    * The X component of the direction vector.
    *
    * @return the X component of the direction vector
    */
    double directionX() const { return directionPt().x - m_orig.x; }

    /**
    * The Y component of the direction vector.
    *
    * @return the Y component of the direction vector
    */
    double directionY() const { return directionPt().y - m_orig.y; }

    /**
    * Gets the symmetric pair edge of this edge.
    *
    * @return the symmetric pair edge
    */
    HalfEdge* sym() const { return m_sym; };

    /**
    * Gets the next edge CCW around the
    * destination vertex of this edge,
    * with the dest vertex as its origin.
    * If the vertex has degree 1 then this is the <b>sym</b> edge.
    *
    * @return the next edge
    */
    HalfEdge* next() const { return m_next; };

    /**
    * Gets the edge previous to this one
    * (with dest being the same as this orig).
    *
    * @return the previous edge to this one
    */
    HalfEdge* prev() const { return m_sym->next()->m_sym; };

    /**
    * Gets the next edge CCW around the origin of this edge,
    * with the same origin.
    *
    * @return the next edge around the origin
    */
    HalfEdge* oNext() const { return m_sym->m_next; };

    /**
    * Sets the next edge CCW around the destination vertex of this edge.
    *
    * @param e the next edge
    */
    void setNext(HalfEdge* e) { m_next = e; };

    /**
    * Finds the edge starting at the origin of this edge
    * with the given dest vertex,
    * if any.
    *
    * @param dest the dest vertex to search for
    * @return the edge with the required dest vertex, if it exists,
    * or null
    */
    HalfEdge* find(const geom::Coordinate& dest);

    /**
    * Tests whether this edge has the given orig and dest vertices.
    *
    * @param p0 the origin vertex to test
    * @param p1 the destination vertex to test
    * @return true if the vertices are equal to the ones of this edge
    */
    bool equals(const geom::Coordinate& p0, const geom::Coordinate& p1) const;

    /**
    * Inserts an edge
    * into the ring of edges around the origin vertex of this edge,
    * ensuring that the edges remain ordered CCW.
    * The inserted edge must have the same origin as this edge.
    *
    * @param eAdd the edge to insert
    */
    void insert(HalfEdge* eAdd);

    /**
    * Tests whether the edges around the origin
    * are sorted correctly.
    * Note that edges must be strictly increasing,
    * which implies no two edges can have the same direction point.
    *
    * @return true if the origin edges are sorted correctly
    */
    bool isEdgesSorted() const;

    /**
    * Implements the total order relation:
    *
    *    The angle of edge a is greater than the angle of edge b,
    *    where the angle of an edge is the angle made by
    *    the first segment of the edge with the positive x-axis
    *
    * When applied to a list of edges originating at the same point,
    * this produces a CCW ordering of the edges around the point.
    *
    * Using the obvious algorithm of computing the angle is not robust,
    * since the angle calculation is susceptible to roundoff error.
    * A robust algorithm is:
    *
    * * First, compare the quadrants the edge vectors lie in.
    *   If the quadrants are different,
    *   it is trivial to determine which edge has a greater angle.
    *
    * * if the vectors lie in the same quadrant, the
    *   geom::Orientation::index() function
    *   can be used to determine the relative orientation of the vectors.
    */
    int compareAngularDirection(const HalfEdge* e) const;
    int compareTo(const HalfEdge* e) const { return compareAngularDirection(e); };

    /**
    * Computes the degree of the origin vertex.
    * The degree is the number of edges
    * originating from the vertex.
    *
    * @return the degree of the origin vertex
    */
    int degree();

    /**
    * Finds the first node previous to this edge, if any.
    * If no such node exists (i.e. the edge is part of a ring)
    * then null is returned.
    *
    * @return an edge originating at the node prior to this edge, if any,
    *   or null if no node exists
    */
    HalfEdge* prevNode();

    friend std::ostream& operator<< (std::ostream& os, const HalfEdge& el);
    static void toStringNode(const HalfEdge* he, std::ostream& os);

};


} // namespace geos.edgegraph
} // namespace geos



