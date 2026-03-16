/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Excensus LLC.
 * Copyright (C) 2019 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: triangulate/quadedge/QuadEdgeSubdivision.java r524
 *
 **********************************************************************/

#ifndef GEOS_TRIANGULATE_QUADEDGE_QUADEDGESUBDIVISION_H
#define GEOS_TRIANGULATE_QUADEDGE_QUADEDGESUBDIVISION_H

#include <memory>
#include <list>
#include <stack>
#include <unordered_set>
#include <vector>

#include <geos/geom/MultiLineString.h>
#include <geos/triangulate/quadedge/QuadEdge.h>
#include <geos/triangulate/quadedge/QuadEdgeLocator.h>
#include <geos/triangulate/quadedge/QuadEdgeQuartet.h>
#include <geos/triangulate/quadedge/Vertex.h>

namespace geos {

namespace geom {

class CoordinateSequence;
class GeometryCollection;
class MultiLineString;
class GeometryFactory;
class Coordinate;
class Geometry;
class Envelope;
}

namespace triangulate { //geos.triangulate
namespace quadedge { //geos.triangulate.quadedge

class TriangleVisitor;

const double EDGE_COINCIDENCE_TOL_FACTOR = 1000;

//-- Frame size factor for initializing subdivision frame.  Larger is more robust
const double FRAME_SIZE_FACTOR = 100;

/** \brief
 * A class that contains the [QuadEdges](@ref QuadEdge) representing a planar
 * subdivision that models a triangulation.
 *
 * The subdivision is constructed using the quadedge algebra defined in the class QuadEdge.
 *
 * All metric calculations are done in the Vertex class.
 * In addition to a triangulation, subdivisions support extraction of Voronoi diagrams.
 * This is easily accomplished, since the Voronoi diagram is the dual
 * of the Delaunay triangulation.
 *
 * Subdivisions can be provided with a tolerance value. Inserted vertices which
 * are closer than this value to vertices already in the subdivision will be
 * ignored. Using a suitable tolerance value can prevent robustness failures
 * from happening during Delaunay triangulation.
 *
 * Subdivisions maintain a **frame** triangle around the client-created
 * edges. The frame is used to provide a bounded "container" for all edges
 * within a TIN. Normally the frame edges, frame connecting edges, and frame
 * triangles are not included in client processing.
 *
 * @author JTS: David Skea
 * @author JTS: Martin Davis
 * @author Benjamin Campbell
 */
class GEOS_DLL QuadEdgeSubdivision {
public:
    typedef std::vector<QuadEdge*> QuadEdgeList;

    /** \brief
     * Gets the edges for the triangle to the left of the given QuadEdge.
     *
     * @param startQE
     * @param triEdge
     *
     * @throws IllegalArgumentException if the edges do not form a triangle
     */
    static void getTriangleEdges(const QuadEdge& startQE,
                                 const QuadEdge* triEdge[3]);

private:
    /**
     * Use a deque to ensure QuadEdge pointers are stable.
     * Note that it is NOT safe to erase entries from the deque.
     */
    std::deque<QuadEdgeQuartet> quadEdges;
    std::array<QuadEdge*, 3> startingEdges;
    double tolerance;
    double edgeCoincidenceTolerance;
    std::array<Vertex, 3> frameVertex;
    geom::Envelope frameEnv;
    std::unique_ptr<QuadEdgeLocator> locator;
    bool visit_state_clean;

public:
    /** \brief
     * Creates a new instance of a quad-edge subdivision based on a frame triangle
     * that encloses a supplied bounding box.
     * A new super-bounding box that contains the triangle is computed and stored.
     *
     * @param env the bouding box to surround
     * @param tolerance the tolerance value for determining if two sites are equal
     */
    QuadEdgeSubdivision(const geom::Envelope& env, double tolerance);

    virtual ~QuadEdgeSubdivision() = default;

private:
    virtual void createFrame(const geom::Envelope& env);

    virtual void initSubdiv();

public:
    /** \brief
     * Gets the vertex-equality tolerance value used in this subdivision
     *
     * @return the tolerance value
     */
    inline double
    getTolerance() const
    {
        return tolerance;
    }

    /** \brief
     * Gets the envelope of the Subdivision (including the frame).
     *
     * @return the envelope
     */
    inline const geom::Envelope&
    getEnvelope() const
    {
        return frameEnv;
    }

    /** \brief
     * Gets the collection of base {@link QuadEdge}s (one for every pair of
     * vertices which is connected).
     *
     * @return a QuadEdgeList
     */
    inline std::deque<QuadEdgeQuartet>&
    getEdges()
    {
        return quadEdges;
    }

    /** \brief
     * Sets the QuadEdgeLocator to use for locating containing triangles
     * in this subdivision.
     *
     * @param p_locator
     *          a QuadEdgeLocator
     */
    inline void
    setLocator(std::unique_ptr<QuadEdgeLocator> p_locator)
    {
        this->locator = std::move(p_locator);
    }

    /** \brief
     * Creates a new quadedge, recording it in the edges list.
     *
     * @param o
     * @param d
     * @return
     */
    virtual QuadEdge& makeEdge(const Vertex& o, const Vertex& d);

    /** \brief
     * Creates a new QuadEdge connecting the destination of a to the origin of b,
     * in such a way that all three have the same left face after the connection
     * is complete.
     * The quadedge is recorded in the edges list.
     *
     * @param a
     * @param b
     * @return
     */
    virtual QuadEdge& connect(QuadEdge& a, QuadEdge& b);

    /** \brief
     * Deletes a quadedge from the subdivision. Linked quadedges are updated to
     * reflect the deletion.
     *
     * @param e the quadedge to delete
     */
    void remove(QuadEdge& e);

    /** \brief
     * Locates an edge of a triangle which contains a location
     * specified by a Vertex `v`.
     *
     * The edge returned has the property that either v is on e,
     * or e is an edge of a triangle containing v.
     * The search starts from startEdge and proceeds on the general direction of v.
     *
     * This locate algorithm relies on the subdivision being Delaunay. For
     * non-Delaunay subdivisions, this may loop for ever.
     *
     * @param v the location to search for
     * @param startEdge an edge of the subdivision to start searching at
     * @return a QuadEdge which contains v, or is on the edge of a triangle containing v
     * @throws LocateFailureException if the location algorithm fails to converge in a
     *                                reasonable number of iterations.
     *
     * @note The returned pointer **should not** be freed be the caller.
     */
    QuadEdge* locateFromEdge(const Vertex& v,
                             const QuadEdge& startEdge) const;

    /** \brief
     * Finds a quadedge of a triangle containing a location
     * specified by a [Vertex](@ref triangulate::quadedge::Vertex), if one exists.
     *
     * @param v the vertex to locate
     * @return a QuadEdge on the edge of a triangle which touches or contains the location
     * @return `null` if no such triangle exists.
     *
     * @note The returned pointer **should not** be freed be the caller.
     */
    inline QuadEdge*
    locate(const Vertex& v) const
    {
        return locator->locate(v);
    }

    /** \brief
     * Finds a quadedge of a triangle containing a location
     * specified by a geom::Coordinate, if one exists.
     *
     * @param p the Coordinate to locate
     * @return a QuadEdge on the edge of a triangle which touches or contains the location
     * @return `null` if no such triangle exists.
     *
     * @note The returned pointer **should not** be freed be the caller.
     */
    inline QuadEdge*
    locate(const geom::Coordinate& p)
    {
        return locator->locate(Vertex(p));
    }

    /** \brief
     * Locates the edge between the given vertices, if it exists in the
     * subdivision.
     *
     * @param p0 a coordinate
     * @param p1 another coordinate
     * @return the edge joining the coordinates, if present
     * @return `null` if no such edge exists
     *
     * @note the caller **should not** free the returned pointer
     */
    QuadEdge* locate(const geom::Coordinate& p0, const geom::Coordinate& p1);

    /** \brief
     * Inserts a new site into the Subdivision, connecting it to the vertices of
     * the containing triangle (or quadrilateral, if the split point falls on an
     * existing edge).
     *
     * This method does NOT maintain the Delaunay condition. If desired, this must
     * be checked and enforced by the caller.
     *
     * This method does NOT check if the inserted vertex falls on an edge. This
     * must be checked by the caller, since this situation may cause erroneous
     * triangulation
     *
     * @param v the vertex to insert
     * @return a new quad edge terminating in v
     */
    QuadEdge& insertSite(const Vertex& v);

    /** \brief
     * Tests whether a QuadEdge is an edge incident on a frame triangle vertex.
     *
     * @param e the edge to test
     * @return `true` if the edge is connected to the frame triangle
     */
    bool isFrameEdge(const QuadEdge& e) const;

    /** \brief
     * Tests whether a QuadEdge is an edge on the border of the frame facets and
     * the internal facets. E.g. an edge which does not itself touch a frame
     * vertex, but which touches an edge which does.
     *
     * @param e the edge to test
     * @return `true` if the edge is on the border of the frame
     */
    bool isFrameBorderEdge(const QuadEdge& e) const;

    /** \brief
     * Tests whether a vertex is a vertex of the outer triangle.
     *
     * @param v the vertex to test
     * @return `true` if the vertex is an outer triangle vertex
     */
    bool isFrameVertex(const Vertex& v) const;


    /** \brief
     * Tests whether a [Coordinate](@ref geom::Coordinate) lies on a QuadEdge,
     *  up to a tolerance determined by the subdivision tolerance.
     *
     * @param e a QuadEdge
     * @param p a point
     * @return `true` if the vertex lies on the edge
     */
    bool isOnEdge(const QuadEdge& e, const geom::Coordinate& p) const;

    /** \brief
     * Tests whether a Vertex is the start or end vertex of a
     * QuadEdge, up to the subdivision tolerance distance.
     *
     * @param e
     * @param v
     * @return `true` if the vertex is a endpoint of the edge
     */
    bool isVertexOfEdge(const QuadEdge& e, const Vertex& v) const;

    /** \brief
     * Gets all primary quadedges in the subdivision.
     *
     * A primary edge is a QuadEdge which occupies the 0'th position in its
     * array of associated quadedges. These provide the unique geometric
     * edges of the triangulation.
     *
     * @param includeFrame `true` if the frame edges are to be included
     * @return a List of QuadEdges. The caller takes ownership of the returned QuadEdgeList but not the
     *         items it contains.
     */
    std::unique_ptr<QuadEdgeList> getPrimaryEdges(bool includeFrame);

    /*****************************************************************************
     * Visitors
     ****************************************************************************/

    void visitTriangles(TriangleVisitor* triVisitor, bool includeFrame);

private:
    typedef std::stack<QuadEdge*> QuadEdgeStack;
    typedef std::vector<std::unique_ptr<geom::CoordinateSequence>> TriList;

    /** \brief
     * The quadedges forming a single triangle.
     *
     * Only one visitor is allowed to be active at a time, so this is safe.
     */
    QuadEdge* triEdges[3];

    /** \brief
     * Resets the `visited` flag of each `QuadEdge` prior to iteration, if necessary.
     */
    void prepareVisit();

    /** \brief
     * Stores the edges for a visited triangle. Also pushes sym (neighbour) edges
     * on stack to visit later.
     *
     * @param edge
     * @param edgeStack
     * @param includeFrame
     * @return the visited triangle edges
     * @return `null` if the triangle should not be visited (for instance, if it is
     *         outer)
     */
    QuadEdge** fetchTriangleToVisit(QuadEdge* edge, QuadEdgeStack& edgeStack, bool includeFrame);

    /** \brief
     * Gets the coordinates for each triangle in the subdivision as an array.
     *
     * @param includeFrame true if the frame triangles should be included
     * @param triList a list of Coordinate[4] representing each triangle
     */
    void getTriangleCoordinates(TriList* triList, bool includeFrame);

private:
    class TriangleCoordinatesVisitor;
    class TriangleCircumcentreVisitor;

public:
    /** \brief
     * Gets the geometry for the edges in the subdivision as a [MultiLineString](@ref geom::MultiLineString)
     * containing 2-point lines.
     *
     * @param geomFact the GeometryFactory to use
     * @return a MultiLineString
     * @note The caller takes ownership of the returned object.
     */
    std::unique_ptr<geom::MultiLineString> getEdges(const geom::GeometryFactory& geomFact);

    /** \brief
     * Gets the geometry for the triangles in a triangulated subdivision as a
     * [GeometryCollection](@ref geos::geom::GeometryCollection)
     * of triangular [Polygons](@ref geos::geom::Polygon).
     *
     * @param geomFact the GeometryFactory to use
     * @return a GeometryCollection of triangular polygons.
     *
     * @note The caller takes ownership of the returned object.
     */
    std::unique_ptr<geom::GeometryCollection> getTriangles(const geom::GeometryFactory& geomFact);

    /** \brief
     * Gets the cells in the Voronoi diagram for this triangulation.
     * The cells are returned as a [GeometryCollection](@ref geom::GeometryCollection)
     * of [Polygons](@ref geom::Polygon).
     *
     * The userData of each polygon is set to be the [Coordinate](@ref geom::Coordinate)
     * of the cell site. This allows easily associating external
     * data associated with the sites to the cells.
     *
     * @param geomFact a geometry factory
     * @return a GeometryCollection of Polygons
     */
    std::unique_ptr<geom::GeometryCollection> getVoronoiDiagram(const geom::GeometryFactory& geomFact);

    /** \brief
     * Gets the cells in the Voronoi diagram for this triangulation.
     *
     * The cells are returned as a [GeometryCollection](@ref geom::GeometryCollection) of
     * [LineStrings](@ref geom::LineString). The userData of each polygon is set to be the
     * [Coordinate](@ref geom::Coordinate) of the cell site. This allows easily associating external
     * data associated with the sites to the cells.
     *
     * @param geomFact a geometry factory
     * @return a MultiLineString
     */
    std::unique_ptr<geom::MultiLineString> getVoronoiDiagramEdges(const geom::GeometryFactory& geomFact);

    /** \brief
     * Gets a List of [Polygons](@ref geom::Polygon) for the Voronoi cells
     * of this triangulation.
     *
     * The userData of each polygon is set to be the [Coordinate](@ref geom::Coordinate)
     * of the cell site. This allows easily associating external data associated
     * with the sites to the cells.
     *
     * @param geomFact a geometry factory
     * @return a List of Polygons
     */
    std::vector<std::unique_ptr<geom::Geometry>> getVoronoiCellPolygons(const geom::GeometryFactory& geomFact);

    /** \brief
     * Gets a List of [LineStrings](@ref geom::LineString) for the Voronoi cells
     * of this triangulation.
     *
     * The userData of each LineString is set to be the [Coordinate](@ref geom::Coordinate)
     * of the cell site. This allows easily associating external
     * data associated with the sites to the cells.
     *
     * @param geomFact a geometry factory
     * @return a List of LineString
     */
    std::vector<std::unique_ptr<geom::Geometry>> getVoronoiCellEdges(const geom::GeometryFactory& geomFact);

    /** \brief
     * Gets a collection of [QuadEdges](@ref QuadEdge) whose origin vertices are a unique set
     * which includes all vertices in the subdivision.
     *
     * The frame vertices can be included if required. This is useful for algorithms which require
     * traversing the subdivision starting at all vertices.
     * Returning a quadedge for each vertex is more efficient than the alternative
     * of finding the actual vertices using `getVertices()` and then locating
     * quadedges attached to them.
     *
     * @param includeFrame `true` if the frame vertices should be included
     * @return a collection of QuadEdge with the vertices of the subdivision as their origins
     */
    std::unique_ptr<QuadEdgeSubdivision::QuadEdgeList> getVertexUniqueEdges(bool includeFrame);

    /** \brief
     * Gets the Voronoi cell around a site specified by the origin of a QuadEdge.
     *
     * The userData of the polygon is set to be the [Coordinate](@ref geom::Coordinate)
     * of the site. This allows attaching external data associated with the site
     * to this cell polygon.
     *
     * @param qe a quadedge originating at the cell site
     * @param geomFact a factory for building the polygon
     * @return a polygon indicating the cell extent
     */
    std::unique_ptr<geom::Geometry> getVoronoiCellPolygon(const QuadEdge* qe, const geom::GeometryFactory& geomFact);

    /** \brief
     * Gets the Voronoi cell edge around a site specified by the origin of a QuadEdge.
     *
     * The userData of the LineString is set to be the [Coordinate](@ref geom::Coordinate)
     * of the site.  This allows attaching external data associated with
     * the site to this cell polygon.
     *
     * @param qe a quadedge originating at the cell site
     * @param geomFact a factory for building the polygon
     * @return a polygon indicating the cell extent
     */
    std::unique_ptr<geom::Geometry> getVoronoiCellEdge(const QuadEdge* qe, const geom::GeometryFactory& geomFact);

};

} //namespace geos.triangulate.quadedge
} //namespace geos.triangulate
} //namespace goes

#endif //GEOS_TRIANGULATE_QUADEDGE_QUADEDGESUBDIVISION_H
