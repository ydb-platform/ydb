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
 * Last port: triangulate/VoronoiDiagramBuilder.java r524
 *
 **********************************************************************/

#ifndef GEOS_TRIANGULATE_VORONOIDIAGRAMBUILDER_H
#define GEOS_TRIANGULATE_VORONOIDIAGRAMBUILDER_H

#include <geos/triangulate/quadedge/QuadEdgeSubdivision.h>
#include <geos/geom/Envelope.h> // for composition
#include <memory>
#include <iostream>

namespace geos {
namespace geom {
class Geometry;
class CoordinateSequence;
class GeometryCollection;
class GeometryFactory;
}
namespace triangulate { //geos.triangulate

/** \brief
 * A utility class which creates Voronoi Diagrams from collections of points.
 *
 * The diagram is returned as a geom::GeometryCollection of {@link geom::Polygon}s,
 * clipped to the larger of a supplied envelope or to an envelope determined
 * by the input sites.
 *
 * @author Martin Davis
 *
 */
class GEOS_DLL VoronoiDiagramBuilder {
public:
    /** \brief
     * Creates a new Voronoi diagram builder.
     *
     */
    VoronoiDiagramBuilder();

    ~VoronoiDiagramBuilder() = default;

    /** \brief
     * Sets the sites (point or vertices) which will be diagrammed.
     * All vertices of the given geometry will be used as sites.
     *
     * @param geom the geometry from which the sites will be extracted.
     */
    void setSites(const geom::Geometry& geom);

    /** \brief
     * Sets the sites (point or vertices) which will be diagrammed
     * from a collection of {@link geom::Coordinate}s.
     *
     * @param coords a collection of Coordinates.
     */
    void setSites(const geom::CoordinateSequence& coords);

    /** \brief
     * Sets the envelope to clip the diagram to.
     *
     * The diagram will be clipped to the larger
     * of this envelope or an envelope surrounding the sites.
     *
     * @param clipEnv the clip envelope; must be kept alive by
     *                caller until done with this instance;
     *                set to 0 for no clipping.
     */
    void setClipEnvelope(const geom::Envelope* clipEnv);

    /** \brief
     * Sets the snapping tolerance which will be used
     * to improved the robustness of the triangulation computation.
     *
     * A tolerance of 0.0 specifies that no snapping will take place.
     *
     * @param tolerance the tolerance distance to use
     */
    void setTolerance(double tolerance);

    /** \brief
     * Gets the quadedge::QuadEdgeSubdivision which models the computed diagram.
     *
     * @return the subdivision containing the triangulation
     */
    std::unique_ptr<quadedge::QuadEdgeSubdivision> getSubdivision();

    /** \brief
     * Gets the faces of the computed diagram as a geom::GeometryCollection
     * of {@link geom::Polygon}s, clipped as specified.
     *
     * @param geomFact the geometry factory to use to create the output
     * @return the faces of the diagram
     */
    std::unique_ptr<geom::GeometryCollection> getDiagram(const geom::GeometryFactory& geomFact);

    /** \brief
     * Gets the faces of the computed diagram as a geom::GeometryCollection
     * of {@link geom::LineString}s, clipped as specified.
     *
     * @param geomFact the geometry factory to use to create the output
     * @return the faces of the diagram
     */
    std::unique_ptr<geom::Geometry> getDiagramEdges(const geom::GeometryFactory& geomFact);

private:

    std::unique_ptr<geom::CoordinateSequence> siteCoords;
    double tolerance;
    std::unique_ptr<quadedge::QuadEdgeSubdivision> subdiv;
    const geom::Envelope* clipEnv; // externally owned
    geom::Envelope diagramEnv;

    void create();

    static std::unique_ptr<geom::GeometryCollection>
    clipGeometryCollection(std::vector<std::unique_ptr<geom::Geometry>> & geoms, const geom::Envelope& clipEnv);

};

} //namespace geos.triangulate
} //namespace geos

#endif //GEOS_TRIANGULATE_VORONOIDIAGRAMBUILDER_H
