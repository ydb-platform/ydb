/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2010 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/polygonize/Polygonizer.java 0b3c7e3eb0d3e
 *
 **********************************************************************/

#ifndef GEOS_OP_POLYGONIZE_POLYGONIZER_H
#define GEOS_OP_POLYGONIZE_POLYGONIZER_H

#include <geos/export.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/GeometryComponentFilter.h> // for LineStringAdder inheritance
#include <geos/operation/polygonize/PolygonizeGraph.h>

#include <memory>
#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Geometry;

class LineString;

class Polygon;
}
namespace operation {
namespace polygonize {
class EdgeRing;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace polygonize { // geos::operation::polygonize

/** \brief
 * Polygonizes a set of Geometrys which contain linework that
 * represents the edges of a planar graph.
 *
 * All types of Geometry are accepted as input; the constituent linework is extracted
 * as the edges to be polygonized.
 * The edges must be correctly noded; that is, they must only meet
 * at their endpoints. Polygonization will accept incorrectly noded input but will
 * not form polygons from non-noded edges, and reports them as errors.
 *
 * The Polygonizer reports the follow kinds of errors:
 *
 * - <b>Dangles</b> - edges which have one or both ends which are
 *   not incident on another edge endpoint
 * - <b>Cut Edges</b> - edges which are connected at both ends but
 *   which do not form part of a polygon
 * - <b>Invalid Ring Lines</b> - edges which form rings which are invalid
 *   (e.g. the component lines contain a self-intersection)
 *
 *   The Polygonizer constructor allows extracting only polygons which form a
 *   valid polygonal result.
 *   The set of extracted polygons is guaranteed to be edge-disjoint.
 *   This is useful when it is known that the input lines form a valid
 *   polygonal geometry (which may include holes or nested polygons).
 *
 */
class GEOS_DLL Polygonizer {
private:
    /**
     * Add every linear element in a geometry into the polygonizer graph.
     */
    class GEOS_DLL LineStringAdder: public geom::GeometryComponentFilter {
    public:
        Polygonizer* pol;
        explicit LineStringAdder(Polygonizer* p);
        //void filter_rw(geom::Geometry *g);
        void filter_ro(const geom::Geometry* g) override;
    };

    // default factory
    LineStringAdder lineStringAdder;

    /**
     * Add a linestring to the graph of polygon edges.
     *
     * @param line the {@link LineString} to add
     */
    void add(const geom::LineString* line);

    /**
     * Perform the polygonization, if it has not already been carried out.
     */
    void polygonize();

    static void findValidRings(const std::vector<EdgeRing*>& edgeRingList,
                               std::vector<EdgeRing*>& validEdgeRingList,
                               std::vector<std::unique_ptr<geom::LineString>>& invalidRingList);

    void findShellsAndHoles(const std::vector<EdgeRing*>& edgeRingList);

    void findDisjointShells();

    static void findOuterShells(std::vector<EdgeRing*>& shellList);

    static std::vector<std::unique_ptr<geom::Polygon>> extractPolygons(std::vector<EdgeRing*> & shellList, bool includeAll);

    bool extractOnlyPolygonal;
    bool computed;

protected:

    std::unique_ptr<PolygonizeGraph> graph;

    // initialize with empty collections, in case nothing is computed
    std::vector<const geom::LineString*> dangles;
    std::vector<const geom::LineString*> cutEdges;
    std::vector<std::unique_ptr<geom::LineString>> invalidRingLines;

    std::vector<EdgeRing*> holeList;
    std::vector<EdgeRing*> shellList;
    std::vector<std::unique_ptr<geom::Polygon>> polyList;

public:

    /** \brief
     * Create a Polygonizer with the same GeometryFactory
     * as the input Geometrys.
     *
     * @param onlyPolygonal true if only polygons which form a valid polygonal geometry should be extracted
     */
    explicit Polygonizer(bool onlyPolygonal = false);

    ~Polygonizer() = default;

    /** \brief
     * Add a collection of geometries to be polygonized.
     * May be called multiple times.
     * Any dimension of Geometry may be added;
     * the constituent linework will be extracted and used
     *
     * @param geomList a list of Geometry with linework to be polygonized
     */
    void add(std::vector<geom::Geometry*>* geomList);

    /** \brief
     * Add a collection of geometries to be polygonized.
     * May be called multiple times.
     * Any dimension of Geometry may be added;
     * the constituent linework will be extracted and used
     *
     * @param geomList a list of Geometry with linework to be polygonized
     */
    void add(std::vector<const geom::Geometry*>* geomList);

    /**
     * Add a geometry to the linework to be polygonized.
     * May be called multiple times.
     * Any dimension of Geometry may be added;
     * the constituent linework will be extracted and used
     *
     * @param g a Geometry with linework to be polygonized
     */
    void add(geom::Geometry* g);

    /**
     * Add a geometry to the linework to be polygonized.
     * May be called multiple times.
     * Any dimension of Geometry may be added;
     * the constituent linework will be extracted and used
     *
     * @param g a Geometry with linework to be polygonized
     */
    void add(const geom::Geometry* g);

    /** \brief
     * Gets the list of polygons formed by the polygonization.
     *
     * Ownership of vector is transferred to caller, subsequent
     * calls will return NULL.
     * @return a collection of Polygons
     */
    std::vector<std::unique_ptr<geom::Polygon>> getPolygons();

    /** \brief
     * Get the list of dangling lines found during polygonization.
     *
     * @return a (possibly empty) collection of pointers to
     *         the input LineStrings which are dangles.
     *
     */
    const std::vector<const geom::LineString*>& getDangles();

    bool hasDangles();

    /** \brief
     * Get the list of cut edges found during polygonization.
     *
     * @return a (possibly empty) collection of pointers to
     *         the input LineStrings which are cut edges.
     *
     */
    const std::vector<const geom::LineString*>& getCutEdges();

    bool hasCutEdges();

    /** \brief
     * Get the list of lines forming invalid rings found during
     * polygonization.
     *
     * @return a (possibly empty) collection of pointers to
     *         the input LineStrings which form invalid rings
     *
     */
    const std::vector<std::unique_ptr<geom::LineString>>& getInvalidRingLines();

    bool hasInvalidRingLines();

    bool allInputsFormPolygons();

// This seems to be needed by    GCC 2.95.4
    friend class Polygonizer::LineStringAdder;
};

} // namespace geos::operation::polygonize
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_POLYGONIZE_POLYGONIZER_H
