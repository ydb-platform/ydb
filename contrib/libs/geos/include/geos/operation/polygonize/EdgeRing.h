/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
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
 * Last port: operation/polygonize/EdgeRing.java 0b3c7e3eb0d3e
 *
 **********************************************************************/


#ifndef GEOS_OP_POLYGONIZE_EDGERING_H
#define GEOS_OP_POLYGONIZE_EDGERING_H

#include <geos/export.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/operation/polygonize/PolygonizeDirectedEdge.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Polygon.h>

#include <memory>
#include <vector>
#include <geos/geom/Location.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class LineString;
class CoordinateSequence;
class GeometryFactory;
class Coordinate;
}
namespace planargraph {
class DirectedEdge;
}
namespace index {
namespace strtree {
class STRtree;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace polygonize { // geos::operation::polygonize

/** \brief
 * Represents a ring of PolygonizeDirectedEdge which form
 * a ring of a polygon.  The ring may be either an outer shell or a hole.
 */
class GEOS_DLL EdgeRing {
private:
    const geom::GeometryFactory* factory;

    typedef std::vector<const PolygonizeDirectedEdge*> DeList;
    DeList deList;

    // cache the following data for efficiency
    std::unique_ptr<geom::LinearRing> ring;
    std::unique_ptr<geom::CoordinateArraySequence> ringPts;
    std::unique_ptr<algorithm::locate::PointOnGeometryLocator> ringLocator;

    std::unique_ptr<std::vector<std::unique_ptr<geom::LinearRing>>> holes;

    EdgeRing* shell = nullptr;
    bool is_hole;
    bool is_processed = false;
    bool is_included_set = false;
    bool is_included = false;
    bool visitedByUpdateIncludedRecursive = false;

    /** \brief
     * Computes the list of coordinates which are contained in this ring.
     * The coordinates are computed once only and cached.
     *
     * @return an array of the Coordinate in this ring
     */
    const geom::CoordinateSequence* getCoordinates();

    static void addEdge(const geom::CoordinateSequence* coords,
                        bool isForward,
                        geom::CoordinateArraySequence* coordList);

    algorithm::locate::PointOnGeometryLocator* getLocator() {
        if (ringLocator == nullptr) {
            ringLocator.reset(new algorithm::locate::IndexedPointInAreaLocator(*getRingInternal()));
        }
        return ringLocator.get();
    }

public:
    /** \brief
     * Adds a DirectedEdge which is known to form part of this ring.
     *
     * @param de the DirectedEdge to add. Ownership to the caller.
     */
    void add(const PolygonizeDirectedEdge* de);

    /**
     * \brief
     * Find the innermost enclosing shell EdgeRing
     * containing this, if any.
     *
     * The innermost enclosing ring is the <i>smallest</i> enclosing ring.
     * The algorithm used depends on the fact that:
     *
     * ring A contains ring B iff envelope(ring A) contains envelope(ring B)
     *
     * This routine is only safe to use if the chosen point of the hole
     * is known to be properly contained in a shell
     * (which is guaranteed to be the case if the hole does not touch
     * its shell)
     *
     * @return containing EdgeRing, if there is one
     * @return null if no containing EdgeRing is found
     */
    EdgeRing* findEdgeRingContaining(const std::vector<EdgeRing*> & erList);

    /**
     * \brief
     * Traverses a ring of DirectedEdges, accumulating them into a list.
     *
     * This assumes that all dangling directed edges have been removed from
     * the graph, so that there is always a next dirEdge.
     *
     * @param startDE the DirectedEdge to start traversing at
     * @return a vector of DirectedEdges that form a ring
     */
    static std::vector<PolygonizeDirectedEdge*> findDirEdgesInRing(PolygonizeDirectedEdge* startDE);

    /**
     * \brief
     * Finds a point in a list of points which is not contained in
     * another list of points.
     *
     * @param testPts the CoordinateSequence to test
     * @param pts the CoordinateSequence to test the input points against
     * @return a Coordinate reference from <code>testPts</code> which is
     * not in <code>pts</code>, or <code>Coordinate::nullCoord</code>
     */
    static const geom::Coordinate& ptNotInList(
        const geom::CoordinateSequence* testPts,
        const geom::CoordinateSequence* pts);

    /** \brief
     * Tests whether a given point is in an array of points.
     * Uses a value-based test.
     *
     * @param pt a Coordinate for the test point
     * @param pts an array of Coordinate to test
     * @return <code>true</code> if the point is in the array
     */
    static bool isInList(const geom::Coordinate& pt,
                         const geom::CoordinateSequence* pts);

    explicit EdgeRing(const geom::GeometryFactory* newFactory);

    ~EdgeRing() = default;

    void build(PolygonizeDirectedEdge* startDE);

    void computeHole();

    /** \brief
     * Tests whether this ring is a hole.
     *
     * Due to the way the edges in the polyongization graph are linked,
     * a ring is a hole if it is oriented counter-clockwise.
     * @return <code>true</code> if this ring is a hole
     */
    bool isHole() const {
        return is_hole;
    }

    /* Indicates whether we know if the ring should be included in a polygonizer
     * output of only polygons.
     */
    bool isIncludedSet() const {
        return is_included_set;
    }

    /* Indicates whether the ring should be included in a polygonizer output of
     * only polygons.
     */
    bool isIncluded() const {
        return is_included;
    }

    void setIncluded(bool included) {
        is_included = included;
        is_included_set = true;
    }

    bool isProcessed() const {
        return is_processed;
    }

    void setProcessed(bool processed) {
        is_processed = processed;
    }

    /** \brief
     *  Sets the containing shell ring of a ring that has been determined to be a hole.
     *
     *  @param shellRing the shell ring
     */
    void setShell(EdgeRing* shellRing) {
        shell = shellRing;
    }

    /** \brief
     * Tests whether this ring has a shell assigned to it.
     *
     * @return true if the ring has a shell
     */
    bool hasShell() const {
        return shell != nullptr;
    }

    /** \brief
     * Gets the shell for this ring. The shell is the ring itself if it is
     * not a hole, otherwise it is the parent shell.
     *
     * @return the shell for the ring
     */
    EdgeRing* getShell() {
        return isHole() ? shell : this;
    }

    /** \brief
     * Tests whether this ring is an outer hole.
     * A hole is an outer hole if it is not contained by any shell.
     *
     * @return true if the ring is an outer hole.
     */
    bool isOuterHole() const {
        if (!isHole()) {
            return false;
        }

        return !hasShell();
    }

    /** \brief
     * Tests whether this ring is an outer shell.
     *
     * @return true if the ring is an outer shell.
     */
    bool isOuterShell() const {
        return getOuterHole() != nullptr;
    }

    /** \brief
     * Gets the outer hole of a shell, if it has one.
     * An outer hole is one that is not contained in any other shell.
     *
     * Each disjoint connected group of shells is surrounded by
     * an outer hole.
     *
     * @return the outer hole edge ring, or nullptr
     */
    EdgeRing* getOuterHole() const;

    /** \brief
     * Updates the included status for currently non-included shells
     * based on whether they are adjacent to an included shell.
     */
    void updateIncludedRecursive();

    /** \brief
     * Adds a hole to the polygon formed by this ring.
     *
     * @param hole the LinearRing forming the hole.
     */
    void addHole(geom::LinearRing* hole);

    void addHole(EdgeRing* holeER);

    /** \brief
     * Computes the Polygon formed by this ring and any contained holes.
     *
     * LinearRings ownership is transferred to returned polygon.
     * Subsequent calls to the function will return NULL.
     *
     * @return the Polygon formed by this ring and its holes.
     */
    std::unique_ptr<geom::Polygon> getPolygon();

    /** \brief
     * Tests if the LinearRing ring formed by this edge ring
     * is topologically valid.
     */
    bool isValid();

    /** \brief
     * Gets the coordinates for this ring as a LineString.
     *
     * Used to return the coordinates in this ring
     * as a valid geometry, when it has been detected that the ring
     * is topologically invalid.
     * @return a LineString containing the coordinates in this ring
     */
    std::unique_ptr<geom::LineString> getLineString();

    /** \brief
     * Returns this ring as a LinearRing, or null if an Exception
     * occurs while creating it (such as a topology problem).
     *
     * Ownership of ring is retained by the object.
     * Details of problems are written to standard output.
     */
    geom::LinearRing* getRingInternal();

    /** \brief
     * Returns this ring as a LinearRing, or null if an Exception
     * occurs while creating it (such as a topology problem).
     *
     * Details of problems are written to standard output.
     * Caller gets ownership of ring.
     */
    std::unique_ptr<geom::LinearRing> getRingOwnership();

    bool isInRing(const geom::Coordinate & pt) {
        return geom::Location::EXTERIOR != getLocator()->locate(&pt);
    }
};

} // namespace geos::operation::polygonize
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_POLYGONIZE_EDGERING_H
