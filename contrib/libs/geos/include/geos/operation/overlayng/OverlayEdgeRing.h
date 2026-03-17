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

#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/LinearRing.h>
#include <geos/export.h>

// Forward declarations
namespace geos {
namespace algorithm {
namespace locate {
class PointOnGeometryLocator;
}
}
namespace geom {
class Coordinate;
class CoordinateSequence;
class GeometryFactory;
class LinearRing;
class Polygon;
}
namespace operation {
namespace overlayng {
class OverlayEdge;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;
using algorithm::locate::PointOnGeometryLocator;
using algorithm::locate::IndexedPointInAreaLocator;

class GEOS_DLL OverlayEdgeRing {

private:

    // Members
    OverlayEdge* startEdge;
    std::unique_ptr<LinearRing> ring;
    bool m_isHole;
    CoordinateArraySequence ringPts;
    std::unique_ptr<IndexedPointInAreaLocator> locator;
    OverlayEdgeRing* shell;
    // a list of EdgeRings which are holes in this EdgeRing
    std::vector<OverlayEdgeRing*> holes;

    // Methods
    void computeRingPts(OverlayEdge* start, CoordinateArraySequence& pts);
    void computeRing(const CoordinateArraySequence& ringPts, const GeometryFactory* geometryFactory);

    /**
    * Computes the list of coordinates which are contained in this ring.
    * The coordinates are computed once only and cached.
    * @return an array of the {@link Coordinate}s in this ring
    */
    const CoordinateArraySequence& getCoordinates();
    PointOnGeometryLocator* getLocator();
    void closeRing(CoordinateArraySequence& pts);


public:

    OverlayEdgeRing(OverlayEdge* start, const GeometryFactory* geometryFactory);

    std::unique_ptr<LinearRing> getRing();
    const LinearRing* getRingPtr() const;

    /**
    * Tests whether this ring is a hole.
    * @return <code>true</code> if this ring is a hole
    */
    bool isHole() const;

    /**
    * Sets the containing shell ring of a ring that has been determined to be a hole.
    *
    * @param shell the shell ring
    */
    void setShell(OverlayEdgeRing* p_shell);

    /**
    * Tests whether this ring has a shell assigned to it.
    *
    * @return true if the ring has a shell
    */
    bool hasShell() const;

    /**
    * Gets the shell for this ring.  The shell is the ring itself if it is not a hole, otherwise its parent shell.
    *
    * @return the shell for this ring
    */
    const OverlayEdgeRing* getShell() const;

    void addHole(OverlayEdgeRing* ring);

    bool isInRing(const Coordinate& pt);

    const Coordinate& getCoordinate();

    /**
    * Computes the {@link Polygon} formed by this ring and any contained holes.
    * @return the {@link Polygon} formed by this ring and its holes.
    */
    std::unique_ptr<Polygon> toPolygon(const GeometryFactory* factory);

    OverlayEdge* getEdge();

    /**
    * Finds the innermost enclosing shell OverlayEdgeRing
    * containing this OverlayEdgeRing, if any.
    * The innermost enclosing ring is the smallest enclosing ring.
    * The algorithm used depends on the fact that:
    *  ring A contains ring B iff envelope(ring A) contains envelope(ring B)
    *
    * This routine is only safe to use if the chosen point of the hole
    * is known to be properly contained in a shell
    * (which is guaranteed to be the case if the hole does not touch its shell)
    *
    * To improve performance of this function the caller should
    * make the passed shellList as small as possible (e.g.
    * by using a spatial index filter beforehand).
    *
    * @return containing EdgeRing, if there is one
    * or null if no containing EdgeRing is found
    */
    OverlayEdgeRing* findEdgeRingContaining(std::vector<OverlayEdgeRing*>& erList);


};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

