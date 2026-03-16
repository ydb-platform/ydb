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

#include <geos/operation/overlayng/OverlayLabel.h>
#include <geos/geom/Coordinate.h>
#include <geos/export.h>

#include <memory>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class CoordinateSequence;
}
namespace operation {
namespace overlayng {
class EdgeSourceInfo;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


/**
 * Represents the underlying linework for edges in a topology graph,
 * and carries the topology information
 * derived from the two parent geometries.
 * The edge may be the result of the merging of
 * two or more edges which have the same underlying linework
 * (although possibly different orientations).
 * In this case the topology information is
 * derived from the merging of the information in the
 * source edges.
 * Merged edges can occur in the following situations
 *
 *  - Due to topology collapse caused by snapping or rounding
 *    of polygonal geometries.
 *  - Due to coincident linework in a linear input
 *
 * The source edges may have the same parent geometry,
 * or different ones, or a mix of the two.
 *
 * @author mdavis
 */
class GEOS_DLL Edge {

private:

    // Members
    int aDim = OverlayLabel::DIM_UNKNOWN;
    int aDepthDelta = 0;
    bool aIsHole = false;
    int bDim = OverlayLabel::DIM_UNKNOWN;
    int bDepthDelta = 0;
    bool bIsHole = false;
    std::unique_ptr<geom::CoordinateSequence> pts;

    // Methods

    /**
    * Populates the label for an edge resulting from an input geometry.
    *  - If the edge is not part of the input, the label is left as NOT_PART
    *  - If input is an Area and the edge is on the boundary
    * (which may include some collapses),
    * edge is marked as an AREA edge and side locations are assigned
    *  - If input is an Area and the edge is collapsed
    * (depth delta = 0),
    * the label is set to COLLAPSE.
    * The location will be determined later
    * by evaluating the final graph topology.
    *  - If input is a Line edge is set to a LINE edge.
    * For line edges the line location is not significant
    * (since there is no parent area for which to determine location).
    */
    void initLabel(OverlayLabel& lbl, int geomIndex, int dim, int depthDelta, bool isHole) const;

    int labelDim(int dim, int depthDelta) const;
    bool isHole(int index) const;
    bool isBoundary(int geomIndex) const;

    /**
    * Tests whether the edge is part of a shell in the given geometry.
    * This is only the case if the edge is a boundary.
    */
    bool isShell(int geomIndex) const;

    geom::Location locationRight(int depthDelta) const;
    geom::Location locationLeft(int depthDelta) const;

    int delSign(int depthDel) const;
    void copyInfo(const EdgeSourceInfo* info);
    bool isHoleMerged(int geomIndex, const Edge* edge1, const Edge* edge2) const;


public:

    Edge()
        : aDim(OverlayLabel::DIM_UNKNOWN)
        , aDepthDelta(0)
        , aIsHole(false)
        , bDim(OverlayLabel::DIM_UNKNOWN)
        , bDepthDelta(0)
        , bIsHole(false)
        , pts(nullptr)
        {};

    friend std::ostream& operator<<(std::ostream& os, const Edge& e);

    static bool isCollapsed(const geom::CoordinateSequence* pts);

    // takes ownership of pts from caller
    Edge(geom::CoordinateSequence* p_pts, const EdgeSourceInfo* info);

    // return a clone of the underlying points
    std::unique_ptr<geom::CoordinateSequence> getCoordinates();
    // return a read-only pointer to the underlying points
    const geom::CoordinateSequence* getCoordinatesRO() const;
    // release the underlying points to the caller
    geom::CoordinateSequence* releaseCoordinates();

    const geom::Coordinate& getCoordinate(size_t index)  const;

    std::size_t size() const;
    bool direction() const;

    /**
    * Compares two coincident edges to determine
    * whether they have the same or opposite direction.
    */
    bool relativeDirection(const Edge* edge2) const;
    int dimension(int geomIndex) const;

    /**
    * Merges an edge into this edge,
    * updating the topology info accordingly.
    */
    void merge(const Edge* edge);

    void populateLabel(OverlayLabel &ovl) const;

    /*public*/
    bool compareTo(const Edge& e) const
    {
        const geom::Coordinate& ca = getCoordinate(0);
        const geom::Coordinate& cb = e.getCoordinate(0);
        if(ca.compareTo(cb) < 0) {
            return true;
        }
        else if (ca.compareTo(cb) > 0) {
            return false;
        }
        else {
            const geom::Coordinate& cca = getCoordinate(1);
            const geom::Coordinate& ccb = e.getCoordinate(1);
            if(cca.compareTo(ccb) < 0) {
                return true;
            }
            else if (cca.compareTo(ccb) > 0) {
                return false;
            }
            else {
                return false;
            }
        }
    }

};

bool EdgeComparator(const Edge* a, const Edge* b);



} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

