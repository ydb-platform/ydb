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

#include <geos/edgegraph/HalfEdge.h>
#include <geos/geom/Location.h>

#include <geos/export.h>

#include <memory>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class CoordinateSequence;
class CoordinateArraySequence;
}
namespace operation {
namespace overlayng {
class OverlayEdgeRing;
class MaximalEdgeRing;
class OverlayLabel;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/**
* Creates a single OverlayEdge.
*/
class GEOS_DLL OverlayEdge : public edgegraph::HalfEdge {

private:

    // Members
    const CoordinateSequence* pts;
    /**
    * 'true' indicates direction is forward along segString
    * 'false' is reverse direction
    * The label must be interpreted accordingly.
    */
    bool direction;
    Coordinate dirPt;
    OverlayLabel* label;
    bool m_isInResultArea;
    bool m_isInResultLine;
    bool m_isVisited;
    OverlayEdge* nextResultEdge;
    const OverlayEdgeRing* edgeRing;
    const MaximalEdgeRing* maxEdgeRing;
    OverlayEdge* nextResultMaxEdge;

    void markVisited();


public:

    // takes ownershiph of CoordinateSequence
    OverlayEdge(const Coordinate& p_orig, const Coordinate& p_dirPt,
                bool p_direction, OverlayLabel* p_label,
                const CoordinateSequence* p_pts)
        : HalfEdge(p_orig)
        , pts(p_pts)
        , direction(p_direction)
        , dirPt(p_dirPt)
        , label(p_label)
        , m_isInResultArea(false)
        , m_isInResultLine(false)
        , m_isVisited(false)
        , nextResultEdge(nullptr)
        , edgeRing(nullptr)
        , maxEdgeRing(nullptr)
        , nextResultMaxEdge(nullptr)
     {}

    bool isForward() const;

    const Coordinate& directionPt() const override;
    ~OverlayEdge() override {};

    OverlayLabel* getLabel() const;

    Location getLocation(int index, int position) const;

    const Coordinate& getCoordinate() const;

    const CoordinateSequence* getCoordinatesRO() const;

    std::unique_ptr<CoordinateSequence> getCoordinates();

    std::unique_ptr<CoordinateSequence> getCoordinatesOriented();

    /**
    * Adds the coordinates of this edge to the given list,
    * in the direction of the edge.
    * Duplicate coordinates are removed
    * (which means that this is safe to use for a path
    * of connected edges in the topology graph).
    *
    * @param coords the coordinate list to add to
    */
    void addCoordinates(CoordinateArraySequence* coords);

    OverlayEdge* symOE() const;
    OverlayEdge* oNextOE() const;

    bool isInResultArea() const;

    bool isInResultAreaBoth() const;

    void unmarkFromResultAreaBoth();

    void markInResultArea();

    void markInResultAreaBoth();

    bool isInResultLine() const;

    void markInResultLine();

    bool isInResult() const;

    bool isInResultEither() const;

    void setNextResult(OverlayEdge* e);

    OverlayEdge* nextResult() const;

    bool isResultLinked() const;

    void setNextResultMax(OverlayEdge* e);

    OverlayEdge* nextResultMax() const;

    bool isResultMaxLinked() const;

    bool isVisited() const;
    void markVisitedBoth();

    const OverlayEdgeRing* getEdgeRing() const;
    void setEdgeRing(const OverlayEdgeRing* p_edgeRing);

    const MaximalEdgeRing* getEdgeRingMax() const;
    void setEdgeRingMax(const MaximalEdgeRing* maximalEdgeRing);

    friend std::ostream& operator<<(std::ostream& os, const OverlayEdge& oe);
    std::string resultSymbol() const;

};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

