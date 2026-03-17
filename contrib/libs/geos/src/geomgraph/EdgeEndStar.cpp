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
 * Last port: geomgraph/EdgeEndStar.java r428 (JTS-1.12+)
 *
 **********************************************************************/

#include <geos/util/TopologyException.h>
#include <geos/geomgraph/EdgeEndStar.h>
#include <geos/algorithm/locate/SimplePointInAreaLocator.h>
#include <geos/geom/Location.h>
#include <geos/geomgraph/Label.h>
#include <geos/geom/Position.h>
#include <geos/geomgraph/GeometryGraph.h>

#include <cassert>
#include <string>
#include <vector>
#include <sstream>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

//using namespace std;
//using namespace geos::algorithm;
using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph

/*public*/
EdgeEndStar::EdgeEndStar()
    :
    edgeMap()
{
    ptInAreaLocation[0] = Location::NONE;
    ptInAreaLocation[1] = Location::NONE;
}

/*public*/
Coordinate&
EdgeEndStar::getCoordinate()
{
    static Coordinate nullCoord(DoubleNotANumber, DoubleNotANumber, DoubleNotANumber);
    if(edgeMap.empty()) {
        return nullCoord;
    }

    EdgeEndStar::iterator it = begin();
    EdgeEnd* e = *it;
    assert(e);
    return e->getCoordinate();
}

/*public*/
const Coordinate&
EdgeEndStar::getCoordinate() const
{
    return const_cast<EdgeEndStar*>(this)->getCoordinate();
}

/*public*/
EdgeEnd*
EdgeEndStar::getNextCW(EdgeEnd* ee)
{
    EdgeEndStar::iterator it = find(ee);
    if(it == end()) {
        return nullptr;
    }
    if(it == begin()) {
        it = end();
        --it;
    }
    else {
        --it;
    }
    return *it;
}

/*public*/
void
EdgeEndStar::computeLabelling(std::vector<GeometryGraph*>* geomGraph)
//throw(TopologyException *)
{
    computeEdgeEndLabels((*geomGraph)[0]->getBoundaryNodeRule());

    // Propagate side labels  around the edges in the star
    // for each parent Geometry
    //
    // these calls can throw a TopologyException
    propagateSideLabels(0);
    propagateSideLabels(1);

    /*
     * If there are edges that still have null labels for a geometry
     * this must be because there are no area edges for that geometry
     * incident on this node.
     * In this case, to label the edge for that geometry we must test
     * whether the edge is in the interior of the geometry.
     * To do this it suffices to determine whether the node for the
     * edge is in the interior of an area.
     * If so, the edge has location INTERIOR for the geometry.
     * In all other cases (e.g. the node is on a line, on a point, or
     * not on the geometry at all) the edge
     * has the location EXTERIOR for the geometry.
     *
     * Note that the edge cannot be on the BOUNDARY of the geometry,
     * since then there would have been a parallel edge from the
     * Geometry at this node also labelled BOUNDARY
     * and this edge would have been labelled in the previous step.
     *
     * This code causes a problem when dimensional collapses are present,
     * since it may try and determine the location of a node where a
     * dimensional collapse has occurred.
     * The point should be considered to be on the EXTERIOR
     * of the polygon, but locate() will return INTERIOR, since it is
     * passed the original Geometry, not the collapsed version.
     *
     * If there are incident edges which are Line edges labelled BOUNDARY,
     * then they must be edges resulting from dimensional collapses.
     * In this case the other edges can be labelled EXTERIOR for this
     * Geometry.
     *
     * MD 8/11/01 - NOT TRUE!  The collapsed edges may in fact be in the
     * interior of the Geometry, which means the other edges should be
     * labelled INTERIOR for this Geometry.
     * Not sure how solve this...  Possibly labelling needs to be split
     * into several phases:
     * area label propagation, symLabel merging, then finally null label
     * resolution.
     */
    bool hasDimensionalCollapseEdge[2] = {false, false};

    EdgeEndStar::iterator endIt = end();
    for(EdgeEndStar::iterator it = begin(); it != endIt; ++it) {
        EdgeEnd* e = *it;
        assert(e);
        const Label& label = e->getLabel();
        for(int geomi = 0; geomi < 2; geomi++) {
            if(label.isLine(geomi) && label.getLocation(geomi) == Location::BOUNDARY) {
                hasDimensionalCollapseEdge[geomi] = true;
            }
        }
    }

    for(EdgeEndStar::iterator it = begin(); it != end(); ++it) {
        EdgeEnd* e = *it;
        assert(e);
        Label& label = e->getLabel();
        for(uint32_t geomi = 0; geomi < 2; ++geomi) {
            if(label.isAnyNull(geomi)) {
                Location loc = Location::NONE;
                if(hasDimensionalCollapseEdge[geomi]) {
                    loc = Location::EXTERIOR;
                }
                else {
                    Coordinate& p = e->getCoordinate();
                    loc = getLocation(geomi, p, geomGraph);
                }
                label.setAllLocationsIfNull(geomi, loc);
            }
        }
    }
}

/*private*/
void
EdgeEndStar::computeEdgeEndLabels(
    const algorithm::BoundaryNodeRule& boundaryNodeRule)
{
    // Compute edge label for each EdgeEnd
    for(EdgeEndStar::iterator it = begin(); it != end(); ++it) {
        EdgeEnd* ee = *it;
        assert(ee);
        ee->computeLabel(boundaryNodeRule);
    }
}

/*public*/
Location
EdgeEndStar::getLocation(uint32_t geomIndex,
                         const Coordinate& p, std::vector<GeometryGraph*>* geom)
{
    // compute location only on demand
    if(ptInAreaLocation[geomIndex] == Location::NONE) {
        ptInAreaLocation[geomIndex] = algorithm::locate::SimplePointInAreaLocator::locate(p,
                                      (*geom)[geomIndex]->getGeometry());
    }
    return ptInAreaLocation[geomIndex];
}

/*public*/
bool
EdgeEndStar::isAreaLabelsConsistent(const GeometryGraph& geomGraph)
{
    computeEdgeEndLabels(geomGraph.getBoundaryNodeRule());
    return checkAreaLabelsConsistent(0);
}

/*private*/
bool
EdgeEndStar::checkAreaLabelsConsistent(uint32_t geomIndex)
{
    // Since edges are stored in CCW order around the node,
    // As we move around the ring we move from the right to
    // the left side of the edge

    // if no edges, trivially consistent
    if(edgeMap.empty()) {
        return true;
    }

    // initialize startLoc to location of last L side (if any)
    assert(*rbegin());
    const Label& startLabel = (*rbegin())->getLabel();
    Location startLoc = startLabel.getLocation(geomIndex, Position::LEFT);

    // Found unlabelled area edge
    assert(startLoc != Location::NONE);

    Location currLoc = startLoc;

    for(EdgeEndStar::iterator it = begin(), itEnd = end(); it != itEnd; ++it) {
        EdgeEnd* e = *it;
        assert(e);
        const Label& eLabel = e->getLabel();

        // we assume that we are only checking a area

        // Found non-area edge
        assert(eLabel.isArea(geomIndex));

        Location leftLoc = eLabel.getLocation(geomIndex, Position::LEFT);
        Location rightLoc = eLabel.getLocation(geomIndex, Position::RIGHT);
        // check that edge is really a boundary between inside and outside!
        if(leftLoc == rightLoc) {
            return false;
        }
        // check side location conflict
        //assert(rightLoc == currLoc); // "side location conflict " + locStr);
        if(rightLoc != currLoc) {
            return false;
        }
        currLoc = leftLoc;
    }
    return true;
}

/*public*/
void
EdgeEndStar::propagateSideLabels(uint32_t geomIndex)
//throw(TopologyException *)
{
    // Since edges are stored in CCW order around the node,
    // As we move around the ring we move from the right to the
    // left side of the edge
    Location startLoc = Location::NONE;

    EdgeEndStar::iterator beginIt = begin();
    EdgeEndStar::iterator endIt = end();
    EdgeEndStar::iterator it;

    // initialize loc to location of last L side (if any)
    for(it = beginIt; it != endIt; ++it) {
        EdgeEnd* e = *it;
        assert(e);
        const Label& label = e->getLabel();
        if(label.isArea(geomIndex) &&
                label.getLocation(geomIndex, Position::LEFT) != Location::NONE) {
            startLoc = label.getLocation(geomIndex, Position::LEFT);
        }
    }

    // no labelled sides found, so no labels to propagate
    if(startLoc == Location::NONE) {
        return;
    }

    Location currLoc = startLoc;
    for(it = beginIt; it != endIt; ++it) {
        EdgeEnd* e = *it;
        assert(e);
        Label& label = e->getLabel();
        // set null ON values to be in current location
        if(label.getLocation(geomIndex, Position::ON) == Location::NONE) {
            label.setLocation(geomIndex, Position::ON, currLoc);
        }

        // set side labels (if any)
        // if (label.isArea())  //ORIGINAL
        if(label.isArea(geomIndex)) {
            Location leftLoc = label.getLocation(geomIndex,
                                                 Position::LEFT);

            Location rightLoc = label.getLocation(geomIndex,
                                                  Position::RIGHT);

            // if there is a right location, that is the next
            // location to propagate
            if(rightLoc != Location::NONE) {
                if(rightLoc != currLoc)
                    throw util::TopologyException("side location conflict",
                                                  e->getCoordinate());
                if(leftLoc == Location::NONE) {
                    // found single null side at e->getCoordinate()
                    assert(0);
                }
                currLoc = leftLoc;
            }
            else {
                /*
                 * RHS is null - LHS must be null too.
                 * This must be an edge from the other
                 * geometry, which has no location
                 * labelling for this geometry.
                 * This edge must lie wholly inside or
                 * outside the other geometry (which is
                 * determined by the current location).
                 * Assign both sides to be the current
                 * location.
                 */
                // found single null side
                assert(label.getLocation(geomIndex,
                                         Position::LEFT) == Location::NONE);

                label.setLocation(geomIndex, Position::RIGHT,
                                  currLoc);
                label.setLocation(geomIndex, Position::LEFT,
                                  currLoc);
            }
        }
    }
}

/*public*/
std::string
EdgeEndStar::print() const
{
    std::ostringstream s;
    s << *this;
    return s.str();
}

std::ostream&
operator<< (std::ostream& os, const EdgeEndStar& es)
{
    os << "EdgeEndStar:   " << es.getCoordinate() << "\n";
    for(EdgeEndStar::const_iterator it = es.begin(), itEnd = es.end(); it != itEnd; ++it) {
        const EdgeEnd* e = *it;
        assert(e);
        os << *e;
    }
    return os;
}

} // namespace geos.geomgraph
} // namespace geos
