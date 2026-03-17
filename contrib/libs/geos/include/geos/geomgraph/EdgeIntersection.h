/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009-2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: geomgraph/EdgeIntersection.java rev. 1.5 (JTS-1.10)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_EDGEINTERSECTION_H
#define GEOS_GEOMGRAPH_EDGEINTERSECTION_H

#include <geos/export.h>

#include <geos/geom/Coordinate.h> // for composition and inlines

#include <geos/inline.h>

#include <ostream>


namespace geos {
namespace geomgraph { // geos.geomgraph

/** \brief
 * Represents a point on an edge which intersects with another edge.
 *
 * The intersection may either be a single point, or a line segment
 * (in which case this point is the start of the line segment)
 * The intersection point must be precise.
 *
 */
class GEOS_DLL EdgeIntersection {
public:

    // the point of intersection
    geom::Coordinate coord;

    // the edge distance of this point along the containing line segment
    double dist;

    // the index of the containing line segment in the parent edge
    size_t segmentIndex;

    EdgeIntersection(const geom::Coordinate& newCoord,
                     size_t newSegmentIndex, double newDist)
        :
        coord(newCoord),
        dist(newDist),
        segmentIndex(newSegmentIndex)
    {}

    bool
    isEndPoint(size_t maxSegmentIndex) const
    {
        if(segmentIndex == 0 && dist == 0.0) {
            return true;
        }
        if(segmentIndex == maxSegmentIndex) {
            return true;
        }
        return false;
    }

    const geom::Coordinate&
    getCoordinate() const
    {
        return coord;
    }

    size_t
    getSegmentIndex() const
    {
        return segmentIndex;
    }

    double
    getDistance() const
    {
        return dist;
    }

    bool operator==(const EdgeIntersection& other) const {
        return segmentIndex == other.segmentIndex &&
            dist == other.dist;

        // We don't check the coordinate, consistent with operator<
    }

};

/// Strict weak ordering operator for EdgeIntersection
///
/// This is the C++ equivalent of JTS's compareTo
inline bool
operator< (const EdgeIntersection& ei1, const EdgeIntersection& ei2)
{
    if(ei1.segmentIndex < ei2.segmentIndex) {
        return true;
    }
    if(ei1.segmentIndex == ei2.segmentIndex) {
        if(ei1.dist < ei2.dist) {
            return true;
        }

        // TODO: check if the Coordinate matches, or this will
        //       be a robustness issue in computin distance
        //       See http://trac.osgeo.org/geos/ticket/350
    }
    return false;
}

// @deprecated, use strict weak ordering operator
struct GEOS_DLL  EdgeIntersectionLessThen {
    bool
    operator()(const EdgeIntersection* ei1,
               const EdgeIntersection* ei2) const
    {
        return *ei1 < *ei2;
    }

    bool
    operator()(const EdgeIntersection& ei1,
               const EdgeIntersection& ei2) const
    {
        return ei1 < ei2;
    }
};

/// Output operator
inline std::ostream&
operator<< (std::ostream& os, const EdgeIntersection& e)
{
    os << e.coord << " seg # = " << e.segmentIndex << " dist = " << e.dist;
    return os;
}

} // namespace geos.geomgraph
} // namespace geos

#endif // ifndef GEOS_GEOMGRAPH_EDGEINTERSECTION_H


