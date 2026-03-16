/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2016 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/distance/FacetSequence.java (f6187ee2 JTS-1.14)
 *
 **********************************************************************/

#ifndef GEOS_OPERATION_DISTANCE_FACETSEQUENCE_H
#define GEOS_OPERATION_DISTANCE_FACETSEQUENCE_H

#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/LineSegment.h>
#include <geos/operation/distance/GeometryLocation.h>

namespace geos {
namespace operation {
namespace distance {
class FacetSequence {
private:
    const geom::CoordinateSequence* pts;
    const size_t start;
    const size_t end;
    const geom::Geometry* geom;
    /*
    * Unlike JTS, we store the envelope in the FacetSequence so
    * that it has a clear owner.  This is helpful when making a
    * tree of FacetSequence objects (FacetSequenceTreeBuilder)
    */
    geom::Envelope env;

    double computeDistanceLineLine(const FacetSequence& facetSeq,
                                   std::vector<GeometryLocation> *locs) const;

    double computeDistancePointLine(const geom::Coordinate& pt,
                                    const FacetSequence& facetSeq,
                                    std::vector<GeometryLocation> *locs) const;

    void updateNearestLocationsPointLine(const geom::Coordinate& pt,
                                         const FacetSequence& facetSeq, size_t i,
                                         const geom::Coordinate& q0, const geom::Coordinate &q1,
                                         std::vector<GeometryLocation> *locs) const;

    void updateNearestLocationsLineLine(size_t i, const geom::Coordinate& p0, const geom::Coordinate& p1,
                                        const FacetSequence& facetSeq,
                                        size_t j, const geom::Coordinate& q0, const geom::Coordinate &q1,
                                        std::vector<GeometryLocation> *locs) const;

    void computeEnvelope();

public:
    const geom::Envelope* getEnvelope() const;

    const geom::Coordinate* getCoordinate(size_t index) const;

    size_t size() const;

    bool isPoint() const;

    double distance(const FacetSequence& facetSeq) const;

    FacetSequence(const geom::CoordinateSequence* pts, size_t start, size_t end);

    FacetSequence(const geom::Geometry* geom, const geom::CoordinateSequence* pts, size_t start, size_t end);

    std::vector<GeometryLocation> nearestLocations(const FacetSequence& facetSeq) const;

};

}
}
}

#endif //GEOS_OPERATION_DISTANCE_FACETSEQUENCE_H
